// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_amqp_1.h"
#include "common/dout.h"
#include "include/ceph_assert.h"

#include <boost/optional/optional.hpp>
#include <boost/lockfree/queue.hpp>

#include <proton/error_condition.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/message.hpp>
#include <proton/container.hpp>
#include <proton/connection.hpp>
#include <proton/connection_options.hpp>
#include <proton/source_options.hpp>
#include <proton/sender.hpp>
#include <proton/work_queue.hpp>
#include <proton/tracker.hpp>
#include <proton/delivery.hpp>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <unordered_map>
#include <thread>

#define dout_subsys ceph_subsys_rgw

namespace rgw::amqp_1 {

static const int RGW_AMQP_1_STATUS_CONNECTION_CLOSED = -0x1002;
static const int RGW_AMQP_1_STATUS_QUEUE_FULL = -0x1003;
static const int RGW_AMQP_1_STATUS_MAX_INFLIGHT = -0x1004;
static const int RGW_AMQP_1_STATUS_MANAGER_STOPPED = -0x1005;

static const int RGW_AMQP_1_STATUS_OK = 0x0;

	struct reply_callback_with_tag_t {
		uint64_t tag;
		reply_callback_t cb;

		reply_callback_with_tag_t(uint64_t _tag, reply_callback_t _cb) : tag(_tag), cb(_cb) {}

		bool operator==(uint64_t rhs) {
			return tag == rhs;
		}
	};

	typedef std::vector<reply_callback_with_tag_t> CallbackList;

	struct connection_t : public proton::messaging_handler{
		bool marked_for_deletion = false;
		uint64_t delivery_tag = 1;
		int status;
		mutable std::atomic<int> ref_count = 0;
		CephContext* const cct;
		CallbackList callbacks;
		std::string broker;
		proton::sender sender;
		// proton::connection_options options;
		proton::work_queue* pwork_queue;
		int queued;
		std::mutex lock;
		std::condition_variable sender_ready;

		const boost::optional<std::string> ca_location;

		public:
		// void send(const proton::message& m);

		private:
		proton::work_queue* work_queue() {
			std::unique_lock<std::mutex> lk(lock);
			while(!pwork_queue) sender_ready.wait(lk);
			return pwork_queue;
		}

		void on_sender_open(proton::sender& s) override {
			std::lock_guard<std::mutex> lk(lock);
			sender = s;
			pwork_queue = &s.work_queue();
		}

		void on_sendable(proton::sender& s) override {
			std::lock_guard<std::mutex> lk(lock);
			sender_ready.notify_all();
		}

		void do_send(const proton::message& m) {
			sender.send(m);
			std::lock_guard<std::mutex> lk(lock);
			--queued;
			sender_ready.notify_all();
		}

		// void on_tracker_accept(proton::tracker& t) override;

		// void on_error(const proton::error_condition& e) override;

		void close() {
			pwork_queue->add([=]() { sender.connection().close(); });
		}

		public:

		// default ctor
		connection_t(CephContext* _cct, const	std::string& _broker, const
				boost::optional<const std::string&> _ca_location) : 
			cct(_cct), broker(_broker), ca_location(_ca_location), pwork_queue(0) { }

		~connection_t() {
			destroy(RGW_AMQP_1_STATUS_CONNECTION_CLOSED);
		}

		// TODO
		void destroy(int s) {
			status = s;
			close();

			// fire all remaining callbacks
			std::for_each(callbacks.begin(), callbacks.end(), [this](auto& cb_tag) {
				cb_tag.cb(status);
				ldout(cct, 20) << "AMQP1.0 destroy: invoking callback with tag=" << cb_tag.tag << dendl;
			});
			callbacks.clear();
			delivery_tag = 1;
		}

		// sender.active() or sender.uninitialized()
		bool is_ok() const {
			return (!sender.active() && !marked_for_deletion);
		}

		friend void instrusive_ptr_add_ref(const connection_t* p);
		friend void instrusive_ptr_release(const connection_t* p);

	};

	// these are required interfaces so that connection_t could be used inside
	// boost::intrusive_ptr
	void intrusive_ptr_add_ref(const connection_t* p) {
		++p->ref_count;
	}
	void intrusive_ptr_release(const connection_t* p) {
		if (--p->ref_count == 0) {
			delete p;
		}
	}

	// TODO
	connection_ptr_t& create_connection(proton::container& container, connection_ptr_t& conn) {
		// pointer must be valid and not marked for deletion
		ceph_assert(conn && !conn->marked_for_deletion);

		// reset all status code
		conn->status = RGW_AMQP_1_STATUS_OK;

		// TODO: ssl config

		// TODO: detail error control and error code handling
		container.open_sender(conn->broker,
				proton::connection_options().handler(*conn));
		return conn;
		
	}

	// TODO: utility function to create a new connection
	connection_ptr_t create_new_connection(proton::container& container, const std::string& broker, CephContext*
			cct, boost::optional<const std::string&> ca_location) {
		// create connection state
		connection_ptr_t conn = new connection_t(cct, broker, ca_location);
		// conn->broker = broker;
		// conn->cct = cct;
		// conn->ca_location = ca_location;
		return create_connection(container, conn);
	}

	struct message_wrapper_t {
		connection_ptr_t conn;
		std::string topic;
		std::string message;
		reply_callback_t cb;

		message_wrapper_t(connection_ptr_t& _conn,
				const std::string& _topic,
				const std::string& _message,
				reply_callback_t _cb) : conn(_conn), topic(_topic), message(_message), cb(_cb) { }
	};


	typedef std::unordered_map<std::string, connection_ptr_t> ConnectionList;
	typedef boost::lockfree::queue<message_wrapper_t*, boost::lockfree::fixed_sized<true>> MessageQueue;


	class Manager {
		public:
			const size_t max_connections;
			const size_t max_inflight;
			const size_t max_queue;
		private:
			std::atomic<size_t> connection_count;
			bool stopped;
			struct timeval read_timeout;
			ConnectionList connections;
			MessageQueue messages;
			std::atomic<size_t> queued;
			std::atomic<size_t> dequeued;
			CephContext* const cct;
			mutable std::mutex connections_lock;
			// TODO is there two to reserve?
			// const ceph::coarse_real_clock::duration idle_time;
			// const ceph::coarse_real_clock::duration reconnect_time;
			std::thread runner;

			proton::container container;
			std::thread container_runner;

			// meeting: to restrict container runner running before the runner
			void run_container() {
				container.run();
			}

			// TODO
			void publish_internal(message_wrapper_t* message) {
				const std::unique_ptr<message_wrapper_t> msg_owner(message);
				auto& conn = message->conn;

				if(!conn->is_ok()) {
					ldout(conn->cct, 1) << "AMQP_1 publish: connection had an issue while"
					"message was in the queue" << dendl;
					if(message->cb) {
						message->cb(RGW_AMQP_1_STATUS_CONNECTION_CLOSED);
					}
					return;
				}

				// message without a callback
				if(message->cb == nullptr) {
					// TODO
				}

				// message with a callback
			}

			void run() noexcept {
			}

			// TODO
			static void delete_message(const message_wrapper_t* message) {
				delete message;
			}

		public:
			Manager(size_t _max_connections,
					size_t _max_inflight,
					size_t _max_queue, 
					CephContext* _cct) : 
				max_connections(_max_connections),
				max_inflight(_max_inflight),
				max_queue(_max_queue),
				connection_count(0),
				stopped(false),
				connections(_max_connections),
				messages(max_queue),
				queued(0),
				dequeued(0),
				cct(_cct),
				runner(&Manager::run, this),
				container_runner(&Manager::run_container, this) {
					// The hashmap has "max connections" as the initial number of buckets, 
					// and allows for 10 collisions per bucket before rehash.
					// This is to prevent rehashing so that iterators are not invalidated 
					// when a new connection is added.
					connections.max_load_factor(10.0);
					// give the runner thread a name for easier debugging
					const auto rc = ceph_pthread_setname(runner.native_handle(), "amqp_1_manager");
					ceph_assert(rc==0);
				}

			// non copyable
			Manager(const Manager&) = delete;
			const Manager& operator=(const Manager&) = delete;

			// stop the main thread
			void stop() {
				stopped = true;
			}

			// disconnect from a broker
			bool disconnect(connection_ptr_t& conn) {
				if (!conn || stopped) {
					return false;
				}
				conn->marked_for_deletion = true;
				return true;
			}

			// TODO with params
			connection_ptr_t connect(const std::string& url, bool use_ssl,
			boost::optional<const std::string&> ca_location) {
				if(stopped) {
					ldout(cct, 1) << "AMQP_1 connect: manager is stopped" << dendl;
					return nullptr;
				}
				// TODO: check url format parse
				// TODO: max connections check
				std::lock_guard<std::mutex> lock(connections_lock);

				// TODO: find in the stale connectionlists

				const auto conn = create_new_connection(container, url, cct, ca_location);

				ceph_assert(conn);
				++connection_count;
				ldout(cct, 10) << "AMQP_1 connect: new connection is created. Total"
					"connections: " << connection_count << dendl;
				return connections.emplace(url, conn).first->second;
			}

			int publish(connection_ptr_t& conn, const std::string& topic, const
					std::string& message) {
				if(stopped) {
					return RGW_AMQP_1_STATUS_MANAGER_STOPPED;
				}
				if(!conn || !conn->is_ok()) {
					return RGW_AMQP_1_STATUS_CONNECTION_CLOSED;
				}
				if(messages.push(new message_wrapper_t(conn, topic, message, nullptr)))
					{
						++queued;
						return RGW_AMQP_1_STATUS_OK;
					}
				return RGW_AMQP_1_STATUS_QUEUE_FULL;
			}

			int publish_with_confirm(connection_ptr_t& conn, const std::string& topic,
					const std::string& message, reply_callback_t cb) {
				if(stopped) {
					return RGW_AMQP_1_STATUS_MANAGER_STOPPED;
				}
				if(!conn || !conn->is_ok()) {
					return RGW_AMQP_1_STATUS_CONNECTION_CLOSED;
				}
				if(messages.push(new message_wrapper_t(conn, topic, message, cb)))
					{
						++queued;
						return RGW_AMQP_1_STATUS_OK;
					}
				return RGW_AMQP_1_STATUS_QUEUE_FULL;
			}

			~Manager() {
				stopped = true;
				runner.join();
				container_runner.join();
				messages.consume_all(delete_message);
			}

			// get the number of connections
			size_t get_connection_count() const {
				return connection_count;
			}

			// get the number of in-flight messages
			size_t get_inflight() const {
				size_t sum = 0;
				std::lock_guard<std::mutex> lock(connections_lock);
				std::for_each(connections.begin(), connections.end(), [&sum](auto& conn_pair) {
						sum += conn_pair.second->callbacks.size();
						});
				return sum;
			}

			// running counter of the queued messages
			size_t get_queued() const {
				return queued;
			}

			// running counter of the dequeued messages
			size_t get_dequeued() const {
				return dequeued;
			}
	};

	// singleton manager
	// note that the manager itself is not a singleton, and multiple instances may co-exist
	// TODO make the pointer atomic in allocation and deallocation to avoid race conditions
	static Manager* s_manager = nullptr;

	static const size_t MAX_CONNECTIONS_DEFAULT = 256;
	static const size_t MAX_INFLIGHT_DEFAULT = 8192; 
	static const size_t MAX_QUEUE_DEFAULT = 8192;
	static const long READ_TIMEOUT_USEC = 100;

	bool init(CephContext* cct) {
		if (s_manager) {
			return false;
		}
		// TODO: take conf from CephContext
		s_manager = new Manager(MAX_CONNECTIONS_DEFAULT, MAX_INFLIGHT_DEFAULT, MAX_QUEUE_DEFAULT, cct);
		return true;
	}

	void shutdown() {
		delete s_manager;
		s_manager = nullptr;
	}

	connection_ptr_t connect(const std::string& url, bool use_ssl,
			boost::optional<const std::string&> ca_location) {
		if (!s_manager) return nullptr;
		return s_manager->connect(url, use_ssl, ca_location);
	}

	int publish(connection_ptr_t& conn, 
			const std::string& topic,
			const std::string& message) {
		if (!s_manager) return RGW_AMQP_1_STATUS_MANAGER_STOPPED;
		return s_manager->publish(conn, topic, message);
	}

	int publish_with_confirm(connection_ptr_t& conn, 
			const std::string& topic,
			const std::string& message,
			reply_callback_t cb) {
		if (!s_manager) return RGW_AMQP_1_STATUS_MANAGER_STOPPED;
		return s_manager->publish_with_confirm(conn, topic, message, cb);
	}

	size_t get_connection_count() {
		if (!s_manager) return 0;
		return s_manager->get_connection_count();
	}

	size_t get_inflight() {
		if (!s_manager) return 0;
		return s_manager->get_inflight();
	}

	size_t get_queued() {
		if (!s_manager) return 0;
		return s_manager->get_queued();
	}

	size_t get_dequeued() {
		if (!s_manager) return 0;
		return s_manager->get_dequeued();
	}

	size_t get_max_connections() {
		if (!s_manager) return MAX_CONNECTIONS_DEFAULT;
		return s_manager->max_connections;
	}

	size_t get_max_inflight() {
		if (!s_manager) return MAX_INFLIGHT_DEFAULT;
		return s_manager->max_inflight;
	}

	size_t get_max_queue() {
		if (!s_manager) return MAX_QUEUE_DEFAULT;
		return s_manager->max_queue;
	}

	bool disconnect(connection_ptr_t& conn) {
		if (!s_manager) return false;
		return s_manager->disconnect(conn);
	}

} // namespace amqp_1
