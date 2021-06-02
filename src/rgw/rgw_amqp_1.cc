// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_amqp_1.h"
#include "common/dout.h"

#include <proton/messaging_handler.hpp>
#include <proton/connection.hpp>
#include <proton/connection_options.hpp>
#include <proton/fwd.hpp>
#include <proton/sender.hpp>
#include <boost/lockfree/queue.hpp>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <unordered_map>
#include <thread>
#define dout_sys ceph_subsys_rgw

namespace rgw::amqp_1 {

	struct reply_callback_with_tag_t {
		uint64_t tag;
		reply_callback_t cb;

		reply_callback_with_tag_t(uint64_t _tag, reply_callback_t _cb) : tag(_tag), cb(_cb) {}

		bool operator==(uint64_t rhs) {
			return tag == rhs;
		}
	};

	typedef std::vector<reply_callback_with_tag_t> CallbackList;

	struct connection_t {
		bool marked_for_deletion;
		mutable std::atomic<int> ref_count = 0;
		CephContext* const cct;
		CallbackList callbacks;
		private:
		class proton_handler_t : public proton::messaging_handler {
			public:
			proton_handler_t(proton::container& _cont, const std::string& _url);

			private:
			std::string conn_url;
			proton::sender sender;

			proton::work_queue* work_queue;
			std::mutex wq_lock;
			std::condition_variable sender_ready;

			public:
			// TODO:

		};

		public:
		int for_unittest() { return 200; }

		// TODO
		void destroy(int s);
		// TODO
		bool is_ok() const;

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
	connection_ptr_t& create_connection(connection_ptr_t& conn);

	// TODO: utility function to create a new connection
	connection_ptr_t create_new_connection(const std::string& info);

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

			// TODO
			void publish_internal(message_wrapper_t* message);

			void run() noexcept;

			static void delete_message(const message_wrapper_t* message);

		public:
			Manager();

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
			connection_ptr_t connect(const std::string& info);

			int publish(connection_ptr_t& conn, const std::string& topic, const
					std::string& message);

			int publish_with_confirm(connection_ptr_t& conn, const std::string& topic,
					const std::string& message, reply_callback_t cb);

			~Manager();

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
		s_manager = new Manager(MAX_CONNECTIONS_DEFAULT, MAX_INFLIGHT_DEFAULT, MAX_QUEUE_DEFAULT, READ_TIMEOUT_MS_DEFAULT, cct);
		return true;
	}

	void shutdown() {
		delete s_manager;
		s_manager = nullptr;
	}

	connection_ptr_t connect(const std::string& url, bool use_ssl, bool verify_ssl,
			boost::optional<const std::string&> ca_location) {
		if (!s_manager) return nullptr;
		return s_manager->connect(url, use_ssl, verify_ssl, ca_location);
	}

	int publish(connection_ptr_t& conn, 
			const std::string& topic,
			const std::string& message) {
		if (!s_manager) return STATUS_MANAGER_STOPPED;
		return s_manager->publish(conn, topic, message);
	}

	int publish_with_confirm(connection_ptr_t& conn, 
			const std::string& topic,
			const std::string& message,
			reply_callback_t cb) {
		if (!s_manager) return STATUS_MANAGER_STOPPED;
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
