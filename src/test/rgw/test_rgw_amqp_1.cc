// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw/rgw_amqp_1.h"
#include "common/ceph_context.h"
#include <chrono>
#include <gtest/gtest.h>

#include <iostream>

/*

 how-to running the test

*/


auto cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);

bool test_ok = false;

class TestAMQP_1 : public ::testing::Test {
	protected:
	// rgw::amqp_1::connection_ptr_t conn = nullptr;

	void SetUp() override {
		ASSERT_TRUE(rgw::amqp_1::init(cct));
	}

	void TearDown() override {
		// we have to make sure that we delete Manager after connections are closed.
		// std::this_thread::sleep_for(std::chrono::milliseconds(10));
		rgw::amqp_1::shutdown();
	}
};

void callback(int status) {
	if(status == 0) {
		test_ok = true;
	} else {
		test_ok = false;
	}
}

TEST_F(TestAMQP_1, BuildOK) {
	EXPECT_EQ(true, true);
}

TEST_F(TestAMQP_1, ConnectionOK) {
	const std::string test_broker = "localhost:5672/amqp1_0";
	const auto connection_number = rgw::amqp_1::get_connection_count();
	auto conn = rgw::amqp_1::connect(test_broker, false, boost::none);
	EXPECT_TRUE(conn);
	const auto connection_number_plus = rgw::amqp_1::get_connection_count();
	EXPECT_EQ(connection_number_plus, connection_number + 1);
}

TEST_F(TestAMQP_1, PublishOK) {
	const std::string test_broker = "localhost:5672/amqp1_0";
	auto conn = rgw::amqp_1::connect(test_broker, false, boost::none);
	EXPECT_TRUE(conn);
	auto rc = rgw::amqp_1::publish(conn, "amqp1_0", "new-sample-message");
	EXPECT_EQ(rc, 0);
}

TEST_F(TestAMQP_1, PublishWithCallback) {
	const std::string test_broker = "localhost:5672/amqp1_0";
	auto conn = rgw::amqp_1::connect(test_broker, false, boost::none);
	EXPECT_TRUE(conn);
	auto rc = rgw::amqp_1::publish_with_confirm(conn, "amqp1_0", "sample-message", callback);
	EXPECT_EQ(rc, 0);
	std::this_thread::sleep_for(std::chrono::milliseconds(10));
	EXPECT_EQ(test_ok, true);
}

TEST_F(TestAMQP_1, InvalidHost) {
	const std::string invalid_broker = "27.1.1.2:5673/amqp1_0";
	const auto connection_number = rgw::amqp_1::get_connection_count();
	auto conn = rgw::amqp_1::connect(invalid_broker, false, boost::none);
	EXPECT_TRUE(conn);
	const auto connection_number_plus = rgw::amqp_1::get_connection_count();
	EXPECT_EQ(connection_number_plus, connection_number + 1);
	auto rc = rgw::amqp_1::publish(conn, "amqp1_0", "sample-message");
	EXPECT_LT(rc, 0);
}

// TEST_F(TestAMQP_1, InvalidPort) {
// 	const std::string broker_invalid_port = "localhost:5673/amqp1_0";
// 	auto conn = rgw::amqp_1::connect(broker_invalid_port, false, boost::none);
// }
// 
// TEST_F(TestAMQP_1, UserPassword) {
// }
// 
// TEST_F(TestAMQP_1, URLParseError) {
// }
// 
// TEST_F(TestAMQP_1, MaxConnections) {
// }
// 
