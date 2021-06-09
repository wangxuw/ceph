// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

// TODO: add the details of running this test
#include "rgw/rgw_amqp_1.h"
#include "common/ceph_context.h"
#include <gtest/gtest.h>

#include <iostream>

/*

 how-to running the test

*/


auto cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);

bool test_ok;

class TestAMQP_1 : public ::testing::Test {
	protected:
	// rgw::amqp_1::connection_ptr_t conn = nullptr;

	void SetUp() override {
		ASSERT_TRUE(rgw::amqp_1::init(cct));
	}

	void TearDown() override {
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
	std::cout << "hi gtest and rgw amqp1!" << std::endl;
}

TEST_F(TestAMQP_1, ConnectionOK) {
	const std::string test_broker = "localhost:5672/amqp1_0";
	const auto connection_number = rgw::amqp_1::get_connection_count();
	auto conn = rgw::amqp_1::connect(test_broker, false, boost::none);
	EXPECT_TRUE(conn);
	EXPECT_EQ(rgw::amqp_1::get_connection_count(), connection_number + 1);
}
