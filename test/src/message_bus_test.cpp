#include <gtest/gtest.h>
#include "npcTrading/message_bus.hpp"
#include <chrono>
#include <thread>

using namespace npcTrading;

// ============================================================================
// Test Message Implementations
// ============================================================================

class TestMessage : public Message {
public:
    explicit TestMessage(const std::string& content, Timestamp ts = Timestamp{}) 
        : content_(content), timestamp_(ts) {}
    
    Timestamp timestamp() const override { return timestamp_; }
    std::string type() const override { return "TestMessage"; }
    std::string content() const { return content_; }
    
private:
    std::string content_;
    Timestamp timestamp_;
};

class TestRequest : public Request {
public:
    explicit TestRequest(const std::string& query, Timestamp ts = Timestamp{})
        : query_(query), timestamp_(ts) {}
    
    Timestamp timestamp() const override { return timestamp_; }
    std::string type() const override { return "TestRequest"; }
    std::string query() const { return query_; }
    
private:
    std::string query_;
    Timestamp timestamp_;
};

class TestResponse : public Response {
public:
    explicit TestResponse(const std::string& result, Timestamp ts = Timestamp{})
        : result_(result), timestamp_(ts) {}
    
    Timestamp timestamp() const override { return timestamp_; }
    std::string type() const override { return "TestResponse"; }
    std::string result() const { return result_; }
    
private:
    std::string result_;
    Timestamp timestamp_;
};

// ============================================================================
// Test Fixtures
// ============================================================================

class MessageBusTest : public ::testing::Test {
protected:
    void SetUp() override {
        config_.max_queue_size = 100;
        config_.persistence_enabled = false;
        bus_ = std::make_unique<MessageBus>(config_);
    }
    
    void TearDown() override {
        if (bus_ && bus_->is_running()) {
            bus_->stop();
        }
    }
    
    MessageBusConfig config_;
    std::unique_ptr<MessageBus> bus_;
};

// ============================================================================
// Basic Functionality Tests
// ============================================================================

TEST_F(MessageBusTest, ConstructorWithValidConfig) {
    EXPECT_FALSE(bus_->is_running());
    EXPECT_EQ(bus_->queue_size(), 0);
    EXPECT_EQ(bus_->total_messages_processed(), 0);
}

TEST_F(MessageBusTest, ConstructorWithInvalidConfig) {
    MessageBusConfig invalid_config;
    invalid_config.max_queue_size = 0;
    
    EXPECT_THROW({
        MessageBus invalid_bus(invalid_config);
    }, std::invalid_argument);
}

TEST_F(MessageBusTest, StartAndStop) {
    EXPECT_FALSE(bus_->is_running());
    
    bus_->start();
    EXPECT_TRUE(bus_->is_running());
    
    bus_->stop();
    EXPECT_FALSE(bus_->is_running());
}

TEST_F(MessageBusTest, DoubleStart) {
    bus_->start();
    EXPECT_TRUE(bus_->is_running());
    
    // Should not throw, just log warning
    EXPECT_NO_THROW(bus_->start());
    EXPECT_TRUE(bus_->is_running());
}

// ============================================================================
// Point-to-Point Messaging Tests
// ============================================================================

TEST_F(MessageBusTest, RegisterAndSendMessage) {
    std::string received_content;
    int call_count = 0;
    
    bus_->register_handler("test.endpoint", [&](const std::shared_ptr<Message>& msg) {
        auto test_msg = std::dynamic_pointer_cast<TestMessage>(msg);
        ASSERT_NE(test_msg, nullptr);
        received_content = test_msg->content();
        call_count++;
    });
    
    bus_->start();
    
    auto msg = std::make_shared<TestMessage>("Hello World");
    bus_->send("test.endpoint", msg);
    
    // Process the queue
    bus_->run();
    
    EXPECT_EQ(call_count, 1);
    EXPECT_EQ(received_content, "Hello World");
    EXPECT_EQ(bus_->total_messages_processed(), 1);
}

TEST_F(MessageBusTest, SendToNonExistentEndpoint) {
    bus_->start();
    
    auto msg = std::make_shared<TestMessage>("Test");
    
    // Should not throw, just log warning
    EXPECT_NO_THROW(bus_->send("nonexistent.endpoint", msg));
    
    bus_->run();
    
    // Message is processed but handler not found
    EXPECT_EQ(bus_->total_messages_processed(), 1);
}

TEST_F(MessageBusTest, SendNullMessage) {
    bus_->register_handler("test.endpoint", [](const std::shared_ptr<Message>&) {});
    bus_->start();
    
    EXPECT_THROW(bus_->send("test.endpoint", nullptr), std::invalid_argument);
}

TEST_F(MessageBusTest, RegisterNullHandler) {
    EXPECT_THROW(bus_->register_handler("test.endpoint", nullptr), std::invalid_argument);
}

TEST_F(MessageBusTest, RegisterEmptyEndpoint) {
    EXPECT_THROW({
        bus_->register_handler("", [](const std::shared_ptr<Message>&) {});
    }, std::invalid_argument);
}

TEST_F(MessageBusTest, UnregisterHandler) {
    int call_count = 0;
    
    bus_->register_handler("test.endpoint", [&](const std::shared_ptr<Message>&) {
        call_count++;
    });
    
    bus_->start();
    
    auto msg = std::make_shared<TestMessage>("Test");
    bus_->send("test.endpoint", msg);
    bus_->run();
    
    EXPECT_EQ(call_count, 1);
    
    // Unregister and send again
    bus_->unregister_handler("test.endpoint");
    bus_->send("test.endpoint", msg);
    bus_->run();
    
    // Should not be called again
    EXPECT_EQ(call_count, 1);
}

// ============================================================================
// Request-Response Tests
// ============================================================================

TEST_F(MessageBusTest, RequestResponse) {
    bus_->register_request_handler("calc.add", [](const std::shared_ptr<Request>& req) 
        -> std::shared_ptr<Response> {
        auto test_req = std::dynamic_pointer_cast<TestRequest>(req);
        std::string result = test_req->query() + " processed";
        return std::make_shared<TestResponse>(result);
    });
    
    auto req = std::make_shared<TestRequest>("compute");
    auto resp = bus_->request("calc.add", req);
    
    ASSERT_NE(resp, nullptr);
    auto test_resp = std::dynamic_pointer_cast<TestResponse>(resp);
    EXPECT_EQ(test_resp->result(), "compute processed");
}

TEST_F(MessageBusTest, RequestToNonExistentEndpoint) {
    auto req = std::make_shared<TestRequest>("test");
    auto resp = bus_->request("nonexistent.endpoint", req);
    
    EXPECT_EQ(resp, nullptr);
}

TEST_F(MessageBusTest, RequestWithNullRequest) {
    bus_->register_request_handler("test.endpoint", 
        [](const std::shared_ptr<Request>&) -> std::shared_ptr<Response> {
            return std::make_shared<TestResponse>("result");
        });
    
    EXPECT_THROW(bus_->request("test.endpoint", nullptr), std::invalid_argument);
}

// ============================================================================
// Pub/Sub Tests
// ============================================================================

TEST_F(MessageBusTest, PublishSubscribe) {
    std::vector<std::string> received_messages;
    
    bus_->subscribe("market.data", [&](const std::shared_ptr<Message>& msg) {
        auto test_msg = std::dynamic_pointer_cast<TestMessage>(msg);
        received_messages.push_back(test_msg->content());
    });
    
    bus_->start();
    
    bus_->publish("market.data", std::make_shared<TestMessage>("Quote 1"));
    bus_->publish("market.data", std::make_shared<TestMessage>("Quote 2"));
    
    bus_->run();
    
    EXPECT_EQ(received_messages.size(), 2);
    EXPECT_EQ(received_messages[0], "Quote 1");
    EXPECT_EQ(received_messages[1], "Quote 2");
}

TEST_F(MessageBusTest, MultipleSubscribers) {
    int subscriber1_count = 0;
    int subscriber2_count = 0;
    
    SubscriptionToken token1 = bus_->subscribe("events", [&](const std::shared_ptr<Message>&) {
        subscriber1_count++;
    });
    
    SubscriptionToken token2 = bus_->subscribe("events", [&](const std::shared_ptr<Message>&) {
        subscriber2_count++;
    });
    
    // Tokens should be different
    EXPECT_NE(token1, INVALID_SUBSCRIPTION_TOKEN);
    EXPECT_NE(token2, INVALID_SUBSCRIPTION_TOKEN);
    EXPECT_NE(token1, token2);
    
    bus_->start();
    
    bus_->publish("events", std::make_shared<TestMessage>("Event 1"));
    bus_->publish("events", std::make_shared<TestMessage>("Event 2"));
    
    bus_->run();
    
    EXPECT_EQ(subscriber1_count, 2);
    EXPECT_EQ(subscriber2_count, 2);
}

TEST_F(MessageBusTest, UnsubscribeByToken) {
    int call_count = 0;
    
    SubscriptionToken token = bus_->subscribe("topic", [&](const std::shared_ptr<Message>&) {
        call_count++;
    });
    
    EXPECT_NE(token, INVALID_SUBSCRIPTION_TOKEN);
    
    bus_->start();
    
    bus_->publish("topic", std::make_shared<TestMessage>("Test 1"));
    bus_->run();
    
    EXPECT_EQ(call_count, 1);
    
    // Unsubscribe by token
    bus_->unsubscribe(token);
    
    bus_->publish("topic", std::make_shared<TestMessage>("Test 2"));
    bus_->run();
    
    // Should not be called again
    EXPECT_EQ(call_count, 1);
}

TEST_F(MessageBusTest, UnsubscribeAllFromTopic) {
    int call_count = 0;
    
    bus_->subscribe("topic", [&](const std::shared_ptr<Message>&) {
        call_count++;
    });
    
    bus_->start();
    
    bus_->publish("topic", std::make_shared<TestMessage>("Test 1"));
    bus_->run();
    
    EXPECT_EQ(call_count, 1);
    
    // Unsubscribe all from topic
    bus_->unsubscribe_all("topic");
    
    bus_->publish("topic", std::make_shared<TestMessage>("Test 2"));
    bus_->run();
    
    // Should not be called again
    EXPECT_EQ(call_count, 1);
}

TEST_F(MessageBusTest, UnsubscribeOneOfMultiple) {
    int subscriber1_count = 0;
    int subscriber2_count = 0;
    
    SubscriptionToken token1 = bus_->subscribe("events", [&](const std::shared_ptr<Message>&) {
        subscriber1_count++;
    });
    
    SubscriptionToken token2 = bus_->subscribe("events", [&](const std::shared_ptr<Message>&) {
        subscriber2_count++;
    });
    
    EXPECT_NE(token1, token2);
    
    bus_->start();
    
    bus_->publish("events", std::make_shared<TestMessage>("Event 1"));
    bus_->run();
    
    EXPECT_EQ(subscriber1_count, 1);
    EXPECT_EQ(subscriber2_count, 1);
    
    // Unsubscribe only subscriber1
    bus_->unsubscribe(token1);
    
    bus_->publish("events", std::make_shared<TestMessage>("Event 2"));
    bus_->run();
    
    // Subscriber1 should not be called, but subscriber2 should be
    EXPECT_EQ(subscriber1_count, 1);
    EXPECT_EQ(subscriber2_count, 2);
}

TEST_F(MessageBusTest, PublishNullMessage) {
    bus_->subscribe("topic", [](const std::shared_ptr<Message>&) {});
    bus_->start();
    
    EXPECT_THROW(bus_->publish("topic", nullptr), std::invalid_argument);
}

TEST_F(MessageBusTest, SubscribeWithEmptyTopic) {
    EXPECT_THROW({
        bus_->subscribe("", [](const std::shared_ptr<Message>&) {});
    }, std::invalid_argument);
}

// ============================================================================
// Queue Management Tests
// ============================================================================

TEST_F(MessageBusTest, QueueSize) {
    bus_->register_handler("test", [](const std::shared_ptr<Message>&) {});
    bus_->start();
    
    EXPECT_EQ(bus_->queue_size(), 0);
    
    bus_->send("test", std::make_shared<TestMessage>("1"));
    bus_->send("test", std::make_shared<TestMessage>("2"));
    bus_->send("test", std::make_shared<TestMessage>("3"));
    
    EXPECT_EQ(bus_->queue_size(), 3);
    
    bus_->run();
    
    EXPECT_EQ(bus_->queue_size(), 0);
    EXPECT_EQ(bus_->total_messages_processed(), 3);
}

TEST_F(MessageBusTest, QueueOverflow) {
    MessageBusConfig small_config;
    small_config.max_queue_size = 2;
    
    auto small_bus = std::make_unique<MessageBus>(small_config);
    small_bus->register_handler("test", [](const std::shared_ptr<Message>&) {});
    small_bus->start();
    
    small_bus->send("test", std::make_shared<TestMessage>("1"));
    small_bus->send("test", std::make_shared<TestMessage>("2"));
    
    EXPECT_EQ(small_bus->queue_size(), 2);
    
    // This should be dropped (queue full)
    small_bus->send("test", std::make_shared<TestMessage>("3"));
    
    EXPECT_EQ(small_bus->queue_size(), 2);
}

TEST_F(MessageBusTest, MessageProcessingOrder) {
    std::vector<std::string> received_order;
    
    bus_->register_handler("test", [&](const std::shared_ptr<Message>& msg) {
        auto test_msg = std::dynamic_pointer_cast<TestMessage>(msg);
        received_order.push_back(test_msg->content());
    });
    
    bus_->start();
    
    bus_->send("test", std::make_shared<TestMessage>("First"));
    bus_->send("test", std::make_shared<TestMessage>("Second"));
    bus_->send("test", std::make_shared<TestMessage>("Third"));
    
    bus_->run();
    
    ASSERT_EQ(received_order.size(), 3);
    EXPECT_EQ(received_order[0], "First");
    EXPECT_EQ(received_order[1], "Second");
    EXPECT_EQ(received_order[2], "Third");
}

// ============================================================================
// Lifecycle Tests
// ============================================================================

TEST_F(MessageBusTest, SendBeforeStart) {
    bus_->register_handler("test", [](const std::shared_ptr<Message>&) {});
    
    auto msg = std::make_shared<TestMessage>("Test");
    
    // Should not throw but log warning
    EXPECT_NO_THROW(bus_->send("test", msg));
    
    EXPECT_EQ(bus_->queue_size(), 0);  // Message not queued
}

TEST_F(MessageBusTest, RunBeforeStart) {
    EXPECT_THROW(bus_->run(), std::runtime_error);
}

TEST_F(MessageBusTest, StopProcessesRemainingMessages) {
    int processed_count = 0;
    
    bus_->register_handler("test", [&](const std::shared_ptr<Message>&) {
        processed_count++;
    });
    
    bus_->start();
    
    bus_->send("test", std::make_shared<TestMessage>("1"));
    bus_->send("test", std::make_shared<TestMessage>("2"));
    bus_->send("test", std::make_shared<TestMessage>("3"));
    
    EXPECT_EQ(bus_->queue_size(), 3);
    
    // Stop should process remaining messages
    bus_->stop();
    
    EXPECT_EQ(bus_->queue_size(), 0);
    EXPECT_EQ(processed_count, 3);
}

// ============================================================================
// Mixed Patterns Test
// ============================================================================

TEST_F(MessageBusTest, MixedSendAndPublish) {
    int send_count = 0;
    int publish_count = 0;
    
    bus_->register_handler("endpoint", [&](const std::shared_ptr<Message>&) {
        send_count++;
    });
    
    bus_->subscribe("topic", [&](const std::shared_ptr<Message>&) {
        publish_count++;
    });
    
    bus_->start();
    
    bus_->send("endpoint", std::make_shared<TestMessage>("Send"));
    bus_->publish("topic", std::make_shared<TestMessage>("Publish"));
    bus_->send("endpoint", std::make_shared<TestMessage>("Send2"));
    
    bus_->run();
    
    EXPECT_EQ(send_count, 2);
    EXPECT_EQ(publish_count, 1);
    EXPECT_EQ(bus_->total_messages_processed(), 3);
}

// ============================================================================
// Error Handling Tests
// ============================================================================

TEST_F(MessageBusTest, HandlerThrowsException) {
    int safe_handler_called = 0;
    
    bus_->register_handler("bad", [](const std::shared_ptr<Message>&) {
        throw std::runtime_error("Handler error");
    });
    
    bus_->register_handler("good", [&](const std::shared_ptr<Message>&) {
        safe_handler_called++;
    });
    
    bus_->start();
    
    bus_->send("bad", std::make_shared<TestMessage>("Bad"));
    bus_->send("good", std::make_shared<TestMessage>("Good"));
    
    // Should not crash, should continue processing
    EXPECT_NO_THROW(bus_->run());
    
    EXPECT_EQ(safe_handler_called, 1);
    EXPECT_EQ(bus_->total_messages_processed(), 2);
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
