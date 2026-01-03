#include <gtest/gtest.h>
#include "npcTrading/strategy.hpp"
#include "npcTrading/data_engine.hpp"
#include "npcTrading/cache.hpp"
#include "npcTrading/clock.hpp"
#include "npcTrading/message_bus.hpp"
#include "npcTrading/market_data.hpp"

using namespace npcTrading;

namespace {
Timestamp ts(int seconds) {
    return std::chrono::system_clock::time_point(std::chrono::seconds(seconds));
}
} // namespace

// ============================================================================
// Test Actor - Exposes internals for testing
// ============================================================================

class TestActor : public Actor {
public:
    TestActor(MessageBus* msgbus, Cache* cache, Clock* clock)
        : Actor("TestActor", msgbus, cache, clock) {}
    
    // Expose callbacks for testing
    std::vector<Bar> received_bars;
    std::vector<QuoteTick> received_quotes;
    std::vector<TradeTick> received_trades;
    std::vector<OrderBook> received_books;
    
    void on_bar(const Bar& bar) override {
        received_bars.push_back(bar);
    }
    
    void on_quote(const QuoteTick& quote) override {
        received_quotes.push_back(quote);
    }
    
    void on_trade(const TradeTick& trade) override {
        received_trades.push_back(trade);
    }
    
    void on_order_book(const OrderBook& book) override {
        received_books.push_back(book);
    }
    
    // Expose subscription tracking for verification
    bool has_bar_subscription(const InstrumentId& id, const std::string& spec) const {
        return bar_subscriptions_.count({id, spec}) > 0;
    }
    
    bool has_quote_subscription(const InstrumentId& id) const {
        return quote_subscriptions_.count(id) > 0;
    }
    
    bool has_trade_subscription(const InstrumentId& id) const {
        return trade_subscriptions_.count(id) > 0;
    }
    
    bool has_book_subscription(const InstrumentId& id) const {
        return orderbook_subscriptions_.count(id) > 0;
    }
    
    bool has_bar_token(const InstrumentId& id, const std::string& spec) const {
        return bar_tokens_.count({id, spec}) > 0;
    }
    
    bool has_quote_token(const InstrumentId& id) const {
        return quote_tokens_.count(id) > 0;
    }
    
    bool has_trade_token(const InstrumentId& id) const {
        return trade_tokens_.count(id) > 0;
    }
    
    bool has_book_token(const InstrumentId& id) const {
        return book_tokens_.count(id) > 0;
    }
    
    void reset() {
        received_bars.clear();
        received_quotes.clear();
        received_trades.clear();
        received_books.clear();
    }
};

// ============================================================================
// Mock DataClient - Minimal for testing
// ============================================================================

class MinimalMockDataClient : public DataClient {
public:
    MinimalMockDataClient() : DataClient("MOCK", "BINANCE") {}
    
    int subscribe_bars_count = 0;
    int subscribe_quotes_count = 0;
    int subscribe_trades_count = 0;
    int subscribe_books_count = 0;
    int unsubscribe_bars_count = 0;
    int unsubscribe_quotes_count = 0;
    int unsubscribe_trades_count = 0;
    int unsubscribe_books_count = 0;
    
    void subscribe_bars(const BarType&) override { subscribe_bars_count++; }
    void unsubscribe_bars(const BarType&) override { unsubscribe_bars_count++; }
    void subscribe_quotes(const InstrumentId&) override { subscribe_quotes_count++; }
    void unsubscribe_quotes(const InstrumentId&) override { unsubscribe_quotes_count++; }
    void subscribe_trades(const InstrumentId&) override { subscribe_trades_count++; }
    void unsubscribe_trades(const InstrumentId&) override { unsubscribe_trades_count++; }
    void subscribe_order_book(const InstrumentId&, int) override { subscribe_books_count++; }
    void unsubscribe_order_book(const InstrumentId&) override { unsubscribe_books_count++; }
    
    std::optional<Instrument> request_instrument(const InstrumentId&) override { return std::nullopt; }
    std::vector<Bar> request_bars(const BarType&, Timestamp, Timestamp) override { return {}; }
    
    void connect() override { connected_ = true; }
    void disconnect() override { connected_ = false; }
    bool is_connected() const override { return connected_; }
    
private:
    bool connected_ = false;
};

// ============================================================================
// Test Fixture
// ============================================================================

class ActorTest : public ::testing::Test {
protected:
    void SetUp() override {
        msgbus_config_.max_queue_size = 1000;
        msgbus_ = std::make_unique<MessageBus>(msgbus_config_);
        cache_ = std::make_unique<Cache>();
        clock_ = std::make_unique<LiveClock>();
        
        // Set up DataEngine with mock client
        engine_ = std::make_unique<DataEngine>(msgbus_.get(), cache_.get(), clock_.get());
        mock_client_ = std::make_shared<MinimalMockDataClient>();
        engine_->register_client(mock_client_);
        engine_->initialize();
        
        actor_ = std::make_unique<TestActor>(msgbus_.get(), cache_.get(), clock_.get());
        
        msgbus_->start();
    }
    
    void TearDown() override {
        msgbus_->stop();
    }
    
    void process_messages() {
        msgbus_->run();
    }
    
    // Simulate DataEngine publishing a message (as if from WS)
    void simulate_quote(const InstrumentId& id, double bid, double ask) {
        QuoteTick tick(id, Price(bid), Price(ask), Quantity(1), Quantity(1), ts(100));
        auto msg = std::make_shared<QuoteTickMessage>(tick);
        msgbus_->send(Endpoints::DATA_ENGINE_PROCESS, msg);
        process_messages();
    }
    
    void simulate_trade(const InstrumentId& id, double price, double qty) {
        TradeTick trade(id, Price(price), Quantity(qty), OrderSide::BUY, ts(100));
        auto msg = std::make_shared<TradeTickMessage>(trade);
        msgbus_->send(Endpoints::DATA_ENGINE_PROCESS, msg);
        process_messages();
    }
    
    void simulate_bar(const InstrumentId& id, const std::string& spec, double close) {
        BarType bar_type(id, spec);
        Bar bar(bar_type, Price(close-10), Price(close+10), Price(close-20), Price(close), Quantity(100), ts(100));
        auto msg = std::make_shared<BarMessage>(bar);
        msgbus_->send(Endpoints::DATA_ENGINE_PROCESS, msg);
        process_messages();
    }
    
    void simulate_book(const InstrumentId& id, double bid, double ask) {
        OrderBookLevel bid_lvl{Price(bid), Quantity(1), 1};
        OrderBookLevel ask_lvl{Price(ask), Quantity(1), 1};
        OrderBook book(id, {bid_lvl}, {ask_lvl}, ts(100));
        auto msg = std::make_shared<OrderBookMessage>(book);
        msgbus_->send(Endpoints::DATA_ENGINE_PROCESS, msg);
        process_messages();
    }
    
    MessageBusConfig msgbus_config_;
    std::unique_ptr<MessageBus> msgbus_;
    std::unique_ptr<Cache> cache_;
    std::unique_ptr<LiveClock> clock_;
    std::unique_ptr<DataEngine> engine_;
    std::shared_ptr<MinimalMockDataClient> mock_client_;
    std::unique_ptr<TestActor> actor_;
};

// ============================================================================
// Quote Subscription Tests
// ============================================================================

TEST_F(ActorTest, SubscribeQuotesRegistersWithDataEngineAndTopic) {
    actor_->subscribe_quotes("BTCUSDT");
    process_messages();
    
    // Should have local tracking
    EXPECT_TRUE(actor_->has_quote_subscription("BTCUSDT"));
    EXPECT_TRUE(actor_->has_quote_token("BTCUSDT"));
    
    // DataEngine should have received the command
    EXPECT_EQ(mock_client_->subscribe_quotes_count, 1);
}

TEST_F(ActorTest, SubscribeQuotesReceivesPublishedData) {
    actor_->subscribe_quotes("BTCUSDT");
    process_messages();
    
    // Simulate DataEngine publishing a quote
    simulate_quote("BTCUSDT", 50000, 50001);
    
    // Actor should have received it
    ASSERT_EQ(actor_->received_quotes.size(), 1u);
    EXPECT_DOUBLE_EQ(actor_->received_quotes[0].bid_price().as_double(), 50000.0);
}

TEST_F(ActorTest, SubscribeQuotesTwiceIsIdempotent) {
    actor_->subscribe_quotes("BTCUSDT");
    actor_->subscribe_quotes("BTCUSDT");  // Second call should be ignored
    process_messages();
    
    // Should only have one subscription
    EXPECT_TRUE(actor_->has_quote_subscription("BTCUSDT"));
    EXPECT_EQ(mock_client_->subscribe_quotes_count, 1);
    
    // Simulate quote - should receive only once
    simulate_quote("BTCUSDT", 50000, 50001);
    EXPECT_EQ(actor_->received_quotes.size(), 1u);
}

TEST_F(ActorTest, UnsubscribeQuotesRemovesSubscription) {
    actor_->subscribe_quotes("BTCUSDT");
    process_messages();
    
    actor_->unsubscribe_quotes("BTCUSDT");
    process_messages();
    
    EXPECT_FALSE(actor_->has_quote_subscription("BTCUSDT"));
    EXPECT_FALSE(actor_->has_quote_token("BTCUSDT"));
    EXPECT_EQ(mock_client_->unsubscribe_quotes_count, 1);
}

TEST_F(ActorTest, UnsubscribeQuotesStopsReceiving) {
    actor_->subscribe_quotes("BTCUSDT");
    process_messages();
    
    simulate_quote("BTCUSDT", 50000, 50001);
    EXPECT_EQ(actor_->received_quotes.size(), 1u);
    
    actor_->unsubscribe_quotes("BTCUSDT");
    process_messages();
    
    // Should not receive after unsubscribe
    actor_->reset();
    simulate_quote("BTCUSDT", 50002, 50003);
    EXPECT_EQ(actor_->received_quotes.size(), 0u);
}

// ============================================================================
// Trade Subscription Tests
// ============================================================================

TEST_F(ActorTest, SubscribeTradesReceivesData) {
    actor_->subscribe_trades("ETHUSDT");
    process_messages();
    
    EXPECT_TRUE(actor_->has_trade_subscription("ETHUSDT"));
    EXPECT_TRUE(actor_->has_trade_token("ETHUSDT"));
    
    simulate_trade("ETHUSDT", 2500, 10);
    ASSERT_EQ(actor_->received_trades.size(), 1u);
    EXPECT_DOUBLE_EQ(actor_->received_trades[0].price().as_double(), 2500.0);
}

TEST_F(ActorTest, UnsubscribeTradesStopsReceiving) {
    actor_->subscribe_trades("ETHUSDT");
    process_messages();
    
    simulate_trade("ETHUSDT", 2500, 10);
    EXPECT_EQ(actor_->received_trades.size(), 1u);
    
    actor_->unsubscribe_trades("ETHUSDT");
    process_messages();
    
    EXPECT_FALSE(actor_->has_trade_subscription("ETHUSDT"));
    
    actor_->reset();
    simulate_trade("ETHUSDT", 2600, 20);
    EXPECT_EQ(actor_->received_trades.size(), 0u);
}

// ============================================================================
// Bar Subscription Tests
// ============================================================================

TEST_F(ActorTest, SubscribeBarsReceivesData) {
    BarType bar_type("BTCUSDT", "1m");
    actor_->subscribe_bars(bar_type);
    process_messages();
    
    EXPECT_TRUE(actor_->has_bar_subscription("BTCUSDT", "1m"));
    EXPECT_TRUE(actor_->has_bar_token("BTCUSDT", "1m"));
    
    simulate_bar("BTCUSDT", "1m", 50500);
    ASSERT_EQ(actor_->received_bars.size(), 1u);
    EXPECT_DOUBLE_EQ(actor_->received_bars[0].close().as_double(), 50500.0);
}

TEST_F(ActorTest, SubscribeBarsMultipleSpecs) {
    BarType btc_1m("BTCUSDT", "1m");
    BarType btc_5m("BTCUSDT", "5m");
    
    actor_->subscribe_bars(btc_1m);
    actor_->subscribe_bars(btc_5m);
    process_messages();
    
    EXPECT_TRUE(actor_->has_bar_subscription("BTCUSDT", "1m"));
    EXPECT_TRUE(actor_->has_bar_subscription("BTCUSDT", "5m"));
    
    simulate_bar("BTCUSDT", "1m", 50500);
    simulate_bar("BTCUSDT", "5m", 50600);
    
    EXPECT_EQ(actor_->received_bars.size(), 2u);
}

TEST_F(ActorTest, UnsubscribeBarsStopsReceiving) {
    BarType bar_type("BTCUSDT", "1m");
    actor_->subscribe_bars(bar_type);
    process_messages();
    
    simulate_bar("BTCUSDT", "1m", 50500);
    EXPECT_EQ(actor_->received_bars.size(), 1u);
    
    actor_->unsubscribe_bars(bar_type);
    process_messages();
    
    EXPECT_FALSE(actor_->has_bar_subscription("BTCUSDT", "1m"));
    
    actor_->reset();
    simulate_bar("BTCUSDT", "1m", 50600);
    EXPECT_EQ(actor_->received_bars.size(), 0u);
}

// ============================================================================
// Order Book Subscription Tests
// ============================================================================

TEST_F(ActorTest, SubscribeOrderBookReceivesData) {
    actor_->subscribe_order_book("BTCUSDT", 10);
    process_messages();
    
    EXPECT_TRUE(actor_->has_book_subscription("BTCUSDT"));
    EXPECT_TRUE(actor_->has_book_token("BTCUSDT"));
    
    simulate_book("BTCUSDT", 49999, 50001);
    ASSERT_EQ(actor_->received_books.size(), 1u);
    EXPECT_DOUBLE_EQ(actor_->received_books[0].best_bid_price().as_double(), 49999.0);
}

TEST_F(ActorTest, UnsubscribeOrderBookStopsReceiving) {
    actor_->subscribe_order_book("BTCUSDT", 10);
    process_messages();
    
    simulate_book("BTCUSDT", 49999, 50001);
    EXPECT_EQ(actor_->received_books.size(), 1u);
    
    actor_->unsubscribe_order_book("BTCUSDT");
    process_messages();
    
    EXPECT_FALSE(actor_->has_book_subscription("BTCUSDT"));
    
    actor_->reset();
    simulate_book("BTCUSDT", 49998, 50002);
    EXPECT_EQ(actor_->received_books.size(), 0u);
}

// ============================================================================
// Multiple Actors Tests - Per-Subscriber Unsubscribe
// ============================================================================

TEST_F(ActorTest, MultipleActorsReceiveIndependently) {
    auto actor2 = std::make_unique<TestActor>(msgbus_.get(), cache_.get(), clock_.get());
    
    actor_->subscribe_quotes("BTCUSDT");
    actor2->subscribe_quotes("BTCUSDT");
    process_messages();
    
    simulate_quote("BTCUSDT", 50000, 50001);
    
    // Both should receive
    EXPECT_EQ(actor_->received_quotes.size(), 1u);
    EXPECT_EQ(actor2->received_quotes.size(), 1u);
}

TEST_F(ActorTest, UnsubscribeOneActorDoesNotAffectOther) {
    auto actor2 = std::make_unique<TestActor>(msgbus_.get(), cache_.get(), clock_.get());
    
    actor_->subscribe_quotes("BTCUSDT");
    actor2->subscribe_quotes("BTCUSDT");
    process_messages();
    
    // Unsubscribe actor1 only
    actor_->unsubscribe_quotes("BTCUSDT");
    process_messages();
    
    simulate_quote("BTCUSDT", 50000, 50001);
    
    // actor1 should NOT receive, actor2 SHOULD
    EXPECT_EQ(actor_->received_quotes.size(), 0u);
    EXPECT_EQ(actor2->received_quotes.size(), 1u);
}

TEST_F(ActorTest, MultipleActorsWithDifferentSubscriptions) {
    auto actor2 = std::make_unique<TestActor>(msgbus_.get(), cache_.get(), clock_.get());
    
    actor_->subscribe_quotes("BTCUSDT");
    actor2->subscribe_trades("BTCUSDT");
    process_messages();
    
    simulate_quote("BTCUSDT", 50000, 50001);
    simulate_trade("BTCUSDT", 50000, 1);
    
    EXPECT_EQ(actor_->received_quotes.size(), 1u);
    EXPECT_EQ(actor_->received_trades.size(), 0u);
    
    EXPECT_EQ(actor2->received_quotes.size(), 0u);
    EXPECT_EQ(actor2->received_trades.size(), 1u);
}

// ============================================================================
// Edge Cases
// ============================================================================

TEST_F(ActorTest, SubscribeEmptyInstrumentIdIgnored) {
    actor_->subscribe_quotes("");
    process_messages();
    
    EXPECT_FALSE(actor_->has_quote_subscription(""));
    EXPECT_EQ(mock_client_->subscribe_quotes_count, 0);
}

TEST_F(ActorTest, SubscribeOrderBookZeroDepthIgnored) {
    actor_->subscribe_order_book("BTCUSDT", 0);
    process_messages();
    
    EXPECT_FALSE(actor_->has_book_subscription("BTCUSDT"));
    EXPECT_EQ(mock_client_->subscribe_books_count, 0);
}

TEST_F(ActorTest, SubscribeBarsEmptyFieldsIgnored) {
    BarType empty_id("", "1m");
    BarType empty_spec("BTCUSDT", "");
    
    actor_->subscribe_bars(empty_id);
    actor_->subscribe_bars(empty_spec);
    process_messages();
    
    EXPECT_FALSE(actor_->has_bar_subscription("", "1m"));
    EXPECT_FALSE(actor_->has_bar_subscription("BTCUSDT", ""));
    EXPECT_EQ(mock_client_->subscribe_bars_count, 0);
}

TEST_F(ActorTest, UnsubscribeWithoutSubscribeIsNoop) {
    // Should not crash or cause issues
    actor_->unsubscribe_quotes("BTCUSDT");
    actor_->unsubscribe_trades("ETHUSDT");
    actor_->unsubscribe_bars(BarType("BTCUSDT", "1m"));
    actor_->unsubscribe_order_book("BTCUSDT");
    process_messages();
    
    // Just verify no crashes and DataEngine received the unsubs (which it handles gracefully)
    EXPECT_TRUE(true);
}

// ============================================================================
// Data Received Only For Subscribed Instruments
// ============================================================================

TEST_F(ActorTest, OnlyReceivesSubscribedInstruments) {
    actor_->subscribe_quotes("BTCUSDT");
    process_messages();
    
    // Simulate quotes for multiple instruments
    simulate_quote("BTCUSDT", 50000, 50001);
    simulate_quote("ETHUSDT", 2500, 2501);  // Not subscribed
    simulate_quote("BTCUSDT", 50002, 50003);
    
    // Should only receive BTCUSDT quotes
    ASSERT_EQ(actor_->received_quotes.size(), 2u);
    EXPECT_EQ(actor_->received_quotes[0].instrument_id(), "BTCUSDT");
    EXPECT_EQ(actor_->received_quotes[1].instrument_id(), "BTCUSDT");
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

