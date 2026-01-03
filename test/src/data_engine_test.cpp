#include <gtest/gtest.h>
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
// Mock DataClient for testing DataEngine without network
// ============================================================================

class MockDataClient : public DataClient {
public:
    MockDataClient(const ClientId& id, const VenueId& venue)
        : DataClient(id, venue) {}
    
    // Subscription tracking
    std::vector<BarType> subscribed_bars;
    std::vector<InstrumentId> subscribed_quotes;
    std::vector<InstrumentId> subscribed_trades;
    std::vector<std::pair<InstrumentId, int>> subscribed_books;  // (instrument, depth)
    
    std::vector<BarType> unsubscribed_bars;
    std::vector<InstrumentId> unsubscribed_quotes;
    std::vector<InstrumentId> unsubscribed_trades;
    std::vector<InstrumentId> unsubscribed_books;
    
    // Request responses
    std::optional<Instrument> instrument_response;
    std::vector<Bar> bars_response;
    
    void subscribe_bars(const BarType& bar_type) override {
        subscribed_bars.push_back(bar_type);
    }
    
    void unsubscribe_bars(const BarType& bar_type) override {
        unsubscribed_bars.push_back(bar_type);
    }
    
    void subscribe_quotes(const InstrumentId& instrument_id) override {
        subscribed_quotes.push_back(instrument_id);
    }
    
    void unsubscribe_quotes(const InstrumentId& instrument_id) override {
        unsubscribed_quotes.push_back(instrument_id);
    }
    
    void subscribe_trades(const InstrumentId& instrument_id) override {
        subscribed_trades.push_back(instrument_id);
    }
    
    void unsubscribe_trades(const InstrumentId& instrument_id) override {
        unsubscribed_trades.push_back(instrument_id);
    }
    
    void subscribe_order_book(const InstrumentId& instrument_id, int depth) override {
        subscribed_books.push_back({instrument_id, depth});
    }
    
    void unsubscribe_order_book(const InstrumentId& instrument_id) override {
        unsubscribed_books.push_back(instrument_id);
    }
    
    std::optional<Instrument> request_instrument(const InstrumentId&) override {
        return instrument_response;
    }
    
    std::vector<Bar> request_bars(const BarType&, Timestamp, Timestamp) override {
        return bars_response;
    }
    
    void connect() override { connected_ = true; }
    void disconnect() override { connected_ = false; }
    bool is_connected() const override { return connected_; }
    
    void reset() {
        subscribed_bars.clear();
        subscribed_quotes.clear();
        subscribed_trades.clear();
        subscribed_books.clear();
        unsubscribed_bars.clear();
        unsubscribed_quotes.clear();
        unsubscribed_trades.clear();
        unsubscribed_books.clear();
    }
    
private:
    bool connected_ = false;
};

// ============================================================================
// Test Fixture
// ============================================================================

class DataEngineTest : public ::testing::Test {
protected:
    void SetUp() override {
        msgbus_config_.max_queue_size = 1000;
        msgbus_ = std::make_unique<MessageBus>(msgbus_config_);
        cache_ = std::make_unique<Cache>();
        clock_ = std::make_unique<LiveClock>();
        
        engine_ = std::make_unique<DataEngine>(msgbus_.get(), cache_.get(), clock_.get());
        
        mock_client_ = std::make_shared<MockDataClient>("MOCK", "BINANCE");
        engine_->register_client(mock_client_);
        
        engine_->initialize();
        msgbus_->start();
    }
    
    void TearDown() override {
        msgbus_->stop();
    }
    
    // Helper to process queued messages
    void process_messages() {
        msgbus_->run();
    }
    
    MessageBusConfig msgbus_config_;
    std::unique_ptr<MessageBus> msgbus_;
    std::unique_ptr<Cache> cache_;
    std::unique_ptr<LiveClock> clock_;
    std::unique_ptr<DataEngine> engine_;
    std::shared_ptr<MockDataClient> mock_client_;
};

// ============================================================================
// Quote Subscription Refcount Tests
// ============================================================================

TEST_F(DataEngineTest, SubscribeQuotesCallsClientOnce) {
    auto cmd = std::make_shared<SubscribeQuotes>("BTCUSDT");
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE, cmd);
    process_messages();
    
    ASSERT_EQ(mock_client_->subscribed_quotes.size(), 1u);
    EXPECT_EQ(mock_client_->subscribed_quotes[0], "BTCUSDT");
}

TEST_F(DataEngineTest, SubscribeQuotesTwiceOnlyCallsClientOnce) {
    auto cmd1 = std::make_shared<SubscribeQuotes>("BTCUSDT");
    auto cmd2 = std::make_shared<SubscribeQuotes>("BTCUSDT");
    
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE, cmd1);
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE, cmd2);
    process_messages();
    
    // Client should only be called once (refcount incremented)
    EXPECT_EQ(mock_client_->subscribed_quotes.size(), 1u);
}

TEST_F(DataEngineTest, UnsubscribeQuotesOnlyWhenRefcountZero) {
    auto sub1 = std::make_shared<SubscribeQuotes>("BTCUSDT");
    auto sub2 = std::make_shared<SubscribeQuotes>("BTCUSDT");
    auto unsub = std::make_shared<UnsubscribeQuotes>("BTCUSDT");
    
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE, sub1);
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE, sub2);
    process_messages();
    
    // First unsubscribe: refcount 2 -> 1, client NOT called
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE, unsub);
    process_messages();
    EXPECT_EQ(mock_client_->unsubscribed_quotes.size(), 0u);
    
    // Second unsubscribe: refcount 1 -> 0, client IS called
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE, unsub);
    process_messages();
    EXPECT_EQ(mock_client_->unsubscribed_quotes.size(), 1u);
    EXPECT_EQ(mock_client_->unsubscribed_quotes[0], "BTCUSDT");
}

// ============================================================================
// Trade Subscription Refcount Tests
// ============================================================================

TEST_F(DataEngineTest, SubscribeTradesRefcounting) {
    auto sub1 = std::make_shared<SubscribeTrades>("ETHUSDT");
    auto sub2 = std::make_shared<SubscribeTrades>("ETHUSDT");
    
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE, sub1);
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE, sub2);
    process_messages();
    
    EXPECT_EQ(mock_client_->subscribed_trades.size(), 1u);
    
    auto unsub = std::make_shared<UnsubscribeTrades>("ETHUSDT");
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE, unsub);
    process_messages();
    EXPECT_EQ(mock_client_->unsubscribed_trades.size(), 0u);
    
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE, unsub);
    process_messages();
    EXPECT_EQ(mock_client_->unsubscribed_trades.size(), 1u);
}

// ============================================================================
// Bar Subscription Refcount Tests
// ============================================================================

TEST_F(DataEngineTest, SubscribeBarsRefcounting) {
    BarType btc_1m("BTCUSDT", "1m");
    
    auto sub1 = std::make_shared<SubscribeBars>(btc_1m);
    auto sub2 = std::make_shared<SubscribeBars>(btc_1m);
    
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE, sub1);
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE, sub2);
    process_messages();
    
    EXPECT_EQ(mock_client_->subscribed_bars.size(), 1u);
    
    auto unsub = std::make_shared<UnsubscribeBars>(btc_1m);
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE, unsub);
    process_messages();
    EXPECT_EQ(mock_client_->unsubscribed_bars.size(), 0u);
    
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE, unsub);
    process_messages();
    EXPECT_EQ(mock_client_->unsubscribed_bars.size(), 1u);
}

TEST_F(DataEngineTest, SubscribeBarsKeyIncludesInstrument) {
    // Different instruments with same spec should be independent subscriptions
    BarType btc_1m("BTCUSDT", "1m");
    BarType eth_1m("ETHUSDT", "1m");
    
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE, std::make_shared<SubscribeBars>(btc_1m));
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE, std::make_shared<SubscribeBars>(eth_1m));
    process_messages();
    
    EXPECT_EQ(mock_client_->subscribed_bars.size(), 2u);
}

// ============================================================================
// Order Book Subscription Tests
// ============================================================================

TEST_F(DataEngineTest, SubscribeOrderBookRefcounting) {
    auto sub1 = std::make_shared<SubscribeOrderBook>("BTCUSDT", 10);
    auto sub2 = std::make_shared<SubscribeOrderBook>("BTCUSDT", 10);
    
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE, sub1);
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE, sub2);
    process_messages();
    
    // Should only subscribe once
    EXPECT_EQ(mock_client_->subscribed_books.size(), 1u);
    EXPECT_EQ(mock_client_->subscribed_books[0].second, 10);
}

TEST_F(DataEngineTest, SubscribeOrderBookDepthUpgrade) {
    // First subscriber wants depth 5
    auto sub1 = std::make_shared<SubscribeOrderBook>("BTCUSDT", 5);
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE, sub1);
    process_messages();
    
    ASSERT_EQ(mock_client_->subscribed_books.size(), 1u);
    EXPECT_EQ(mock_client_->subscribed_books[0].second, 5);
    
    // Second subscriber wants depth 20 - should resubscribe with higher depth
    auto sub2 = std::make_shared<SubscribeOrderBook>("BTCUSDT", 20);
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE, sub2);
    process_messages();
    
    // Should have unsubscribed and resubscribed with new depth
    EXPECT_EQ(mock_client_->unsubscribed_books.size(), 1u);
    EXPECT_EQ(mock_client_->subscribed_books.size(), 2u);
    EXPECT_EQ(mock_client_->subscribed_books[1].second, 20);
}

// ============================================================================
// Process Tests - Cache First Then Publish
// ============================================================================

TEST_F(DataEngineTest, ProcessQuoteCachesAndPublishes) {
    std::vector<QuoteTick> received_quotes;
    msgbus_->subscribe("MarketData.Quote.BTCUSDT", [&](const std::shared_ptr<Message>& msg) {
        auto quote_msg = std::dynamic_pointer_cast<QuoteTickMessage>(msg);
        if (quote_msg) {
            received_quotes.push_back(quote_msg->tick());
        }
    });
    
    QuoteTick tick("BTCUSDT", Price(50000), Price(50001), Quantity(1), Quantity(2), ts(100));
    auto msg = std::make_shared<QuoteTickMessage>(tick);
    
    msgbus_->send(Endpoints::DATA_ENGINE_PROCESS, msg);
    process_messages();
    
    // Check cache
    const QuoteTick* cached = cache_->quote_tick("BTCUSDT");
    ASSERT_NE(cached, nullptr);
    EXPECT_DOUBLE_EQ(cached->bid_price().as_double(), 50000.0);
    
    // Check published
    ASSERT_EQ(received_quotes.size(), 1u);
    EXPECT_DOUBLE_EQ(received_quotes[0].bid_price().as_double(), 50000.0);
}

TEST_F(DataEngineTest, ProcessTradeCachesAndPublishes) {
    std::vector<TradeTick> received_trades;
    msgbus_->subscribe("MarketData.Trade.ETHUSDT", [&](const std::shared_ptr<Message>& msg) {
        auto trade_msg = std::dynamic_pointer_cast<TradeTickMessage>(msg);
        if (trade_msg) {
            received_trades.push_back(trade_msg->trade());
        }
    });
    
    TradeTick trade("ETHUSDT", Price(2500), Quantity(10), OrderSide::BUY, ts(200));
    auto msg = std::make_shared<TradeTickMessage>(trade);
    
    msgbus_->send(Endpoints::DATA_ENGINE_PROCESS, msg);
    process_messages();
    
    // Check cache
    const TradeTick* cached = cache_->trade_tick("ETHUSDT");
    ASSERT_NE(cached, nullptr);
    EXPECT_DOUBLE_EQ(cached->price().as_double(), 2500.0);
    
    // Check published
    ASSERT_EQ(received_trades.size(), 1u);
}

TEST_F(DataEngineTest, ProcessBarCachesAndPublishes) {
    BarType bar_type("BTCUSDT", "1m");
    std::vector<Bar> received_bars;
    
    msgbus_->subscribe("MarketData.Bar.BTCUSDT|1m", [&](const std::shared_ptr<Message>& msg) {
        auto bar_msg = std::dynamic_pointer_cast<BarMessage>(msg);
        if (bar_msg) {
            received_bars.push_back(bar_msg->bar());
        }
    });
    
    Bar bar(bar_type, Price(50000), Price(51000), Price(49500), Price(50500), Quantity(100), ts(300));
    auto msg = std::make_shared<BarMessage>(bar);
    
    msgbus_->send(Endpoints::DATA_ENGINE_PROCESS, msg);
    process_messages();
    
    // Check cache
    const Bar* cached = cache_->bar(bar_type);
    ASSERT_NE(cached, nullptr);
    EXPECT_DOUBLE_EQ(cached->close().as_double(), 50500.0);
    
    // Check published
    ASSERT_EQ(received_bars.size(), 1u);
    EXPECT_DOUBLE_EQ(received_bars[0].close().as_double(), 50500.0);
}

TEST_F(DataEngineTest, ProcessOrderBookCachesAndPublishes) {
    std::vector<OrderBook> received_books;
    msgbus_->subscribe("MarketData.Book.BTCUSDT", [&](const std::shared_ptr<Message>& msg) {
        auto book_msg = std::dynamic_pointer_cast<OrderBookMessage>(msg);
        if (book_msg) {
            received_books.push_back(book_msg->book());
        }
    });
    
    OrderBookLevel bid{Price(49999), Quantity(5), 3};
    OrderBookLevel ask{Price(50001), Quantity(4), 2};
    OrderBook book("BTCUSDT", {bid}, {ask}, ts(400));
    auto msg = std::make_shared<OrderBookMessage>(book);
    
    msgbus_->send(Endpoints::DATA_ENGINE_PROCESS, msg);
    process_messages();
    
    // Check cache
    const OrderBook* cached = cache_->order_book("BTCUSDT", "default");
    ASSERT_NE(cached, nullptr);
    EXPECT_DOUBLE_EQ(cached->best_bid_price().as_double(), 49999.0);
    
    // Check published
    ASSERT_EQ(received_books.size(), 1u);
}

// ============================================================================
// Historical Alias Tests
// ============================================================================

TEST_F(DataEngineTest, ProcessHistoricalAliasWorks) {
    // process_historical should behave exactly like process
    std::vector<Bar> received_bars;
    BarType bar_type("BTCUSDT", "5m");
    
    msgbus_->subscribe("MarketData.Bar.BTCUSDT|5m", [&](const std::shared_ptr<Message>& msg) {
        auto bar_msg = std::dynamic_pointer_cast<BarMessage>(msg);
        if (bar_msg) {
            received_bars.push_back(bar_msg->bar());
        }
    });
    
    Bar bar(bar_type, Price(50000), Price(51000), Price(49500), Price(50500), Quantity(100), ts(500));
    auto msg = std::make_shared<BarMessage>(bar);
    
    // Send to historical endpoint
    msgbus_->send(Endpoints::DATA_ENGINE_PROCESS_HISTORICAL, msg);
    process_messages();
    
    // Should work identically to regular process
    const Bar* cached = cache_->bar(bar_type);
    ASSERT_NE(cached, nullptr);
    EXPECT_DOUBLE_EQ(cached->close().as_double(), 50500.0);
    
    ASSERT_EQ(received_bars.size(), 1u);
}

// ============================================================================
// Request/Response Tests
// ============================================================================

TEST_F(DataEngineTest, RequestInstrumentReturnsAndCaches) {
    mock_client_->instrument_response = Instrument(
        "BTCUSDT", "BTCUSDT", "BINANCE", 0.01, 0.001, 0.001, 1000, ts(100));
    
    auto req = std::make_shared<RequestInstrument>("BTCUSDT", "BINANCE");
    auto resp = msgbus_->request(Endpoints::DATA_ENGINE_REQUEST, req);
    
    ASSERT_NE(resp, nullptr);
    auto instr_resp = std::dynamic_pointer_cast<InstrumentResponse>(resp);
    ASSERT_NE(instr_resp, nullptr);
    ASSERT_TRUE(instr_resp->instrument().has_value());
    EXPECT_EQ(instr_resp->instrument()->id(), "BTCUSDT");
    
    // Check cached
    const Instrument* cached = cache_->instrument("BTCUSDT");
    ASSERT_NE(cached, nullptr);
    EXPECT_EQ(cached->id(), "BTCUSDT");
}

TEST_F(DataEngineTest, RequestInstrumentNotFound) {
    mock_client_->instrument_response = std::nullopt;
    
    auto req = std::make_shared<RequestInstrument>("UNKNOWN", "BINANCE");
    auto resp = msgbus_->request(Endpoints::DATA_ENGINE_REQUEST, req);
    
    ASSERT_NE(resp, nullptr);
    auto instr_resp = std::dynamic_pointer_cast<InstrumentResponse>(resp);
    ASSERT_NE(instr_resp, nullptr);
    EXPECT_FALSE(instr_resp->instrument().has_value());
}

TEST_F(DataEngineTest, RequestBarsReturnsAndCaches) {
    BarType bar_type("BTCUSDT", "1h");
    
    mock_client_->bars_response = {
        Bar(bar_type, Price(50000), Price(51000), Price(49000), Price(50500), Quantity(100), ts(1000)),
        Bar(bar_type, Price(50500), Price(52000), Price(50000), Price(51500), Quantity(120), ts(2000)),
    };
    
    // Subscribe to topic to receive published bars
    std::vector<Bar> published_bars;
    msgbus_->subscribe("MarketData.Bar.BTCUSDT|1h", [&](const std::shared_ptr<Message>& msg) {
        auto bar_msg = std::dynamic_pointer_cast<BarMessage>(msg);
        if (bar_msg) {
            published_bars.push_back(bar_msg->bar());
        }
    });
    
    auto req = std::make_shared<RequestBarsRange>(bar_type, ts(0), ts(3000));
    auto resp = msgbus_->request(Endpoints::DATA_ENGINE_REQUEST, req);
    
    ASSERT_NE(resp, nullptr);
    auto bars_resp = std::dynamic_pointer_cast<BarsResponse>(resp);
    ASSERT_NE(bars_resp, nullptr);
    EXPECT_EQ(bars_resp->bars().size(), 2u);
    
    // Process published messages
    process_messages();
    
    // Check published (cache-first publish)
    EXPECT_EQ(published_bars.size(), 2u);
    
    // Check cached (latest bar)
    const Bar* cached = cache_->bar(bar_type);
    ASSERT_NE(cached, nullptr);
    EXPECT_DOUBLE_EQ(cached->close().as_double(), 51500.0);
}

// ============================================================================
// Client Registration Tests
// ============================================================================

TEST_F(DataEngineTest, FirstClientBecomesDefault) {
    // mock_client_ was registered first in SetUp, should be default
    auto* client = engine_->get_client("MOCK");
    ASSERT_NE(client, nullptr);
    EXPECT_EQ(client->client_id(), "MOCK");
}

TEST_F(DataEngineTest, GetClientForVenue) {
    auto* client = engine_->get_client_for_venue("BINANCE");
    ASSERT_NE(client, nullptr);
    EXPECT_EQ(client->venue(), "BINANCE");
}

TEST_F(DataEngineTest, MultipleClients) {
    auto second_client = std::make_shared<MockDataClient>("MOCK2", "COINBASE");
    engine_->register_client(second_client);
    
    auto* c1 = engine_->get_client_for_venue("BINANCE");
    auto* c2 = engine_->get_client_for_venue("COINBASE");
    
    ASSERT_NE(c1, nullptr);
    ASSERT_NE(c2, nullptr);
    EXPECT_EQ(c1->client_id(), "MOCK");
    EXPECT_EQ(c2->client_id(), "MOCK2");
}

// ============================================================================
// Engine Lifecycle Tests
// ============================================================================

TEST_F(DataEngineTest, StartConnectsClients) {
    EXPECT_FALSE(mock_client_->is_connected());
    
    engine_->start();
    
    EXPECT_TRUE(mock_client_->is_connected());
}

TEST_F(DataEngineTest, StopDisconnectsClients) {
    engine_->start();
    EXPECT_TRUE(mock_client_->is_connected());
    
    engine_->stop();
    
    EXPECT_FALSE(mock_client_->is_connected());
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

