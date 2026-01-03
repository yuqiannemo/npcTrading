#include "npcTrading/binance/binance_data_client.hpp"
#include "npcTrading/logger.hpp" // Added Logger
#include "npcTrading/common.hpp"

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <boost/json.hpp>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <queue>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace npcTrading {

namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
namespace net = boost::asio;
namespace ssl = net::ssl;
namespace json = boost::json;
using tcp = net::ip::tcp;

// ============================================================================
// Helper: Convert Timestamp to/from milliseconds
// ============================================================================

static int64_t timestamp_to_ms(Timestamp ts) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        ts.time_since_epoch()).count();
}

static Timestamp ms_to_timestamp(int64_t ms) {
    return Timestamp(std::chrono::milliseconds(ms));
}

// ============================================================================
// Helper: Map BarType.spec() to Binance interval and vice versa
// ============================================================================

static std::string spec_to_binance_interval(const std::string& spec) {
    // Common mappings: "1m", "5m", "15m", "1h", "4h", "1d", etc.
    // Your spec format might be "1-MINUTE", "5-MINUTE", etc.
    // For simplicity, assume spec IS the Binance interval directly.
    // Adjust this if your spec format differs.
    return spec;
}

static std::string binance_interval_to_spec(const std::string& interval) {
    // Reverse mapping - for now, just return the interval as-is
    return interval;
}

// ============================================================================
// Order book state for maintaining local book
// ============================================================================

struct LocalOrderBook {
    InstrumentId instrument_id;
    int64_t last_update_id = 0;
    std::map<double, double, std::greater<double>> bids;  // price -> qty (descending)
    std::map<double, double> asks;                         // price -> qty (ascending)
    int subscribed_depth = 10;
    bool initialized = false;
    
    OrderBook to_order_book(int depth, Timestamp ts) const {
        std::vector<OrderBookLevel> bid_levels;
        std::vector<OrderBookLevel> ask_levels;
        
        int count = 0;
        for (const auto& [price, qty] : bids) {
            if (count++ >= depth) break;
            if (qty > 0) {
                bid_levels.push_back({Price(price), Quantity(qty), 0});
            }
        }
        
        count = 0;
        for (const auto& [price, qty] : asks) {
            if (count++ >= depth) break;
            if (qty > 0) {
                ask_levels.push_back({Price(price), Quantity(qty), 0});
            }
        }
        
        return OrderBook(instrument_id, bid_levels, ask_levels, ts);
    }
};

// ============================================================================
// BinanceDataClientImpl - Hidden implementation with Boost dependencies
// ============================================================================

class BinanceDataClientImpl {
public:
    BinanceDataClientImpl(MessageBus* msgbus, Clock* clock, 
                          const BinanceDataClientConfig& config)
        : msgbus_(msgbus)
        , clock_(clock)
        , config_(config)
        , ssl_ctx_(ssl::context::tlsv12_client)
        , connected_(false)
        , running_(false)
    {
        ssl_ctx_.set_default_verify_paths();
        ssl_ctx_.set_verify_mode(ssl::verify_peer);
    }
    
    ~BinanceDataClientImpl() {
        disconnect();
    }
    
    // ========================================================================
    // Connection Management
    // ========================================================================
    
    void connect() {
        if (running_) return;
        running_ = true;
        
        // Start the IO thread for WebSocket
        io_thread_ = std::thread([this]() {
            run_io_loop();
        });
        
        connected_ = true;
        std::cout << "[BinanceDataClient] Connected" << std::endl;
    }
    
    void disconnect() {
        if (!running_) return;
        running_ = false;
        connected_ = false;
        
        // Stop the io_context
        ioc_.stop();
        
        if (io_thread_.joinable()) {
            io_thread_.join();
        }
        
        // Clear all subscriptions
        {
            std::lock_guard<std::mutex> lock(sub_mutex_);
            quote_subs_.clear();
            trade_subs_.clear();
            bar_subs_.clear();
            book_subs_.clear();
            order_books_.clear();
        }
        
        std::cout << "[BinanceDataClient] Disconnected" << std::endl;
    }
    
    bool is_connected() const {
        return connected_;
    }
    
    // ========================================================================
    // Subscription Management
    // ========================================================================
    
    void subscribe_quotes(const InstrumentId& instrument_id) {
        std::lock_guard<std::mutex> lock(sub_mutex_);
        if (quote_subs_.insert(instrument_id).second) {
            // New subscription - update WS streams
            schedule_ws_resubscribe();
        }
    }
    
    void unsubscribe_quotes(const InstrumentId& instrument_id) {
        std::lock_guard<std::mutex> lock(sub_mutex_);
        if (quote_subs_.erase(instrument_id) > 0) {
            schedule_ws_resubscribe();
        }
    }
    
    void subscribe_trades(const InstrumentId& instrument_id) {
        std::lock_guard<std::mutex> lock(sub_mutex_);
        if (trade_subs_.insert(instrument_id).second) {
            schedule_ws_resubscribe();
        }
    }
    
    void unsubscribe_trades(const InstrumentId& instrument_id) {
        std::lock_guard<std::mutex> lock(sub_mutex_);
        if (trade_subs_.erase(instrument_id) > 0) {
            schedule_ws_resubscribe();
        }
    }
    
    void subscribe_bars(const BarType& bar_type) {
        std::string key = bar_type.instrument_id() + "|" + bar_type.spec();
        std::lock_guard<std::mutex> lock(sub_mutex_);
        if (bar_subs_.insert({key, bar_type}).second) {
            schedule_ws_resubscribe();
        }
    }
    
    void unsubscribe_bars(const BarType& bar_type) {
        std::string key = bar_type.instrument_id() + "|" + bar_type.spec();
        std::lock_guard<std::mutex> lock(sub_mutex_);
        if (bar_subs_.erase(key) > 0) {
            schedule_ws_resubscribe();
        }
    }
    
    void subscribe_order_book(const InstrumentId& instrument_id, int depth) {
        std::lock_guard<std::mutex> lock(sub_mutex_);
        auto it = book_subs_.find(instrument_id);
        if (it == book_subs_.end()) {
            book_subs_[instrument_id] = depth;
            order_books_[instrument_id] = LocalOrderBook{instrument_id, 0, {}, {}, depth, false};
            schedule_ws_resubscribe();
            // Schedule initial snapshot fetch
            schedule_book_snapshot(instrument_id, depth);
        } else if (it->second < depth) {
            it->second = depth;
            order_books_[instrument_id].subscribed_depth = depth;
        }
    }
    
    void unsubscribe_order_book(const InstrumentId& instrument_id) {
        std::lock_guard<std::mutex> lock(sub_mutex_);
        if (book_subs_.erase(instrument_id) > 0) {
            order_books_.erase(instrument_id);
            schedule_ws_resubscribe();
        }
    }
    
    // ========================================================================
    // REST Requests (Synchronous)
    // ========================================================================
    
    std::optional<Instrument> request_instrument(const InstrumentId& instrument_id) {
        try {
            std::string path = "/api/v3/exchangeInfo?symbol=" + instrument_id;
            auto response = http_get(config_.rest_base_url, path);
            
            auto jv = json::parse(response);
            auto& obj = jv.as_object();
            
            if (!obj.contains("symbols") || obj["symbols"].as_array().empty()) {
                return std::nullopt;
            }
            
            auto& sym = obj["symbols"].as_array()[0].as_object();
            
            double tick_size = 0.0;
            double step_size = 0.0;
            double min_qty = 0.0;
            double max_qty = 0.0;
            
            for (auto& filter : sym["filters"].as_array()) {
                auto& f = filter.as_object();
                std::string filter_type(f["filterType"].as_string());
                
                if (filter_type == "PRICE_FILTER") {
                    tick_size = std::stod(std::string(f["tickSize"].as_string()));
                } else if (filter_type == "LOT_SIZE") {
                    step_size = std::stod(std::string(f["stepSize"].as_string()));
                    min_qty = std::stod(std::string(f["minQty"].as_string()));
                    max_qty = std::stod(std::string(f["maxQty"].as_string()));
                }
            }
            
            std::string symbol(sym["symbol"].as_string());
            
            return Instrument(
                instrument_id,
                symbol,
                "BINANCE",  // venue
                tick_size,
                step_size,
                min_qty,
                max_qty,
                clock_->now()
            );
        } catch (const std::exception& e) {
            std::cerr << "[BinanceDataClient] request_instrument error: " << e.what() << std::endl;
            return std::nullopt;
        }
    }
    
    std::vector<Bar> request_bars(const BarType& bar_type, Timestamp start, Timestamp end) {
        std::vector<Bar> all_bars;
        
        try {
            std::string interval = spec_to_binance_interval(bar_type.spec());
            int64_t start_ms = timestamp_to_ms(start);
            int64_t end_ms = timestamp_to_ms(end);
            
            const int limit = 1000;  // Binance max
            
            while (start_ms < end_ms) {
                std::ostringstream path;
                path << "/api/v3/klines?symbol=" << bar_type.instrument_id()
                     << "&interval=" << interval
                     << "&startTime=" << start_ms
                     << "&endTime=" << end_ms
                     << "&limit=" << limit;
                
                auto response = http_get(config_.rest_base_url, path.str());
                auto jv = json::parse(response);
                auto& arr = jv.as_array();
                
                if (arr.empty()) break;
                
                for (auto& kline : arr) {
                    auto& k = kline.as_array();
                    // [openTime, open, high, low, close, volume, closeTime, ...]
                    int64_t open_time = k[0].as_int64();
                    double open = std::stod(std::string(k[1].as_string()));
                    double high = std::stod(std::string(k[2].as_string()));
                    double low = std::stod(std::string(k[3].as_string()));
                    double close = std::stod(std::string(k[4].as_string()));
                    double volume = std::stod(std::string(k[5].as_string()));
                    int64_t close_time = k[6].as_int64();
                    
                    all_bars.emplace_back(
                        bar_type,
                        Price(open),
                        Price(high),
                        Price(low),
                        Price(close),
                        Quantity(volume),
                        ms_to_timestamp(close_time)
                    );
                    
                    start_ms = open_time + 1;  // Next batch starts after this
                }
                
                // If we got fewer than limit, we're done
                if (arr.size() < static_cast<size_t>(limit)) break;
            }
        } catch (const std::exception& e) {
            std::cerr << "[BinanceDataClient] request_bars error: " << e.what() << std::endl;
        }
        
        return all_bars;
    }

private:
    // ========================================================================
    // HTTP GET Helper (Synchronous)
    // ========================================================================
    
    std::string http_get(const std::string& host_url, const std::string& target) {
        try {
            // Parse host from URL
            std::string host = host_url;
            if (host.find("https://") == 0) {
                host = host.substr(8);
            } else if (host.find("http://") == 0) {
                host = host.substr(7);
            }
            
            net::io_context ioc;
            ssl::context ctx(ssl::context::tlsv12_client);
            ctx.set_default_verify_paths();
            
            tcp::resolver resolver(ioc);
            beast::ssl_stream<beast::tcp_stream> stream(ioc, ctx);
            
            // Set SNI hostname
            if (!SSL_set_tlsext_host_name(stream.native_handle(), host.c_str())) {
                throw beast::system_error(
                    beast::error_code(static_cast<int>(::ERR_get_error()),
                                     net::error::get_ssl_category()),
                    "Failed to set SNI hostname");
            }
            
            auto const results = resolver.resolve(host, "443");
            beast::get_lowest_layer(stream).connect(results);
            stream.handshake(ssl::stream_base::client);
            
            http::request<http::string_body> req{http::verb::get, target, 11};
            req.set(http::field::host, host);
            req.set(http::field::user_agent, "npcTrading/1.0");
            
            http::write(stream, req);
            
            beast::flat_buffer buffer;
            http::response<http::string_body> res;
            http::read(stream, buffer, res);
            
            // Graceful shutdown
            beast::error_code ec;
            stream.shutdown(ec);
            
            return res.body();
        } catch (const std::exception& e) {
            std::cerr << "[BinanceDataClient] HTTP GET error: " << e.what() << std::endl;
            throw;
        }
    }
    
    // ========================================================================
    // WebSocket IO Loop
    // ========================================================================
    
    void run_io_loop() {
        while (running_) {
            try {
                // Build stream list from current subscriptions
                std::vector<std::string> streams = build_stream_list();
                
                if (streams.empty()) {
                    // No subscriptions, wait a bit
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    continue;
                }
                
                // Connect to combined streams endpoint
                run_ws_connection(streams);
                
            } catch (const std::exception& e) {
                LOG_ERROR("BinanceDataClient", "WS error: " + std::string(e.what()));
                if (running_) {
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(config_.reconnect_delay_ms));
                }
            }
        }
    }
    
    std::vector<std::string> build_stream_list() {
        std::lock_guard<std::mutex> lock(sub_mutex_);
        std::vector<std::string> streams;
        
        // Quote streams (bookTicker)
        for (const auto& inst : quote_subs_) {
            std::string lower_inst = to_lower(inst);
            streams.push_back(lower_inst + "@bookTicker");
        }
        
        // Trade streams (aggTrade)
        for (const auto& inst : trade_subs_) {
            std::string lower_inst = to_lower(inst);
            streams.push_back(lower_inst + "@aggTrade");
        }
        
        // Bar streams (kline)
        for (const auto& [key, bar_type] : bar_subs_) {
            std::string lower_inst = to_lower(bar_type.instrument_id());
            std::string interval = spec_to_binance_interval(bar_type.spec());
            streams.push_back(lower_inst + "@kline_" + interval);
        }
        
        // Order book streams (depth@100ms)
        for (const auto& [inst, depth] : book_subs_) {
            std::string lower_inst = to_lower(inst);
            streams.push_back(lower_inst + "@depth@100ms");
        }
        
        return streams;
    }
    
    void run_ws_connection(const std::vector<std::string>& streams) {
        net::io_context ioc;
        ssl::context ctx(ssl::context::tlsv12_client);
        ctx.set_default_verify_paths();
        
        tcp::resolver resolver(ioc);
        websocket::stream<beast::ssl_stream<tcp::socket>> ws(ioc, ctx);
        
        std::string host = config_.ws_base_url;
        std::string port = std::to_string(config_.ws_port);
        
        // Build combined streams path
        std::ostringstream path;
        path << "/stream?streams=";
        for (size_t i = 0; i < streams.size(); ++i) {
            if (i > 0) path << "/";
            path << streams[i];
        }
        
        // Resolve and connect
        auto const results = resolver.resolve(host, port);
        auto ep = net::connect(beast::get_lowest_layer(ws), results);
        
        // Set SNI hostname
        if (!SSL_set_tlsext_host_name(ws.next_layer().native_handle(), host.c_str())) {
            throw beast::system_error(
                beast::error_code(static_cast<int>(::ERR_get_error()),
                                 net::error::get_ssl_category()),
                "Failed to set SNI hostname");
        }
        
        // SSL handshake
        ws.next_layer().handshake(ssl::stream_base::client);
        
        // Set up pong handler for ping/pong keepalive
        ws.control_callback([](websocket::frame_type kind, beast::string_view payload) {
            if (kind == websocket::frame_type::ping) {
                // Beast auto-responds to pings with pongs, but log for debug
                // std::cout << "[BinanceDataClient] Received ping" << std::endl;
            }
        });
        
        // WebSocket handshake
        std::string ws_host = host + ":" + port;
        ws.handshake(ws_host, path.str());
        
        std::cout << "[BinanceDataClient] WebSocket connected to " << streams.size() 
                  << " streams" << std::endl;
        
        // Read loop
        beast::flat_buffer buffer;
        while (running_) {
            // Check if we need to resubscribe (streams changed)
            {
                std::lock_guard<std::mutex> lock(sub_mutex_);
                if (needs_resubscribe_) {
                    needs_resubscribe_ = false;
                    break;  // Will reconnect with new streams
                }
            }
            
            ws.read(buffer);
            std::string msg = beast::buffers_to_string(buffer.data());
            buffer.consume(buffer.size());
            
            process_ws_message(msg);
        }
        
        // Close gracefully
        beast::error_code ec;
        ws.close(websocket::close_code::normal, ec);
    }
    
    void process_ws_message(const std::string& msg) {
        try {
            auto jv = json::parse(msg);
            auto& obj = jv.as_object();
            
            // Combined stream format: { "stream": "...", "data": {...} }
            if (!obj.contains("stream") || !obj.contains("data")) {
                return;
            }
            
            std::string stream(obj["stream"].as_string());
            auto& data = obj["data"].as_object();
            
            // Determine stream type and dispatch
            if (stream.find("@bookTicker") != std::string::npos) {
                process_book_ticker(data);
            } else if (stream.find("@aggTrade") != std::string::npos) {
                process_agg_trade(data);
            } else if (stream.find("@kline_") != std::string::npos) {
                process_kline(data);
            } else if (stream.find("@depth") != std::string::npos) {
                process_depth_update(data);
            }
            
        } catch (const std::exception& e) {
            std::cerr << "[BinanceDataClient] Error processing message: " << e.what() << std::endl;
        }
    }
    
    // ========================================================================
    // Stream Event Processors
    // ========================================================================
    
    void process_book_ticker(const json::object& data) {
        // Fields: s=symbol, b=bidPx, B=bidQty, a=askPx, A=askQty
        // E (event time) may be absent in Spot bookTicker
        std::string symbol(data.at("s").as_string());
        double bid_price = std::stod(std::string(data.at("b").as_string()));
        double bid_qty = std::stod(std::string(data.at("B").as_string()));
        double ask_price = std::stod(std::string(data.at("a").as_string()));
        double ask_qty = std::stod(std::string(data.at("A").as_string()));
        
        Timestamp ts = clock_->now();  // E may be absent
        if (data.contains("E")) {
            ts = ms_to_timestamp(data.at("E").as_int64());
        }
        
        auto tick = QuoteTick(
            symbol,
            Price(bid_price),
            Price(ask_price),
            Quantity(bid_qty),
            Quantity(ask_qty),
            ts
        );
        
        auto msg = std::make_shared<QuoteTickMessage>(tick);
        msgbus_->send(Endpoints::DATA_ENGINE_PROCESS, msg);
    }
    
    void process_agg_trade(const json::object& data) {
        // Fields: s=symbol, p=price, q=qty, T=tradeTime, m=buyerIsMaker
        std::string symbol(data.at("s").as_string());
        double price = std::stod(std::string(data.at("p").as_string()));
        double qty = std::stod(std::string(data.at("q").as_string()));
        int64_t trade_time = data.at("T").as_int64();
        bool buyer_is_maker = data.at("m").as_bool();
        
        // If buyer is maker, then seller was aggressor (SELL side aggressed)
        // If buyer is NOT maker, then buyer was aggressor (BUY side aggressed)
        OrderSide aggressor_side = buyer_is_maker ? OrderSide::SELL : OrderSide::BUY;
        
        auto tick = TradeTick(
            symbol,
            Price(price),
            Quantity(qty),
            aggressor_side,
            ms_to_timestamp(trade_time)
        );
        
        auto msg = std::make_shared<TradeTickMessage>(tick);
        msgbus_->send(Endpoints::DATA_ENGINE_PROCESS, msg);
    }
    
    void process_kline(const json::object& data) {
        // Wrapper structure: s=symbol, k={...kline data...}
        std::string symbol(data.at("s").as_string());
        auto& k = data.at("k").as_object();
        
        std::string interval(k.at("i").as_string());
        bool is_closed = k.at("x").as_bool();
        
        // Only emit on close (x=true)
        if (!is_closed) return;
        
        double open = std::stod(std::string(k.at("o").as_string()));
        double high = std::stod(std::string(k.at("h").as_string()));
        double low = std::stod(std::string(k.at("l").as_string()));
        double close = std::stod(std::string(k.at("c").as_string()));
        double volume = std::stod(std::string(k.at("v").as_string()));
        int64_t close_time = k.at("T").as_int64();
        
        std::string spec = binance_interval_to_spec(interval);
        BarType bar_type(symbol, spec);
        
        auto bar = Bar(
            bar_type,
            Price(open),
            Price(high),
            Price(low),
            Price(close),
            Quantity(volume),
            ms_to_timestamp(close_time)
        );
        
        auto msg = std::make_shared<BarMessage>(bar);
        msgbus_->send(Endpoints::DATA_ENGINE_PROCESS, msg);
    }
    
    void process_depth_update(const json::object& data) {
        // Fields: s=symbol, U=firstUpdateId, u=finalUpdateId, b=bids, a=asks, E=eventTime
        std::string symbol(data.at("s").as_string());
        int64_t first_update_id = data.at("U").as_int64();
        int64_t final_update_id = data.at("u").as_int64();
        
        std::lock_guard<std::mutex> lock(sub_mutex_);
        auto it = order_books_.find(symbol);
        if (it == order_books_.end()) return;
        
        auto& book = it->second;
        
        // If not initialized, buffer updates until snapshot arrives
        if (!book.initialized) {
            // For simplicity, just skip until initialized
            LOG_DEBUG("BinanceDataClient", "Book not initialized, skipping WS update U=" + std::to_string(first_update_id) + " u=" + std::to_string(final_update_id));
            return;
        }
        
        // Drop if this update is too old
        if (final_update_id <= book.last_update_id) {
            LOG_DEBUG("BinanceDataClient", "Stale update (u=" + std::to_string(final_update_id) + " <= last=" + std::to_string(book.last_update_id) + "), skipping.");
            return;
        }

        LOG_DEBUG("BinanceDataClient", "Processing depth update U=" + std::to_string(first_update_id) + " u=" + std::to_string(final_update_id));
        
        // Apply bid updates
        for (auto& bid : data.at("b").as_array()) {
            auto& b = bid.as_array();
            double price = std::stod(std::string(b[0].as_string()));
            double qty = std::stod(std::string(b[1].as_string()));
            if (qty == 0) {
                book.bids.erase(price);
            } else {
                book.bids[price] = qty;
            }
        }
        
        // Apply ask updates
        for (auto& ask : data.at("a").as_array()) {
            auto& a = ask.as_array();
            double price = std::stod(std::string(a[0].as_string()));
            double qty = std::stod(std::string(a[1].as_string()));
            if (qty == 0) {
                book.asks.erase(price);
            } else {
                book.asks[price] = qty;
            }
        }
        
        book.last_update_id = final_update_id;
        
        // Emit order book message
        Timestamp ts = clock_->now();
        if (data.contains("E")) {
            ts = ms_to_timestamp(data.at("E").as_int64());
        }
        
        auto ob = book.to_order_book(book.subscribed_depth, ts);
        auto msg = std::make_shared<OrderBookMessage>(ob);
        msgbus_->send(Endpoints::DATA_ENGINE_PROCESS, msg);
    }
    
    // ========================================================================
    // Order Book Snapshot Fetching
    // ========================================================================
    
    void schedule_book_snapshot(const InstrumentId& instrument_id, int depth) {
        // Fetch snapshot in a separate thread to not block
        std::thread([this, instrument_id, depth]() {
            try {
                int limit = std::min(depth * 2, 1000);  // Fetch extra for buffering
                std::ostringstream path;
                path << "/api/v3/depth?symbol=" << instrument_id << "&limit=" << limit;
                
                auto response = http_get(config_.rest_base_url, path.str());
                auto jv = json::parse(response);
                auto& obj = jv.as_object();
                
                int64_t last_update_id = obj["lastUpdateId"].as_int64();
                
                std::lock_guard<std::mutex> lock(sub_mutex_);
                auto it = order_books_.find(instrument_id);
                if (it == order_books_.end()) return;
                
                auto& book = it->second;
                book.bids.clear();
                book.asks.clear();
                
                for (auto& bid : obj["bids"].as_array()) {
                    auto& b = bid.as_array();
                    double price = std::stod(std::string(b[0].as_string()));
                    double qty = std::stod(std::string(b[1].as_string()));
                    book.bids[price] = qty;
                }
                
                for (auto& ask : obj["asks"].as_array()) {
                    auto& a = ask.as_array();
                    double price = std::stod(std::string(a[0].as_string()));
                    double qty = std::stod(std::string(a[1].as_string()));
                    book.asks[price] = qty;
                }
                
                book.last_update_id = last_update_id;
                book.initialized = true;
                
                // Emit initial snapshot
                auto ob = book.to_order_book(book.subscribed_depth, clock_->now());
                auto msg = std::make_shared<OrderBookMessage>(ob);
                msgbus_->send(Endpoints::DATA_ENGINE_PROCESS, msg);
                
            } catch (const std::exception& e) {
                LOG_ERROR("BinanceDataClient", "Error fetching book snapshot: " + std::string(e.what()));
            }
        }).detach();
    }
    
    // ========================================================================
    // Utility
    // ========================================================================
    
    static std::string to_lower(const std::string& s) {
        std::string result = s;
        for (char& c : result) {
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        }
        return result;
    }
    
    void schedule_ws_resubscribe() {
        needs_resubscribe_ = true;
    }
    
    // ========================================================================
    // Members
    // ========================================================================
    
    MessageBus* msgbus_;
    Clock* clock_;
    BinanceDataClientConfig config_;
    
    net::io_context ioc_;
    ssl::context ssl_ctx_;
    std::thread io_thread_;
    
    std::atomic<bool> connected_;
    std::atomic<bool> running_;
    
    // Subscriptions (protected by sub_mutex_)
    std::mutex sub_mutex_;
    std::unordered_set<InstrumentId> quote_subs_;
    std::unordered_set<InstrumentId> trade_subs_;
    std::unordered_map<std::string, BarType> bar_subs_;  // key = instrument|spec
    std::unordered_map<InstrumentId, int> book_subs_;    // instrument -> depth
    std::unordered_map<InstrumentId, LocalOrderBook> order_books_;
    
    bool needs_resubscribe_ = false;
};

// ============================================================================
// BinanceDataClient Public Interface Implementation
// ============================================================================

BinanceDataClient::BinanceDataClient(const ClientId& client_id,
                                     MessageBus* msgbus,
                                     Clock* clock,
                                     const BinanceDataClientConfig& config)
    : DataClient(client_id, "BINANCE")
    , impl_(std::make_unique<BinanceDataClientImpl>(msgbus, clock, config))
    , msgbus_(msgbus)
    , clock_(clock)
    , config_(config)
{
}

BinanceDataClient::~BinanceDataClient() = default;

BinanceDataClient::BinanceDataClient(BinanceDataClient&&) noexcept = default;
BinanceDataClient& BinanceDataClient::operator=(BinanceDataClient&&) noexcept = default;

void BinanceDataClient::subscribe_bars(const BarType& bar_type) {
    impl_->subscribe_bars(bar_type);
}

void BinanceDataClient::unsubscribe_bars(const BarType& bar_type) {
    impl_->unsubscribe_bars(bar_type);
}

void BinanceDataClient::subscribe_quotes(const InstrumentId& instrument_id) {
    impl_->subscribe_quotes(instrument_id);
}

void BinanceDataClient::unsubscribe_quotes(const InstrumentId& instrument_id) {
    impl_->unsubscribe_quotes(instrument_id);
}

void BinanceDataClient::subscribe_trades(const InstrumentId& instrument_id) {
    impl_->subscribe_trades(instrument_id);
}

void BinanceDataClient::unsubscribe_trades(const InstrumentId& instrument_id) {
    impl_->unsubscribe_trades(instrument_id);
}

void BinanceDataClient::subscribe_order_book(const InstrumentId& instrument_id, int depth) {
    impl_->subscribe_order_book(instrument_id, depth);
}

void BinanceDataClient::unsubscribe_order_book(const InstrumentId& instrument_id) {
    impl_->unsubscribe_order_book(instrument_id);
}

std::optional<Instrument> BinanceDataClient::request_instrument(const InstrumentId& instrument_id) {
    return impl_->request_instrument(instrument_id);
}

std::vector<Bar> BinanceDataClient::request_bars(const BarType& bar_type, Timestamp start, Timestamp end) {
    return impl_->request_bars(bar_type, start, end);
}

void BinanceDataClient::connect() {
    impl_->connect();
}

void BinanceDataClient::disconnect() {
    impl_->disconnect();
}

bool BinanceDataClient::is_connected() const {
    return impl_->is_connected();
}

} // namespace npcTrading
