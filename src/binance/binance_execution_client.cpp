#include "npcTrading/binance/binance_execution_client.hpp"
#include "npcTrading/clock.hpp"

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <boost/json.hpp>

#include <openssl/hmac.h>
#include <openssl/sha.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <thread>
#include <unordered_map>

namespace npcTrading {

namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
namespace net = boost::asio;
namespace ssl = net::ssl;
namespace json = boost::json;
using tcp = net::ip::tcp;

// ============================================================================
// Helper: Convert Timestamp to milliseconds since epoch
// ============================================================================

static int64_t timestamp_to_ms(Timestamp ts) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        ts.time_since_epoch()).count();
}

static Timestamp ms_to_timestamp(int64_t ms) {
    return Timestamp(std::chrono::milliseconds(ms));
}

// ============================================================================
// Helper: HMAC-SHA256 signature
// ============================================================================

static std::string hmac_sha256(const std::string& key, const std::string& data) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    unsigned int hash_len = 0;
    
    HMAC(EVP_sha256(),
         key.c_str(), static_cast<int>(key.size()),
         reinterpret_cast<const unsigned char*>(data.c_str()), data.size(),
         hash, &hash_len);
    
    std::ostringstream oss;
    for (unsigned int i = 0; i < hash_len; ++i) {
        oss << std::hex << std::setfill('0') << std::setw(2) << static_cast<int>(hash[i]);
    }
    return oss.str();
}

// ============================================================================
// Helper: URL encode
// ============================================================================

static std::string url_encode(const std::string& value) {
    std::ostringstream escaped;
    escaped.fill('0');
    escaped << std::hex;
    
    for (char c : value) {
        if (isalnum(static_cast<unsigned char>(c)) || c == '-' || c == '_' || c == '.' || c == '~') {
            escaped << c;
        } else {
            escaped << '%' << std::setw(2) << static_cast<int>(static_cast<unsigned char>(c));
        }
    }
    return escaped.str();
}

// ============================================================================
// Helper: Map enums to Binance strings
// ============================================================================

static std::string side_to_binance(OrderSide side) {
    return (side == OrderSide::BUY) ? "BUY" : "SELL";
}

static std::string type_to_binance(OrderType type) {
    switch (type) {
        case OrderType::MARKET: return "MARKET";
        case OrderType::LIMIT: return "LIMIT";
        case OrderType::STOP_MARKET: return "STOP_LOSS";
        case OrderType::STOP_LIMIT: return "STOP_LOSS_LIMIT";
        default: return "LIMIT";
    }
}

static std::string tif_to_binance(TimeInForce tif) {
    switch (tif) {
        case TimeInForce::GTC: return "GTC";
        case TimeInForce::IOC: return "IOC";
        case TimeInForce::FOK: return "FOK";
        default: return "GTC";
    }
}

// ============================================================================
// BinanceExecutionClientImpl - Hidden implementation
// ============================================================================

class BinanceExecutionClientImpl {
public:
    BinanceExecutionClientImpl(const ClientId& client_id,
                               const AccountId& account_id,
                               MessageBus* msgbus,
                               Clock* clock,
                               const BinanceExecutionClientConfig& config)
        : client_id_(client_id)
        , account_id_(account_id)
        , msgbus_(msgbus)
        , clock_(clock)
        , config_(config)
        , ssl_ctx_(ssl::context::tlsv12_client)
        , connected_(false)
        , running_(false)
    {
        ssl_ctx_.set_default_verify_paths();
        ssl_ctx_.set_verify_mode(ssl::verify_peer);
        
        // Set base URLs based on testnet flag if not provided
        if (config_.rest_base_url.empty()) {
            config_.rest_base_url = config_.use_testnet 
                ? "https://testnet.binance.vision"
                : "https://api.binance.com";
        }
        if (config_.ws_base_url.empty()) {
            config_.ws_base_url = config_.use_testnet
                ? "testnet.binance.vision"
                : "stream.binance.com";
        }
    }
    
    ~BinanceExecutionClientImpl() {
        disconnect();
    }
    
    // ========================================================================
    // Connection Management
    // ========================================================================
    
    void connect() {
        if (running_) return;
        running_ = true;
        
        std::string secret_mask = config_.api_key.substr(0, 4) + "****" + config_.api_key.substr(config_.api_key.length() > 4 ? config_.api_key.length() - 4 : 0);
        std::cout << "[DEBUG] Connecting with Key: " << secret_mask << std::endl;

        // Create listen key for user data stream
        listen_key_ = create_listen_key();
        if (listen_key_.empty()) {
            std::cerr << "[BinanceExecutionClient] Failed to create listen key" << std::endl;
            running_ = false;
            return;
        }
        
        // Start WS thread for user data stream
        ws_thread_ = std::thread([this]() {
            run_user_data_stream();
        });
        
        // Start keepalive thread
        keepalive_thread_ = std::thread([this]() {
            run_keepalive_loop();
        });
        
        connected_ = true;
        std::cout << "[BinanceExecutionClient] Connected (testnet=" 
                  << (config_.use_testnet ? "true" : "false") << ")" << std::endl;
    }
    
    void disconnect() {
        if (!running_) return;
        running_ = false;
        connected_ = false;
        
        if (ws_thread_.joinable()) {
            ws_thread_.join();
        }
        if (keepalive_thread_.joinable()) {
            keepalive_thread_.join();
        }
        
        std::cout << "[BinanceExecutionClient] Disconnected" << std::endl;
    }
    
    bool is_connected() const {
        return connected_;
    }
    
    // ========================================================================
    // Order Operations
    // ========================================================================
    
    void submit_order(Order* order) {
        if (!order) return;
        
        // Emit OrderSubmitted immediately
        auto submitted = std::make_shared<OrderSubmitted>(
            std::make_shared<Order>(*order), clock_->now());
        msgbus_->send(Endpoints::EXEC_ENGINE_PROCESS, submitted);
        
        // Track order for WS updates
        {
            std::lock_guard<std::mutex> lock(orders_mutex_);
            orders_[order->order_id()] = std::make_shared<Order>(*order);
        }
        
        // Build request parameters
        std::ostringstream params;
        // Fix: Subtract 2000ms to avoid 'Timestamp ahead of server' error
        auto ts = timestamp_to_ms(clock_->now() - std::chrono::milliseconds(2000));
        
        params << "symbol=" << order->instrument_id()
               << "&side=" << side_to_binance(order->side())
               << "&type=" << type_to_binance(order->type())
               << "&quantity=" << std::fixed << std::setprecision(8) << order->quantity().as_double()
               << "&newClientOrderId=" << url_encode(order->order_id())
               << "&recvWindow=" << config_.recv_window_ms
               << "&timestamp=" << ts;
        
        // Add price and timeInForce for LIMIT orders
        if (order->type() == OrderType::LIMIT) {
            params << "&price=" << std::fixed << std::setprecision(8) << order->price().as_double()
                   << "&timeInForce=" << tif_to_binance(order->time_in_force());
        }
        
        std::string query = params.str();
        std::string signature = hmac_sha256(config_.api_secret, query);
        query += "&signature=" + signature;
        
        // Execute REST request in a separate thread to not block
        std::thread([this, query, order_id = order->order_id()]() {
            try {
                auto response = http_post("/api/v3/order", query);
                process_order_response(order_id, response);
            } catch (const std::exception& e) {
                std::cerr << "[BinanceExecutionClient] Order submit error: " << e.what() << std::endl;
                emit_order_rejected(order_id, e.what());
            }
        }).detach();
    }
    
    void modify_order(Order* /*order*/, Quantity /*new_quantity*/, Price /*new_price*/) {
        // Binance Spot doesn't support native modify - cancel and re-submit
        // For now, just log a warning
        std::cerr << "[BinanceExecutionClient] modify_order not implemented (cancel+replace needed)\n";
    }
    
    void cancel_order(Order* order) {
        if (!order) return;
        
        std::ostringstream params;
        // Fix: Subtract 2000ms to avoid 'Timestamp ahead of server' error
        auto ts = timestamp_to_ms(clock_->now() - std::chrono::milliseconds(2000));

        params << "symbol=" << order->instrument_id()
               << "&origClientOrderId=" << url_encode(order->order_id())
               << "&recvWindow=" << config_.recv_window_ms
               << "&timestamp=" << ts;
        
        std::string query = params.str();
        std::string signature = hmac_sha256(config_.api_secret, query);
        query += "&signature=" + signature;
        
        std::thread([this, query, order_id = order->order_id()]() {
            try {
                auto response = http_delete("/api/v3/order", query);
                process_cancel_response(order_id, response);
            } catch (const std::exception& e) {
                std::cerr << "[BinanceExecutionClient] Order cancel error: " << e.what() << std::endl;
            }
        }).detach();
    }
    
    void query_order(const OrderId& /*order_id*/) {
        // Not implemented for minimal slice
        std::cerr << "[BinanceExecutionClient] query_order not implemented\n";
    }

private:
    // ========================================================================
    // HTTP Helpers
    // ========================================================================
    
    std::string parse_host(const std::string& url) {
        std::string host = url;
        if (host.find("https://") == 0) {
            host = host.substr(8);
        } else if (host.find("http://") == 0) {
            host = host.substr(7);
        }
        return host;
    }
    
    std::string http_post(const std::string& target, const std::string& body) {
        std::string host = parse_host(config_.rest_base_url);
        
        net::io_context ioc;
        ssl::context ctx(ssl::context::tlsv12_client);
        ctx.set_default_verify_paths();
        
        tcp::resolver resolver(ioc);
        beast::ssl_stream<beast::tcp_stream> stream(ioc, ctx);
        
        if (!SSL_set_tlsext_host_name(stream.native_handle(), host.c_str())) {
            throw beast::system_error(
                beast::error_code(static_cast<int>(::ERR_get_error()),
                                 net::error::get_ssl_category()),
                "Failed to set SNI hostname");
        }
        
        auto const results = resolver.resolve(host, "443");
        beast::get_lowest_layer(stream).connect(results);
        stream.handshake(ssl::stream_base::client);
        
        std::string full_target = target + "?" + body;
        http::request<http::string_body> req{http::verb::post, full_target, 11};
        req.set(http::field::host, host);
        req.set(http::field::user_agent, "npcTrading/1.0");
        req.set("X-MBX-APIKEY", config_.api_key);
        req.set(http::field::content_type, "application/x-www-form-urlencoded");
        
        http::write(stream, req);
        
        beast::flat_buffer buffer;
        http::response<http::string_body> res;
        http::read(stream, buffer, res);
        
        beast::error_code ec;
        stream.shutdown(ec);
        
        return res.body();
    }
    
    std::string http_delete(const std::string& target, const std::string& query) {
        std::string host = parse_host(config_.rest_base_url);
        
        net::io_context ioc;
        ssl::context ctx(ssl::context::tlsv12_client);
        ctx.set_default_verify_paths();
        
        tcp::resolver resolver(ioc);
        beast::ssl_stream<beast::tcp_stream> stream(ioc, ctx);
        
        if (!SSL_set_tlsext_host_name(stream.native_handle(), host.c_str())) {
            throw beast::system_error(
                beast::error_code(static_cast<int>(::ERR_get_error()),
                                 net::error::get_ssl_category()),
                "Failed to set SNI hostname");
        }
        
        auto const results = resolver.resolve(host, "443");
        beast::get_lowest_layer(stream).connect(results);
        stream.handshake(ssl::stream_base::client);
        
        std::string full_target = target + "?" + query;
        http::request<http::string_body> req{http::verb::delete_, full_target, 11};
        req.set(http::field::host, host);
        req.set(http::field::user_agent, "npcTrading/1.0");
        req.set("X-MBX-APIKEY", config_.api_key);
        
        http::write(stream, req);
        
        beast::flat_buffer buffer;
        http::response<http::string_body> res;
        http::read(stream, buffer, res);
        
        beast::error_code ec;
        stream.shutdown(ec);
        
        return res.body();
    }
    
    // ========================================================================
    // Listen Key Management
    // ========================================================================
    
    std::string create_listen_key() {
        try {
            std::string host = parse_host(config_.rest_base_url);
            
            net::io_context ioc;
            ssl::context ctx(ssl::context::tlsv12_client);
            ctx.set_default_verify_paths();
            
            tcp::resolver resolver(ioc);
            beast::ssl_stream<beast::tcp_stream> stream(ioc, ctx);
            
            if (!SSL_set_tlsext_host_name(stream.native_handle(), host.c_str())) {
                return "";
            }
            
            auto const results = resolver.resolve(host, "443");
            beast::get_lowest_layer(stream).connect(results);
            stream.handshake(ssl::stream_base::client);
            
            http::request<http::string_body> req{http::verb::post, "/api/v3/userDataStream", 11};
            req.set(http::field::host, host);
            req.set(http::field::user_agent, "npcTrading/1.0");
            req.set("X-MBX-APIKEY", config_.api_key);
            
            http::write(stream, req);
            
            beast::flat_buffer buffer;
            http::response<http::string_body> res;
            http::read(stream, buffer, res);
            
            beast::error_code ec;
            stream.shutdown(ec);
            
            std::cout << "[DEBUG] create_listen_key response: " << res.body() << std::endl;

            auto jv = json::parse(res.body());
            auto& obj = jv.as_object();
            if (obj.contains("listenKey")) {
                return std::string(obj["listenKey"].as_string());
            } else {
                std::cerr << "[DEBUG] Response missing listenKey. Content: " << res.body() << std::endl;
            }
        } catch (const std::exception& e) {
            std::cerr << "[BinanceExecutionClient] create_listen_key error: " << e.what() << std::endl;
        }
        return "";
    }
    
    void keepalive_listen_key() {
        try {
            std::string host = parse_host(config_.rest_base_url);
            
            net::io_context ioc;
            ssl::context ctx(ssl::context::tlsv12_client);
            ctx.set_default_verify_paths();
            
            tcp::resolver resolver(ioc);
            beast::ssl_stream<beast::tcp_stream> stream(ioc, ctx);
            
            if (!SSL_set_tlsext_host_name(stream.native_handle(), host.c_str())) {
                return;
            }
            
            auto const results = resolver.resolve(host, "443");
            beast::get_lowest_layer(stream).connect(results);
            stream.handshake(ssl::stream_base::client);
            
            std::string target = "/api/v3/userDataStream?listenKey=" + listen_key_;
            http::request<http::string_body> req{http::verb::put, target, 11};
            req.set(http::field::host, host);
            req.set(http::field::user_agent, "npcTrading/1.0");
            req.set("X-MBX-APIKEY", config_.api_key);
            
            http::write(stream, req);
            
            beast::flat_buffer buffer;
            http::response<http::string_body> res;
            http::read(stream, buffer, res);
            
            beast::error_code ec;
            stream.shutdown(ec);
        } catch (const std::exception& e) {
            std::cerr << "[BinanceExecutionClient] keepalive error: " << e.what() << std::endl;
        }
    }
    
    void run_keepalive_loop() {
        while (running_) {
            // Keepalive every 30 minutes (listen key expires after 60 min)
            for (int i = 0; i < 1800 && running_; ++i) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
            if (running_) {
                keepalive_listen_key();
            }
        }
    }
    
    // ========================================================================
    // User Data Stream WebSocket
    // ========================================================================
    
    void run_user_data_stream() {
        while (running_) {
            try {
                net::io_context ioc;
                ssl::context ctx(ssl::context::tlsv12_client);
                ctx.set_default_verify_paths();
                
                tcp::resolver resolver(ioc);
                websocket::stream<beast::ssl_stream<tcp::socket>> ws(ioc, ctx);
                
                std::string host = config_.ws_base_url;
                std::string port = std::to_string(config_.ws_port);
                
                auto const results = resolver.resolve(host, port);
                net::connect(beast::get_lowest_layer(ws), results);
                
                if (!SSL_set_tlsext_host_name(ws.next_layer().native_handle(), host.c_str())) {
                    throw beast::system_error(
                        beast::error_code(static_cast<int>(::ERR_get_error()),
                                         net::error::get_ssl_category()),
                        "Failed to set SNI hostname");
                }
                
                ws.next_layer().handshake(ssl::stream_base::client);
                
                std::string ws_path = "/ws/" + listen_key_;
                std::string ws_host = host + ":" + port;
                ws.handshake(ws_host, ws_path);
                
                std::cout << "[BinanceExecutionClient] User data stream connected" << std::endl;
                
                beast::flat_buffer buffer;
                while (running_) {
                    ws.read(buffer);
                    std::string msg = beast::buffers_to_string(buffer.data());
                    buffer.consume(buffer.size());
                    
                    process_user_data_message(msg);
                }
                
                beast::error_code ec;
                ws.close(websocket::close_code::normal, ec);
                
            } catch (const std::exception& e) {
                std::cerr << "[BinanceExecutionClient] WS error: " << e.what() << std::endl;
                if (running_) {
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(config_.reconnect_delay_ms));
                }
            }
        }
    }
    
    void process_user_data_message(const std::string& msg) {
        try {
            auto jv = json::parse(msg);
            auto& obj = jv.as_object();
            
            if (!obj.contains("e")) return;
            
            std::string event_type(obj["e"].as_string());
            
            if (event_type == "executionReport") {
                process_execution_report(obj);
            }
        } catch (const std::exception& e) {
            std::cerr << "[BinanceExecutionClient] Error processing user data: " << e.what() << std::endl;
        }
    }
    
    void process_execution_report(const json::object& data) {
        // c = clientOrderId (our order_id)
        // X = current order status
        // x = execution type (NEW, CANCELED, REPLACED, REJECTED, TRADE, EXPIRED)
        // l = last executed quantity
        // L = last executed price
        // n = commission amount
        // z = cumulative filled quantity
        // Z = cumulative quote asset transacted quantity
        
        std::string client_order_id(data.at("c").as_string());
        std::string exec_type(data.at("x").as_string());
        // Note: order status (X) is available but we use exec_type (x) for routing
        
        std::shared_ptr<Order> order;
        {
            std::lock_guard<std::mutex> lock(orders_mutex_);
            auto it = orders_.find(client_order_id);
            if (it != orders_.end()) {
                order = it->second;
            }
        }
        
        if (!order) {
            // Unknown order - might be from another session
            return;
        }
        
        Timestamp ts = clock_->now();
        if (data.contains("E")) {
            ts = ms_to_timestamp(data.at("E").as_int64());
        }
        
        if (exec_type == "NEW") {
            // Order accepted
            auto event = std::make_shared<OrderAccepted>(order, ts);
            msgbus_->send(Endpoints::EXEC_ENGINE_PROCESS, event);
        } else if (exec_type == "CANCELED") {
            auto event = std::make_shared<OrderCanceled>(order, ts);
            msgbus_->send(Endpoints::EXEC_ENGINE_PROCESS, event);
        } else if (exec_type == "REJECTED") {
            std::string reason = "Order rejected by exchange";
            if (data.contains("r")) {
                reason = std::string(data.at("r").as_string());
            }
            auto event = std::make_shared<OrderRejected>(order, reason, ts);
            msgbus_->send(Endpoints::EXEC_ENGINE_PROCESS, event);
        } else if (exec_type == "TRADE") {
            // Fill
            double last_qty = std::stod(std::string(data.at("l").as_string()));
            double last_price = std::stod(std::string(data.at("L").as_string()));
            
            Fill fill(
                order->order_id(),
                order->instrument_id(),
                Price(last_price),
                Quantity(last_qty),
                order->side(),
                ts
            );
            
            auto event = std::make_shared<OrderFilled>(order, fill, ts);
            msgbus_->send(Endpoints::EXEC_ENGINE_PROCESS, event);
        } else if (exec_type == "EXPIRED") {
            // Treat expired as canceled
            auto event = std::make_shared<OrderCanceled>(order, ts);
            msgbus_->send(Endpoints::EXEC_ENGINE_PROCESS, event);
        }
    }
    
    // ========================================================================
    // Response Processing
    // ========================================================================
    
    void process_order_response(const OrderId& order_id, const std::string& response) {
        try {
            auto jv = json::parse(response);
            auto& obj = jv.as_object();
            
            std::shared_ptr<Order> order;
            {
                std::lock_guard<std::mutex> lock(orders_mutex_);
                auto it = orders_.find(order_id);
                if (it != orders_.end()) {
                    order = it->second;
                }
            }
            
            if (!order) return;
            
            Timestamp ts = clock_->now();
            
            // Check for error
            if (obj.contains("code") && obj.contains("msg")) {
                std::string error_msg(obj["msg"].as_string());
                emit_order_rejected(order_id, error_msg);
                return;
            }
            
            // Order accepted
            auto accepted = std::make_shared<OrderAccepted>(order, ts);
            msgbus_->send(Endpoints::EXEC_ENGINE_PROCESS, accepted);
            
            // Check if already filled (MARKET orders)
            if (obj.contains("status")) {
                std::string status(obj["status"].as_string());
                if (status == "FILLED") {
                    // Emit fill event
                    double exec_qty = std::stod(std::string(obj["executedQty"].as_string()));
                    double avg_price = 0;
                    if (obj.contains("fills") && !obj["fills"].as_array().empty()) {
                        // Calculate average fill price
                        double total_qty = 0;
                        double total_value = 0;
                        for (auto& f : obj["fills"].as_array()) {
                            auto& fill_obj = f.as_object();
                            double qty = std::stod(std::string(fill_obj["qty"].as_string()));
                            double price = std::stod(std::string(fill_obj["price"].as_string()));
                            total_qty += qty;
                            total_value += qty * price;
                        }
                        if (total_qty > 0) {
                            avg_price = total_value / total_qty;
                        }
                    } else if (obj.contains("price")) {
                        avg_price = std::stod(std::string(obj["price"].as_string()));
                    }
                    
                    Fill fill(
                        order->order_id(),
                        order->instrument_id(),
                        Price(avg_price),
                        Quantity(exec_qty),
                        order->side(),
                        ts
                    );
                    
                    auto filled = std::make_shared<OrderFilled>(order, fill, ts);
                    msgbus_->send(Endpoints::EXEC_ENGINE_PROCESS, filled);
                }
            }
            
        } catch (const std::exception& e) {
            std::cerr << "[BinanceExecutionClient] Error processing order response: " << e.what() 
                      << "\nResponse: " << response << std::endl;
            emit_order_rejected(order_id, e.what());
        }
    }
    
    void process_cancel_response(const OrderId& order_id, const std::string& response) {
        try {
            auto jv = json::parse(response);
            auto& obj = jv.as_object();
            
            std::shared_ptr<Order> order;
            {
                std::lock_guard<std::mutex> lock(orders_mutex_);
                auto it = orders_.find(order_id);
                if (it != orders_.end()) {
                    order = it->second;
                }
            }
            
            if (!order) return;
            
            Timestamp ts = clock_->now();
            
            if (obj.contains("code") && obj.contains("msg")) {
                std::cerr << "[BinanceExecutionClient] Cancel failed: " 
                          << std::string(obj["msg"].as_string()) << std::endl;
                return;
            }
            
            auto canceled = std::make_shared<OrderCanceled>(order, ts);
            msgbus_->send(Endpoints::EXEC_ENGINE_PROCESS, canceled);
            
        } catch (const std::exception& e) {
            std::cerr << "[BinanceExecutionClient] Error processing cancel response: " << e.what() << std::endl;
        }
    }
    
    void emit_order_rejected(const OrderId& order_id, const std::string& reason) {
        std::shared_ptr<Order> order;
        {
            std::lock_guard<std::mutex> lock(orders_mutex_);
            auto it = orders_.find(order_id);
            if (it != orders_.end()) {
                order = it->second;
            }
        }
        
        if (order) {
            auto rejected = std::make_shared<OrderRejected>(order, reason, clock_->now());
            msgbus_->send(Endpoints::EXEC_ENGINE_PROCESS, rejected);
        }
    }
    
    // ========================================================================
    // Members
    // ========================================================================
    
    ClientId client_id_;
    AccountId account_id_;
    MessageBus* msgbus_;
    Clock* clock_;
    BinanceExecutionClientConfig config_;
    
    ssl::context ssl_ctx_;
    std::thread ws_thread_;
    std::thread keepalive_thread_;
    
    std::atomic<bool> connected_;
    std::atomic<bool> running_;
    
    std::string listen_key_;
    
    // Order tracking
    std::mutex orders_mutex_;
    std::unordered_map<OrderId, std::shared_ptr<Order>> orders_;
};

// ============================================================================
// BinanceExecutionClientConfig - Load from environment
// ============================================================================

BinanceExecutionClientConfig BinanceExecutionClientConfig::from_env() {
    BinanceExecutionClientConfig config;
    
    if (const char* key = std::getenv("BINANCE_API_KEY")) {
        config.api_key = key;
    }
    if (const char* secret = std::getenv("BINANCE_API_SECRET")) {
        config.api_secret = secret;
    }
    if (const char* testnet = std::getenv("BINANCE_TESTNET")) {
        std::string val(testnet);
        config.use_testnet = (val == "true" || val == "1" || val == "yes");
    }
    if (const char* url = std::getenv("BINANCE_REST_BASE_URL")) {
        config.rest_base_url = url;
    }
    if (const char* recv = std::getenv("BINANCE_RECV_WINDOW_MS")) {
        config.recv_window_ms = std::stoi(recv);
    }
    
    return config;
}

// ============================================================================
// BinanceExecutionClient Public Interface
// ============================================================================

BinanceExecutionClient::BinanceExecutionClient(const ClientId& client_id,
                                               const AccountId& account_id,
                                               MessageBus* msgbus,
                                               Clock* clock,
                                               const BinanceExecutionClientConfig& config)
    : ExecutionClient(client_id, "BINANCE", account_id)
    , impl_(std::make_unique<BinanceExecutionClientImpl>(client_id, account_id, msgbus, clock, config))
    , msgbus_(msgbus)
    , clock_(clock)
    , config_(config)
{
}

BinanceExecutionClient::~BinanceExecutionClient() = default;

BinanceExecutionClient::BinanceExecutionClient(BinanceExecutionClient&&) noexcept = default;
BinanceExecutionClient& BinanceExecutionClient::operator=(BinanceExecutionClient&&) noexcept = default;

void BinanceExecutionClient::submit_order(Order* order) {
    impl_->submit_order(order);
}

void BinanceExecutionClient::modify_order(Order* order, Quantity new_quantity, Price new_price) {
    impl_->modify_order(order, new_quantity, new_price);
}

void BinanceExecutionClient::cancel_order(Order* order) {
    impl_->cancel_order(order);
}

void BinanceExecutionClient::query_order(const OrderId& order_id) {
    impl_->query_order(order_id);
}

void BinanceExecutionClient::connect() {
    impl_->connect();
}

void BinanceExecutionClient::disconnect() {
    impl_->disconnect();
}

bool BinanceExecutionClient::is_connected() const {
    return impl_->is_connected();
}

} // namespace npcTrading

