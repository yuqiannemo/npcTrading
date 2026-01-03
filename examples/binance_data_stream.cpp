#include "npcTrading/binance/binance_data_client.hpp"
#include "npcTrading/data_engine.hpp"
#include "npcTrading/message_bus.hpp"
#include "npcTrading/clock.hpp"
#include "npcTrading/cache.hpp"
#include "npcTrading/config.hpp"
#include "npcTrading/logger.hpp"
#include <iostream>
#include <thread>
#include <atomic>
#include <csignal>

using namespace npcTrading;

std::atomic<bool> running{true};

void signal_handler(int signal) {
    LOG_INFO("Main", "Interrupt signal received.");
    running = false;
}

int main() {
    std::signal(SIGINT, signal_handler);

    // 1. Load Config
    std::string config_path = "config.json";
    Config app_config = Config::load(config_path);
    
    auto& logger = Logger::instance();
    if (app_config.system.log_level == "DEBUG") logger.set_level(LogLevel::LVL_DEBUG);
    else if (app_config.system.log_level == "WARN") logger.set_level(LogLevel::LVL_WARN);
    else if (app_config.system.log_level == "ERROR") logger.set_level(LogLevel::LVL_ERROR);
    else logger.set_level(LogLevel::LVL_INFO);

    LOG_INFO("Main", "Starting Binance Data Stream Test...");

    // 2. Core Components
    MessageBusConfig bus_config;
    auto msgbus = std::make_unique<MessageBus>(bus_config);
    auto cache = std::make_unique<Cache>();
    auto clock = std::make_unique<LiveClock>();

    // 3. Data Engine
    DataEngineConfig engine_config;
    auto data_engine = std::make_unique<DataEngine>(msgbus.get(), cache.get(), clock.get(), engine_config);
    data_engine->initialize();

    // 4. Binance Data Client Setup from Config
    BinanceDataClientConfig client_config;
    client_config.api_key = app_config.exchange.binance.api_key;
    client_config.api_secret = app_config.exchange.binance.api_secret;
    client_config.use_testnet = app_config.exchange.binance.use_testnet;
    
    // Explicit overrides from config if present
    if (!app_config.exchange.binance.rest_base_url.empty()) {
        client_config.rest_base_url = app_config.exchange.binance.rest_base_url;
    }
    if (!app_config.exchange.binance.ws_base_url.empty()) {
        client_config.ws_base_url = app_config.exchange.binance.ws_base_url;
    }

    LOG_INFO("Main", "Client Config: Testnet=" + std::to_string(client_config.use_testnet) + 
                     ", BaseURL=" + client_config.rest_base_url);
    
    auto binance_client = std::make_shared<BinanceDataClient>("BinanceClient", msgbus.get(), clock.get(), client_config);
    
    // 5. Register and Connect
    data_engine->register_client(binance_client);
    
    msgbus->start();
    data_engine->start(); 

    LOG_INFO("Main", "Engine Started. Waiting for connection...");
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // 6. Send Subscription Request
    LOG_INFO("Main", "Subscribing to BTCUSDT Order Book (Depth 10)...");
    auto sub_msg = std::make_shared<SubscribeOrderBook>("BTCUSDT", 10);
    msgbus->send(Endpoints::DATA_ENGINE_EXECUTE, sub_msg);

    // 7. Listen for Data
    msgbus->subscribe("MarketData.Book.BTCUSDT", [](const std::shared_ptr<Message>& msg) {
        if (auto book_msg = std::dynamic_pointer_cast<OrderBookMessage>(msg)) {
            const auto& book = book_msg->book();
            std::ostringstream oss;
            oss << "Bid: " << book.best_bid_price().as_double() 
                << " (" << (book.bids().empty() ? 0 : book.bids()[0].size.as_double()) << ")"
                << " | Ask: " << book.best_ask_price().as_double() 
                << " (" << (book.asks().empty() ? 0 : book.asks()[0].size.as_double()) << ")";
            LOG_INFO("Data", oss.str());
            // Use std::cout only for raw data visualization if needed, but LOG is better
        }
    });

    LOG_INFO("Main", "Listening for data...");

    // 8. Event Loop
    while (running) {
        msgbus->run(); 
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    LOG_INFO("Main", "Stopping...");
    data_engine->stop();
    msgbus->stop();

    return 0;
}
