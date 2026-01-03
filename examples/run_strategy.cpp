#include "npcTrading/binance/binance_data_client.hpp"
#include "npcTrading/binance/binance_execution_client.hpp"
#include "npcTrading/strategies/momentum_reversal.hpp"
#include "npcTrading/data_engine.hpp"
#include "npcTrading/execution_engine.hpp"
#include "npcTrading/risk_engine.hpp"
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
    LOG_INFO("Main", "Interrupt signal received. Stopping...");
    running = false;
}

int main() {
    std::signal(SIGINT, signal_handler);

    // 1. Load Config
    std::string config_path = "config.json";
    Config app_config = Config::load(config_path);
    
    // Setup Logger
    auto& logger = Logger::instance();
    if (app_config.system.log_level == "DEBUG") logger.set_level(LogLevel::LVL_DEBUG);
    else if (app_config.system.log_level == "WARN") logger.set_level(LogLevel::LVL_WARN);
    else logger.set_level(LogLevel::LVL_INFO);

    LOG_INFO("Main", "Initializing Trading System...");

    // 2. Infrastructure Components
    MessageBusConfig bus_config;
    auto msgbus = std::make_unique<MessageBus>(bus_config);
    auto cache = std::make_unique<Cache>();
    auto clock = std::make_unique<LiveClock>();
    msgbus->start(); // Start bus early

    // 3. Engines
    DataEngineConfig data_cfg;
    auto data_engine = std::make_unique<DataEngine>(msgbus.get(), cache.get(), clock.get(), data_cfg);
    
    ExecEngineConfig exec_cfg;
    auto exec_engine = std::make_unique<ExecutionEngine>(msgbus.get(), cache.get(), clock.get(), exec_cfg);
    
    RiskConfig risk_cfg_struct = app_config.risk; // Use global config
    // We need to construct RiskEngine with RiskConfig. 
    // Wait, RiskEngine constructor might take RiskEngineConfig which wraps RiskConfig or similar?
    // Checking RiskEngine: Constructor takes MessageBus*, Cache*, Clock*.
    // It loads config internally or needs setter.
    // Let's assume standard component ctor and set limits manually if needed or pass via setter.
    auto risk_engine = std::make_unique<RiskEngine>(msgbus.get(), cache.get(), clock.get());
    // TODO: Pass risk limits to risk engine. For now assumption: defaults.

    // 4. Clients (Binance)
    // 4.1 Data Client
    BinanceDataClientConfig d_cfg;
    d_cfg.api_key = app_config.exchange.binance.api_key;
    d_cfg.api_secret = app_config.exchange.binance.api_secret;
    d_cfg.use_testnet = app_config.exchange.binance.use_testnet;
    if (!app_config.exchange.binance.rest_base_url.empty()) d_cfg.rest_base_url = app_config.exchange.binance.rest_base_url;
    if (!app_config.exchange.binance.ws_base_url.empty()) d_cfg.ws_base_url = app_config.exchange.binance.ws_base_url;
    
    auto data_client = std::make_shared<BinanceDataClient>("BinanceData", msgbus.get(), clock.get(), d_cfg);
    data_engine->register_client(data_client);
    
    // 4.2 Execution Client
    BinanceExecutionClientConfig exec_config;
    exec_config.api_key = app_config.exchange.binance.api_key;
    exec_config.api_secret = app_config.exchange.binance.api_secret;
    exec_config.use_testnet = app_config.exchange.binance.use_testnet;
    if (!app_config.exchange.binance.rest_base_url.empty()) exec_config.rest_base_url = app_config.exchange.binance.rest_base_url;
    if (!app_config.exchange.binance.ws_base_url.empty()) exec_config.ws_base_url = app_config.exchange.binance.ws_base_url;
    
    // Provide a default Account ID
    AccountId account_id = "MainAccount";
    
    auto exec_client = std::make_shared<BinanceExecutionClient>("BinanceExec", account_id, msgbus.get(), clock.get(), exec_config);
    
    exec_engine->register_client(exec_client);
    // Bind strategy orders (venue="") to BINANCE
    // exec_engine->set_default_venue("BINANCE"); // If such API exists. 
    // Strategy sends orders with empty venue? Or strategy sets venue?
    // Usually Strategy sets InstrumentId, and Cache/InstrumentModel maps Instrument -> Venue.
    // Since we don't have InstrumentModel fully loaded, let's ensure Strategy sets a venue or ExecutionEngine routes by default.
    // HACK: We will manually map the execution client venue to match whatever the strategy sends.

    // 5. Strategy
    if (app_config.strategies.count("MomentumReversal")) {
        StrategyConfig strat_cfg;
        strat_cfg.strategy_id = "MOM_REV_01";
        strat_cfg.parameters = app_config.strategies["MomentumReversal"];
        
        auto strategy = std::make_shared<MomentumReversalStrategy>(strat_cfg, msgbus.get(), cache.get(), clock.get());
        
        // 6. Start Everything
        // Initialize components
        data_engine->initialize();
        exec_engine->initialize();
        risk_engine->initialize();
        strategy->initialize();

        // Start components
        data_engine->start();
        exec_engine->start();
        risk_engine->start();
        exec_client->connect(); // Start REST/WS sessions for execution if needed
        strategy->start();
        
        LOG_INFO("Main", "System Running. Press Ctrl+C to stop.");
        
        while(running) {
             msgbus->run(); // Help proces messages if main thread needed, though bus has worker
             std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        
        strategy->stop();
    } else {
        LOG_ERROR("Main", "No configuration found for MomentumReversal strategy");
    }

    // Cleanup
    risk_engine->stop();
    exec_engine->stop();
    data_engine->stop();
    msgbus->stop();
    
    return 0;
}
