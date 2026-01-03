#pragma once

#include <string>
#include <vector>
#include <map>

namespace npcTrading {

struct SystemConfig {
    std::string log_level = "INFO";
};

struct BinanceConfig {
    std::string api_key;
    std::string api_secret;
    std::string rest_base_url; // Optional override
    std::string ws_base_url;   // Optional override
    bool use_testnet = false;
};

struct ExchangeConfig {
    BinanceConfig binance;
};

struct RiskConfig {
    double max_order_notional = 100000.0;
    double max_position_size = 10.0;
    bool allow_short = true;
};

struct Config {
    SystemConfig system;
    ExchangeConfig exchange;
    RiskConfig risk;
    
    // Allow strategies to load their own arbitrary config
    // We'll store it as a map strings for simplicity until we need more
    std::map<std::string, std::map<std::string, std::string>> strategies;
    
    // Load from JSON file
    static Config load(const std::string& path);
};

} // namespace npcTrading
