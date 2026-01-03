#include "npcTrading/config.hpp"
#include "npcTrading/logger.hpp"
#include <boost/json.hpp>
#include <fstream>
#include <iostream>

namespace npcTrading {

namespace json = boost::json;

// Helper to safely get string from object
std::string get_string(const json::object& obj, const std::string& key, const std::string& default_val = "") {
    if (obj.contains(key)) {
        const auto& val = obj.at(key);
        if (val.is_string()) return json::value_to<std::string>(val);
    }
    return default_val;
}

// Helper to safely get double
double get_double(const json::object& obj, const std::string& key, double default_val = 0.0) {
    if (obj.contains(key)) {
        const auto& val = obj.at(key);
        if (val.is_double()) return val.as_double();
        if (val.is_int64()) return static_cast<double>(val.as_int64());
    }
    return default_val;
}

// Helper to safely get bool
bool get_bool(const json::object& obj, const std::string& key, bool default_val = false) {
    if (obj.contains(key)) {
        const auto& val = obj.at(key);
        if (val.is_bool()) return val.as_bool();
    }
    return default_val;
}

Config Config::load(const std::string& path) {
    Config config;
    try {
        std::ifstream file(path);
        if (!file.is_open()) {
            LOG_ERROR("Config", "Could not open config file: " + path);
            return config;
        }

        // Read entire file into string
        std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
        
        // Parse JSON
        json::value jv = json::parse(content);
        json::object root = jv.as_object();

        // 1. System
        if (root.contains("system")) {
            json::object sys = root.at("system").as_object();
            config.system.log_level = get_string(sys, "log_level", "INFO");
        }

        // 2. Exchange
        if (root.contains("exchange")) {
            json::object ex = root.at("exchange").as_object();
            if (ex.contains("binance")) {
                json::object bn = ex.at("binance").as_object();
                config.exchange.binance.api_key = get_string(bn, "api_key");
                config.exchange.binance.api_secret = get_string(bn, "api_secret");
                config.exchange.binance.use_testnet = get_bool(bn, "testnet", false);
                config.exchange.binance.rest_base_url = get_string(bn, "rest_base_url");
                config.exchange.binance.ws_base_url = get_string(bn, "ws_base_url");
            }
        }

        // 3. Risk
        if (root.contains("risk")) {
            json::object risk = root.at("risk").as_object();
            config.risk.max_order_notional = get_double(risk, "max_order_notional", 100000.0);
            config.risk.max_position_size = get_double(risk, "max_position_size", 10.0);
            config.risk.allow_short = get_bool(risk, "allow_short", true);
        }

        // 4. Strategies
        if (root.contains("strategies")) {
            json::object strats = root.at("strategies").as_object();
            for (auto it = strats.begin(); it != strats.end(); ++it) {
                std::string strat_name = std::string(it->key());
                if (it->value().is_object()) {
                    json::object params = it->value().as_object();
                    std::map<std::string, std::string> param_map;
                    for (auto pit = params.begin(); pit != params.end(); ++pit) {
                        std::string p_key = std::string(pit->key());
                        // Convert all values to string for simplicity
                        if (pit->value().is_string()) {
                            param_map[p_key] = json::value_to<std::string>(pit->value());
                        } else if (pit->value().is_int64()) {
                            param_map[p_key] = std::to_string(pit->value().as_int64());
                        } else if (pit->value().is_double()) {
                            param_map[p_key] = std::to_string(pit->value().as_double());
                        } else if (pit->value().is_bool()) {
                            param_map[p_key] = pit->value().as_bool() ? "true" : "false";
                        }
                    }
                    config.strategies[strat_name] = param_map;
                }
            }
        }
        
        LOG_INFO("Config", "Configuration loaded successfully from " + path);

    } catch (const std::exception& e) {
        LOG_ERROR("Config", std::string("Error parsing config: ") + e.what());
    }
    return config;
}

} // namespace npcTrading
