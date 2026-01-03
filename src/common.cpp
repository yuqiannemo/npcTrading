#include "npcTrading/common.hpp"

namespace npcTrading {

// String conversion implementations
std::string to_string(ComponentState state) {
    switch (state) {
        case ComponentState::PRE_INITIALIZED: return "PRE_INITIALIZED";
        case ComponentState::INITIALIZED: return "INITIALIZED";
        case ComponentState::READY: return "READY";
        case ComponentState::RUNNING: return "RUNNING";
        case ComponentState::STOPPED: return "STOPPED";
        case ComponentState::DISPOSED: return "DISPOSED";
        default: return "UNKNOWN";
    }
}

std::string to_string(OrderSide side) {
    switch (side) {
        case OrderSide::BUY: return "BUY";
        case OrderSide::SELL: return "SELL";
        default: return "UNKNOWN";
    }
}

std::string to_string(OrderType type) {
    switch (type) {
        case OrderType::MARKET: return "MARKET";
        case OrderType::LIMIT: return "LIMIT";
        case OrderType::STOP_MARKET: return "STOP_MARKET";
        case OrderType::STOP_LIMIT: return "STOP_LIMIT";
        default: return "UNKNOWN";
    }
}

std::string to_string(TimeInForce tif) {
    switch (tif) {
        case TimeInForce::GTC: return "GTC";
        case TimeInForce::IOC: return "IOC";
        case TimeInForce::FOK: return "FOK";
        case TimeInForce::GTD: return "GTD";
        default: return "UNKNOWN";
    }
}

std::string to_string(OrderStatus status) {
    switch (status) {
        case OrderStatus::INITIALIZED: return "INITIALIZED";
        case OrderStatus::SUBMITTED: return "SUBMITTED";
        case OrderStatus::ACCEPTED: return "ACCEPTED";
        case OrderStatus::REJECTED: return "REJECTED";
        case OrderStatus::CANCELED: return "CANCELED";
        case OrderStatus::EXPIRED: return "EXPIRED";
        case OrderStatus::TRIGGERED: return "TRIGGERED";
        case OrderStatus::PARTIALLY_FILLED: return "PARTIALLY_FILLED";
        case OrderStatus::FILLED: return "FILLED";
        default: return "UNKNOWN";
    }
}

std::string to_string(TradingState state) {
    switch (state) {
        case TradingState::ACTIVE: return "ACTIVE";
        case TradingState::HALTED: return "HALTED";
        case TradingState::REDUCING: return "REDUCING";
        default: return "UNKNOWN";
    }
}

} // namespace npcTrading

// Implement Boost.JSON header-only here to ensure it's linked for all clients
#ifdef NPCTRADING_BINANCE_ENABLED
#include <boost/json/src.hpp>
#endif
