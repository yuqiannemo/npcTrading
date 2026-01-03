#include "npcTrading/strategies/momentum_reversal.hpp"
#include "npcTrading/logger.hpp"
#include <iostream>
#include <algorithm>

namespace npcTrading {

MomentumReversalStrategy::MomentumReversalStrategy(const StrategyConfig& config,
                                                   MessageBus* msgbus,
                                                   Cache* cache,
                                                   Clock* clock)
    : Strategy(config, msgbus, cache, clock) {
    
    // Load parameters
    if (config.parameters.count("symbol")) 
        symbol_ = config.parameters.at("symbol");
    
    if (config.parameters.count("threshold"))
        reversal_threshold_ = std::stod(config.parameters.at("threshold"));
        
    if (config.parameters.count("window_size"))
        window_size_ = std::stoi(config.parameters.at("window_size"));
        
    if (config.parameters.count("quantity"))
        quantity_ = std::stod(config.parameters.at("quantity"));

    if (config.parameters.count("client_id"))
        client_id_ = config.parameters.at("client_id");
        
    LOG_INFO(strategy_id(), "Initialized MomentumReversal: Symbol=" + symbol_ + 
             ", Threshold=" + std::to_string(reversal_threshold_) +
             ", Window=" + std::to_string(window_size_));
}

void MomentumReversalStrategy::on_start() {
    if (symbol_.empty()) {
        LOG_ERROR(strategy_id(), "No symbol configured!");
        return;
    }
    
    LOG_INFO(strategy_id(), "Starting strategy, subscribing to book for " + symbol_);
    subscribe_order_book(symbol_, 5); // Depth 5 is enough for mid price
}

void MomentumReversalStrategy::on_order_book(const OrderBook& book) {
    double best_bid = book.best_bid_price().as_double();
    double best_ask = book.best_ask_price().as_double();
    
    if (best_bid <= 0 || best_ask <= 0) return;
    
    double mid_price = (best_bid + best_ask) / 2.0;
    
    // Update history
    price_history_.push_back(mid_price);
    if (price_history_.size() > static_cast<size_t>(window_size_)) {
        price_history_.pop_front();
    }

    // Debug log removed
    
    if (price_history_.size() < static_cast<size_t>(window_size_)) return; // Wait for full window
    
    check_signal(mid_price);
}

void MomentumReversalStrategy::check_signal(double current_price) {
    double old_price = price_history_.front();
    double percent_change = (current_price - old_price) / old_price;
    
    // DEBUG: Print change if it's significant (e.g. > 10% of threshold) to see if we are close
    if (std::abs(percent_change) > reversal_threshold_ * 0.1) {
       // LOG_INFO(strategy_id(), "Monitoring: Change=" + std::to_string(percent_change*100) + 
       //          "%, Thr=" + std::to_string(reversal_threshold_*100) + "%");
    }
    
    const Position* pos = position_for(symbol_);
    double current_qty = 0.0;
    if (pos) {
        current_qty = pos->quantity().as_double();
        if (pos->is_short()) current_qty = -current_qty;
    }
    
    // Logic: 
    // If price rose > threshold (Momentum UP), we expect Reversal -> SELL
    // If price fell < -threshold (Momentum DOWN), we expect Reversal -> BUY
    
    if (percent_change > reversal_threshold_) {
        // Signal: SELL
        if (current_qty <= 0) { // Only sell if we are flat or long (to close), don't go net short for simplicity? Or allow flip?
            // Let's implement simple Reverter: Always want to be Short if high, Long if low.
            // Simplified: If not Short, Sell.
             if (current_qty > -quantity_) { // Max short position check
                 LOG_INFO(strategy_id(), "Signal SELL: Change " + std::to_string(percent_change*100) + "% > " + std::to_string(reversal_threshold_*100) + "%");
                 
                 auto order_id = generate_order_id();
                 auto order = std::make_shared<Order>(
                    order_id, strategy_id(), symbol_, client_id_, 
                    OrderSide::SELL, OrderType::MARKET, Quantity(quantity_)
                 );
                 submit_order(order);

                 price_history_.clear(); // Reset window after trade to avoid spam
             }
        }
    }
    else if (percent_change < -reversal_threshold_) {
        // Signal: BUY
        if (current_qty < quantity_) { // Max long position check
             LOG_INFO(strategy_id(), "Signal BUY: Change " + std::to_string(percent_change*100) + "% < -" + std::to_string(reversal_threshold_*100) + "%");
             
             auto order_id = generate_order_id();
             auto order = std::make_shared<Order>(
                order_id, strategy_id(), symbol_, client_id_, 
                OrderSide::BUY, OrderType::MARKET, Quantity(quantity_)
             );
             submit_order(order);

             price_history_.clear(); // Reset window
        }
    }
}

void MomentumReversalStrategy::on_order_filled(const Order& order, const Fill& fill) {
    LOG_INFO(strategy_id(), "Order FIllED: " + order.instrument_id() + " " + 
             (order.side() == OrderSide::BUY ? "BUY" : "SELL") + " " + 
             std::to_string(fill.quantity().as_double()) + " @ " + 
             std::to_string(fill.price().as_double()));
}

} // namespace npcTrading
