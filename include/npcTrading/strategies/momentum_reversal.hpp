#pragma once

#include "npcTrading/strategy.hpp"
#include <deque>

namespace npcTrading {

class MomentumReversalStrategy : public Strategy {
public:
    MomentumReversalStrategy(const StrategyConfig& config,
                            MessageBus* msgbus,
                            Cache* cache,
                            Clock* clock);
                            
    void on_start() override;
    void on_order_book(const OrderBook& book) override;
    void on_order_filled(const Order& order, const Fill& fill) override;

private:
    // Parameters loaded from config
    std::string symbol_;
    double reversal_threshold_ = 0.001; // 0.1% move triggers reversal
    int window_size_ = 10;              // Number of ticks/updates to look back
    double quantity_ = 0.001;           // Trade size
    std::string client_id_ = "BinanceExec"; // Default execution client
    
    // State
    std::deque<double> price_history_;
    
    // logic helper
    void check_signal(double current_price);
};

} // namespace npcTrading
