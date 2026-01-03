#include "npcTrading/strategy.hpp"
#include "npcTrading/data_engine.hpp"
#include "npcTrading/logger.hpp"
#include <stdexcept>
#include <utility>

namespace npcTrading {

// Actor implementation
Actor::Actor(const std::string& actor_id, MessageBus* msgbus, Cache* cache, Clock* clock)
    : Component(actor_id, msgbus, cache, clock) {
}

void Actor::subscribe_bars(const BarType& bar_type) {
    BarKey key{bar_type.instrument_id(), bar_type.spec()};
    if (key.instrument_id.empty() || key.spec.empty()) {
        LOG_WARN("Actor", "subscribe_bars called with empty bar fields");
        return;
    }
    if (!bar_subscriptions_.insert(key).second) {
        return; // already subscribed
    }
    
    // Subscribe to MessageBus topic for this bar type
    std::string topic = bar_topic(key.instrument_id, key.spec);
    SubscriptionToken token = msgbus_->subscribe(topic, [this](const std::shared_ptr<Message>& msg) {
        handle_message(msg);
    });
    bar_tokens_[key] = token;
    
    // Tell DataEngine to subscribe with the data client
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE,
                  std::make_shared<SubscribeBars>(bar_type));
}

void Actor::unsubscribe_bars(const BarType& bar_type) {
    BarKey key{bar_type.instrument_id(), bar_type.spec()};
    if (key.instrument_id.empty() || key.spec.empty()) {
        return;
    }
    bar_subscriptions_.erase(key);
    
    // Unsubscribe from MessageBus topic
    auto it = bar_tokens_.find(key);
    if (it != bar_tokens_.end()) {
        msgbus_->unsubscribe(it->second);
        bar_tokens_.erase(it);
    }
    
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE,
                  std::make_shared<UnsubscribeBars>(bar_type));
}

void Actor::subscribe_quotes(const InstrumentId& instrument_id) {
    if (instrument_id.empty()) {
        LOG_WARN("Actor", "subscribe_quotes called with empty instrument_id");
        return;
    }
    if (!quote_subscriptions_.insert(instrument_id).second) {
        return;
    }
    
    // Subscribe to MessageBus topic for quotes
    std::string topic = quote_topic(instrument_id);
    SubscriptionToken token = msgbus_->subscribe(topic, [this](const std::shared_ptr<Message>& msg) {
        handle_message(msg);
    });
    quote_tokens_[instrument_id] = token;
    
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE,
                  std::make_shared<SubscribeQuotes>(instrument_id));
}

void Actor::unsubscribe_quotes(const InstrumentId& instrument_id) {
    if (instrument_id.empty()) {
        return;
    }
    quote_subscriptions_.erase(instrument_id);
    
    // Unsubscribe from MessageBus topic
    auto it = quote_tokens_.find(instrument_id);
    if (it != quote_tokens_.end()) {
        msgbus_->unsubscribe(it->second);
        quote_tokens_.erase(it);
    }
    
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE,
                  std::make_shared<UnsubscribeQuotes>(instrument_id));
}

void Actor::subscribe_trades(const InstrumentId& instrument_id) {
    if (instrument_id.empty()) {
        LOG_WARN("Actor", "subscribe_trades called with empty instrument_id");
        return;
    }
    if (!trade_subscriptions_.insert(instrument_id).second) {
        return;
    }
    
    // Subscribe to MessageBus topic for trades
    std::string topic = trade_topic(instrument_id);
    SubscriptionToken token = msgbus_->subscribe(topic, [this](const std::shared_ptr<Message>& msg) {
        handle_message(msg);
    });
    trade_tokens_[instrument_id] = token;
    
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE,
                  std::make_shared<SubscribeTrades>(instrument_id));
}

void Actor::unsubscribe_trades(const InstrumentId& instrument_id) {
    if (instrument_id.empty()) {
        return;
    }
    trade_subscriptions_.erase(instrument_id);
    
    // Unsubscribe from MessageBus topic
    auto it = trade_tokens_.find(instrument_id);
    if (it != trade_tokens_.end()) {
        msgbus_->unsubscribe(it->second);
        trade_tokens_.erase(it);
    }
    
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE,
                  std::make_shared<UnsubscribeTrades>(instrument_id));
}

void Actor::subscribe_order_book(const InstrumentId& instrument_id, int depth) {
    if (instrument_id.empty()) {
        log_warning("subscribe_order_book called with empty instrument_id");
        return;
    }
    if (depth <= 0) {
        LOG_WARN("Actor", "subscribe_order_book called with non-positive depth");
        return;
    }
    if (!orderbook_subscriptions_.insert(instrument_id).second) {
        return;
    }
    
    // Subscribe to MessageBus topic for order book
    std::string topic = book_topic(instrument_id);
    SubscriptionToken token = msgbus_->subscribe(topic, [this](const std::shared_ptr<Message>& msg) {
        handle_message(msg);
    });
    book_tokens_[instrument_id] = token;
    
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE,
                  std::make_shared<SubscribeOrderBook>(instrument_id, depth));
}

void Actor::unsubscribe_order_book(const InstrumentId& instrument_id) {
    if (instrument_id.empty()) {
        return;
    }
    orderbook_subscriptions_.erase(instrument_id);
    
    // Unsubscribe from MessageBus topic
    auto it = book_tokens_.find(instrument_id);
    if (it != book_tokens_.end()) {
        msgbus_->unsubscribe(it->second);
        book_tokens_.erase(it);
    }
    
    msgbus_->send(Endpoints::DATA_ENGINE_EXECUTE,
                  std::make_shared<UnsubscribeOrderBook>(instrument_id));
}

void Actor::register_event_handler(const std::string& endpoint) {
    if (endpoint.empty()) {
        throw std::invalid_argument("Endpoint cannot be empty");
    }
    msgbus_->register_handler(endpoint, [this](const std::shared_ptr<Message>& msg) {
        handle_message(msg);
    });
}

void Actor::handle_message(const std::shared_ptr<Message>& msg) {
    if (!msg) {
        LOG_WARN("Actor", "Received null message in Actor");
        return;
    }

    if (auto md = std::dynamic_pointer_cast<MarketDataMessage>(msg)) {
        md->dispatch_to(*this);
        return;
    }

    if (auto bar_msg = std::dynamic_pointer_cast<BarMessage>(msg)) {
        on_bar(bar_msg->bar());
        return;
    }

    if (auto quote_msg = std::dynamic_pointer_cast<QuoteTickMessage>(msg)) {
        on_quote(quote_msg->tick());
        return;
    }

    if (auto trade_msg = std::dynamic_pointer_cast<TradeTickMessage>(msg)) {
        on_trade(trade_msg->trade());
        return;
    }

    if (auto book_msg = std::dynamic_pointer_cast<OrderBookMessage>(msg)) {
        on_order_book(book_msg->book());
        return;
    }

    on_event(msg);
}

// Strategy implementation
Strategy::Strategy(const StrategyConfig& config, MessageBus* msgbus, Cache* cache, Clock* clock)
    : Actor(config.strategy_id, msgbus, cache, clock), config_(config) {
}

void Strategy::submit_order(const std::shared_ptr<Order>& order) {
    if (!order) {
        LOG_WARN("Strategy", "submit_order called with null order");
        return;
    }
    orders_[order->order_id()] = order;
    auto command = std::make_shared<SubmitOrder>(order);
    msgbus_->send(Endpoints::RISK_ENGINE_EXECUTE, command);
}

void Strategy::submit_order_list(const std::vector<std::shared_ptr<Order>>& orders) {
    for (const auto& order : orders) {
        submit_order(order);
    }
}

void Strategy::submit_market_order(const InstrumentId& instrument_id,
                                  OrderSide side,
                                  Quantity quantity) {
    if (instrument_id.empty()) {
        LOG_WARN("Strategy", "submit_market_order called with empty instrument_id");
        return;
    }
    if (quantity.as_double() <= 0) {
        LOG_WARN("Strategy", "submit_market_order called with non-positive quantity");
        return;
    }

    auto order_id = generate_order_id();
    auto order = std::make_shared<Order>(
        order_id,
        config_.strategy_id,
        instrument_id,
        "", // client id
        side,
        OrderType::MARKET,
        quantity,
        Price(0),
        TimeInForce::GTC,
        clock_->now()
    );

    orders_[order_id] = order;

    auto command = std::make_shared<SubmitOrder>(order);
    msgbus_->send(Endpoints::RISK_ENGINE_EXECUTE, command);
}

void Strategy::submit_limit_order(const InstrumentId& instrument_id,
                                 OrderSide side,
                                 Quantity quantity,
                                 Price price) {
    if (instrument_id.empty()) {
        LOG_WARN("Strategy", "submit_limit_order called with empty instrument_id");
        return;
    }
    if (quantity.as_double() <= 0) {
        LOG_WARN("Strategy", "submit_limit_order called with non-positive quantity");
        return;
    }
    if (price.as_double() <= 0) {
        LOG_WARN("Strategy", "submit_limit_order called with non-positive price");
        return;
    }

    auto order_id = generate_order_id();
    auto order = std::make_shared<Order>(
        order_id,
        config_.strategy_id,
        instrument_id,
        "", // client id
        side,
        OrderType::LIMIT,
        quantity,
        price,
        TimeInForce::GTC,
        clock_->now()
    );

    orders_[order_id] = order;

    auto command = std::make_shared<SubmitOrder>(order);
    msgbus_->send(Endpoints::RISK_ENGINE_EXECUTE, command);
}

void Strategy::modify_order(const std::shared_ptr<Order>& order, Quantity new_quantity, Price new_price) {
    if (!order) {
        LOG_WARN("Strategy", "modify_order called with null order");
        return;
    }
    auto command = std::make_shared<ModifyOrder>(order, new_quantity, new_price);
    msgbus_->send(Endpoints::RISK_ENGINE_EXECUTE, command);
}

void Strategy::cancel_order(const std::shared_ptr<Order>& order) {
    if (!order) {
        LOG_WARN("Strategy", "cancel_order called with null order");
        return;
    }
    auto command = std::make_shared<CancelOrder>(order);
    msgbus_->send(Endpoints::RISK_ENGINE_EXECUTE, command);
}

void Strategy::cancel_all_orders(const InstrumentId& instrument_id) {
    auto command = std::make_shared<CancelAllOrders>(instrument_id);
    msgbus_->send(Endpoints::RISK_ENGINE_EXECUTE, command);
}

std::vector<const Position*> Strategy::positions_open() const {
    return cache_->positions_open(config_.strategy_id);
}

const Position* Strategy::position_for(const InstrumentId& instrument_id) const {
    return cache_->position_for_instrument(instrument_id, config_.strategy_id);
}

bool Strategy::has_position() const {
    return !positions_open().empty();
}

bool Strategy::has_position(const InstrumentId& instrument_id) const {
    return position_for(instrument_id) != nullptr;
}

void Strategy::on_event(const std::shared_ptr<Message>& event) {
    // Dispatch to specific callbacks based on event type
    if (!event) {
        LOG_WARN("Strategy", "Strategy received null event");
        return;
    }

    if (auto submitted = std::dynamic_pointer_cast<OrderSubmitted>(event)) {
        if (submitted->order()) {
            on_order_submitted(*submitted->order());
        }
        return;
    }

    if (auto accepted = std::dynamic_pointer_cast<OrderAccepted>(event)) {
        if (accepted->order()) {
            on_order_accepted(*accepted->order());
        }
        return;
    }

    if (auto rejected = std::dynamic_pointer_cast<OrderRejected>(event)) {
        if (rejected->order()) {
            on_order_rejected(*rejected->order(), rejected->reason());
        }
        return;
    }

    if (auto denied = std::dynamic_pointer_cast<OrderDenied>(event)) {
        if (denied->order()) {
            on_order_denied(*denied->order(), denied->reason());
        }
        return;
    }

    if (auto filled = std::dynamic_pointer_cast<OrderFilled>(event)) {
        if (filled->order()) {
            on_order_filled(*filled->order(), filled->fill());
        }
        return;
    }

    if (auto canceled = std::dynamic_pointer_cast<OrderCanceled>(event)) {
        if (canceled->order()) {
            on_order_canceled(*canceled->order());
        }
        return;
    }

    Actor::on_event(event);
}

OrderId Strategy::generate_order_id() {
    return config_.strategy_id + "-O" + std::to_string(++order_counter_);
}

// ExecAlgorithm implementation
ExecAlgorithm::ExecAlgorithm(const std::string& algo_id, MessageBus* msgbus,
                             Cache* cache, Clock* clock)
    : Actor(algo_id, msgbus, cache, clock) {
}

void ExecAlgorithm::spawn_market_order(const InstrumentId& instrument_id,
                                      OrderSide side,
                                      Quantity quantity,
                                      TimeInForce tif) {
    // TODO: Create and submit child order
}

void ExecAlgorithm::spawn_limit_order(const InstrumentId& instrument_id,
                                     OrderSide side,
                                     Quantity quantity,
                                     Price price,
                                     TimeInForce tif) {
    // TODO: Create and submit child order
}

void ExecAlgorithm::subscribe_to_order(const OrderId& order_id) {
    // TODO: Subscribe to order events
}

} // namespace npcTrading
