#include "npcTrading/execution_engine.hpp"
#include "npcTrading/cache.hpp"
#include <iostream>

namespace npcTrading {

ExecutionEngine::ExecutionEngine(MessageBus* msgbus, Cache* cache, Clock* clock,
                                 const ExecEngineConfig& config)
    : Component("ExecutionEngine", msgbus, cache, clock), config_(config) {
}

void ExecutionEngine::on_initialize() {
    // Register message handlers
    msgbus_->register_handler(
        Endpoints::EXEC_ENGINE_EXECUTE,
        [this](const std::shared_ptr<Message>& msg) { handle_execute(msg); }
    );

    msgbus_->register_handler(
        Endpoints::EXEC_ENGINE_PROCESS,
        [this](const std::shared_ptr<Message>& msg) { handle_process(msg); }
    );
    
    log_info("ExecutionEngine initialized");
}

void ExecutionEngine::on_start() {
    log_info("ExecutionEngine started with " + std::to_string(clients_.size()) + " clients");
}

void ExecutionEngine::on_stop() {
    log_info("ExecutionEngine stopped");
}

void ExecutionEngine::register_client(std::shared_ptr<ExecutionClient> client) {
    clients_[client->client_id()] = client;
    venue_to_client_[client->venue()] = client->client_id();
}

ExecutionClient* ExecutionEngine::get_client(const ClientId& client_id) const {
    auto it = clients_.find(client_id);
    return (it != clients_.end()) ? it->second.get() : nullptr;
}

ExecutionClient* ExecutionEngine::get_client_for_venue(const VenueId& venue) const {
    auto it = venue_to_client_.find(venue);
    if (it != venue_to_client_.end()) {
        return get_client(it->second);
    }
    return nullptr;
}

void ExecutionEngine::handle_execute(const std::shared_ptr<Message>& msg) {
    log_debug("Received execute command: " + msg->type());
    
    // Handle SubmitOrder
    if (auto submit = std::dynamic_pointer_cast<SubmitOrder>(msg)) {
        Order* order = submit->order();
        if (!order) {
            log_error("SubmitOrder has null order");
            return;
        }
        
        // Add order to cache
        cache_->add_order(*order);
        
        // Route to execution client by client_id
        ExecutionClient* client = get_client(order->client_id());
        if (!client) {
            // Try to find by venue
            // For now, assume client_id matches
            log_error("No execution client found for client_id: " + order->client_id());
            
            // Emit OrderRejected
            auto rejected = std::make_shared<OrderRejected>(
                submit->order_ptr(), 
                "No execution client registered for: " + order->client_id(),
                clock_->now());
            msgbus_->publish("OrderEvent", rejected);
            return;
        }
        
        if (!client->is_connected()) {
            log_error("Execution client not connected: " + order->client_id());
            
            auto rejected = std::make_shared<OrderRejected>(
                submit->order_ptr(),
                "Execution client not connected",
                clock_->now());
            msgbus_->publish("OrderEvent", rejected);
            return;
        }
        
        // Submit to client
        client->submit_order(order);
        return;
    }
    
    // Handle CancelOrder
    if (auto cancel = std::dynamic_pointer_cast<CancelOrder>(msg)) {
        Order* order = cancel->order();
        if (!order) {
            log_error("CancelOrder has null order");
            return;
        }
        
        ExecutionClient* client = get_client(order->client_id());
        if (client && client->is_connected()) {
            client->cancel_order(order);
        }
        return;
    }
    
    // Handle ModifyOrder
    if (auto modify = std::dynamic_pointer_cast<ModifyOrder>(msg)) {
        Order* order = modify->order();
        if (!order) {
            log_error("ModifyOrder has null order");
            return;
        }
        
        ExecutionClient* client = get_client(order->client_id());
        if (client && client->is_connected()) {
            client->modify_order(order, modify->new_quantity(), modify->new_price());
        }
        return;
    }
    
    log_warning("Unhandled execute command type: " + msg->type());
}

void ExecutionEngine::handle_process(const std::shared_ptr<Message>& msg) {
    log_debug("Received order event: " + msg->type());
    
    // Handle OrderSubmitted
    if (auto event = std::dynamic_pointer_cast<OrderSubmitted>(msg)) {
        Order* order = event->order();
        if (order) {
            order->set_status(OrderStatus::SUBMITTED);
            cache_->update_order(*order);
        }
        msgbus_->publish("OrderEvent", msg);
        return;
    }
    
    // Handle OrderAccepted
    if (auto event = std::dynamic_pointer_cast<OrderAccepted>(msg)) {
        Order* order = event->order();
        if (order) {
            order->set_status(OrderStatus::ACCEPTED);
            cache_->update_order(*order);
        }
        msgbus_->publish("OrderEvent", msg);
        return;
    }
    
    // Handle OrderRejected
    if (auto event = std::dynamic_pointer_cast<OrderRejected>(msg)) {
        Order* order = event->order();
        if (order) {
            order->set_status(OrderStatus::REJECTED);
            cache_->update_order(*order);
        }
        msgbus_->publish("OrderEvent", msg);
        return;
    }
    
    // Handle OrderFilled
    if (auto event = std::dynamic_pointer_cast<OrderFilled>(msg)) {
        Order* order = event->order();
        if (order) {
            // Update filled quantity
            double current_filled = order->filled_qty().as_double();
            double fill_qty = event->fill().quantity().as_double();
            order->set_filled_qty(Quantity(current_filled + fill_qty));
            
            // Check if fully filled
            if (order->filled_qty().as_double() >= order->quantity().as_double()) {
                order->set_status(OrderStatus::FILLED);
            } else {
                order->set_status(OrderStatus::PARTIALLY_FILLED);
            }
            
            cache_->update_order(*order);
            
            // Update position
            handle_fill(*event);
        }
        msgbus_->publish("OrderEvent", msg);
        return;
    }
    
    // Handle OrderCanceled
    if (auto event = std::dynamic_pointer_cast<OrderCanceled>(msg)) {
        Order* order = event->order();
        if (order) {
            order->set_status(OrderStatus::CANCELED);
            cache_->update_order(*order);
        }
        msgbus_->publish("OrderEvent", msg);
        return;
    }
    
    // Handle OrderDenied (from RiskEngine)
    if (auto event = std::dynamic_pointer_cast<OrderDenied>(msg)) {
        Order* order = event->order();
        if (order) {
            order->set_status(OrderStatus::REJECTED);
            cache_->update_order(*order);
        }
        msgbus_->publish("OrderEvent", msg);
        return;
    }
    
    log_warning("Unhandled order event type: " + msg->type());
}

void ExecutionEngine::handle_fill(const OrderFilled& event) {
    Order* order = event.order();
    if (!order) return;

    PositionId pos_id = generate_position_id(order);
    const Fill& fill = event.fill();

    if (cache_->position_exists(pos_id)) {
        // Update existing position
        const Position* existing_pos = cache_->position(pos_id);
        Position pos = *existing_pos; // Copy
        pos.apply_fill(fill);
        cache_->update_position(pos);
        
        // Link order to position
        order->set_position_id(pos_id);
        
        log_info("Position updated: " + pos_id + 
                 " Qty=" + std::to_string(pos.quantity().as_double()) + 
                 " AvgPrice=" + std::to_string(pos.entry_price().as_double()));
    } else {
        // Create new position
        Position pos(pos_id, order->instrument_id(), order->strategy_id());
        pos.apply_fill(fill);
        cache_->add_position(pos);
        
        // Link order to position
        order->set_position_id(pos_id);
        
        log_info("Position created: " + pos_id + 
                 " Qty=" + std::to_string(pos.quantity().as_double()) + 
                 " AvgPrice=" + std::to_string(pos.entry_price().as_double()));
    }
}

void ExecutionEngine::update_position(Order* order, const Fill& fill) {
    // Deprecated/Unused helper, logic moved to handle_fill
}

PositionId ExecutionEngine::generate_position_id(Order* order) {
    return order->instrument_id() + "-" + order->strategy_id();
}

// LiveExecutionEngine implementation
LiveExecutionEngine::LiveExecutionEngine(MessageBus* msgbus, Cache* cache, Clock* clock,
                                         const LiveExecEngineConfig& config)
    : ExecutionEngine(msgbus, cache, clock, config), live_config_(config) {
}

void LiveExecutionEngine::on_start() {
    ExecutionEngine::on_start();
    // TODO: Live-specific start logic
}

void LiveExecutionEngine::on_stop() {
    ExecutionEngine::on_stop();
    // TODO: Live-specific stop logic
}

} // namespace npcTrading
