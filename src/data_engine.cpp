#include "npcTrading/data_engine.hpp"
#include "npcTrading/logger.hpp"
#include <iostream>

namespace npcTrading {

DataEngine::DataEngine(MessageBus* msgbus, Cache* cache, Clock* clock,
                       const DataEngineConfig& config)
    : Component("DataEngine", msgbus, cache, clock), config_(config) {
}

void DataEngine::on_initialize() {
    // Register message handlers
    msgbus_->register_handler(
        Endpoints::DATA_ENGINE_EXECUTE,
        [this](const std::shared_ptr<Message>& msg) { handle_execute(msg); }
    );

    msgbus_->register_handler(
        Endpoints::DATA_ENGINE_PROCESS,
        [this](const std::shared_ptr<Message>& msg) { handle_process(msg); }
    );

    // Alias for historical data injection - same handler as process
    msgbus_->register_handler(
        Endpoints::DATA_ENGINE_PROCESS_HISTORICAL,
        [this](const std::shared_ptr<Message>& msg) { handle_process(msg); }
    );

    msgbus_->register_request_handler(
        Endpoints::DATA_ENGINE_REQUEST,
        [this](const std::shared_ptr<Request>& req) { return handle_request(req); }
    );

    msgbus_->register_handler(
        Endpoints::DATA_ENGINE_RESPONSE,
        [this](const std::shared_ptr<Message>& msg) { handle_response(msg); }
    );
}

void DataEngine::on_start() {
    // State is managed by base Component usually, but if overridden here:
    // transmission_to is better if manual control needed. 
    // However, Component::start() usually calls transition_to(RUNNING)
    // If we want to force it or ensure it matches legacy code:
    // transition_to(ComponentState::RUNNING); 
    // Actually, looking at the pattern, Component base likely handles it.
    // But to fix the compile error directly:
    transition_to(ComponentState::RUNNING);
    LOG_INFO("DataEngine", "Connecting " + std::to_string(clients_.size()) + " data clients");
    
    for (auto& pair : clients_) {
        LOG_INFO("DataEngine", "Connecting data client: " + pair.first);
        pair.second->connect();
    }
}

void DataEngine::on_stop() {
    transition_to(ComponentState::STOPPED);
    
    for (auto& pair : clients_) {
        LOG_INFO("DataEngine", "Disconnecting data client: " + pair.first);
        pair.second->disconnect();
    }
}

void DataEngine::register_client(std::shared_ptr<DataClient> client) {
    // Set first registered client as default
    if (default_client_id_.empty()) {
        default_client_id_ = client->client_id();
        LOG_INFO("DataEngine", "Default data client set to: " + default_client_id_);
    }
    clients_[client->client_id()] = client;
    venue_to_client_[client->venue()] = client->client_id();
}

ClientId DataEngine::select_client(const InstrumentId& instrument_id, 
                                    const ClientId& requested_client_id) const {
    // 1. If explicit client_id requested and exists, use it
    if (!requested_client_id.empty() && get_client(requested_client_id) != nullptr) {
        return requested_client_id;
    }
    
    // 2. Try route-by-venue via cached instrument
    if (!instrument_id.empty()) {
        const auto* instr = cache_->instrument(instrument_id);
        if (instr != nullptr) {
            auto* client = get_client_for_venue(instr->venue());
            if (client != nullptr) {
                return client->client_id();
            }
        }
    }
    
    // 3. Fall back to default client
    return default_client_id_;
}

DataClient* DataEngine::get_client(const ClientId& client_id) const {
    auto it = clients_.find(client_id);
    return (it != clients_.end()) ? it->second.get() : nullptr;
}

DataClient* DataEngine::get_client_for_venue(const VenueId& venue) const {
    auto it = venue_to_client_.find(venue);
    if (it != venue_to_client_.end()) {
        return get_client(it->second);
    }
    return nullptr;
}

void DataEngine::handle_execute(const std::shared_ptr<Message>& msg) {
    LOG_DEBUG("DataEngine", "Received execute command: " + msg->type());
    
    // ========================================================================
    // Quote subscriptions
    // ========================================================================
    if (auto sub = std::dynamic_pointer_cast<SubscribeQuotes>(msg)) {
        const auto& instrument_id = sub->instrument_id();
        ClientId chosen = select_client(instrument_id, sub->client_id());
        
        if (chosen.empty()) {
            LOG_WARN("DataEngine", "SubscribeQuotes: No client available for " + instrument_id);
            return;
        }
        
        int& refcount = quote_refcount_[instrument_id];
        refcount++;
        
        if (refcount == 1) {
            // First subscription: pin the client and subscribe
            quote_client_[instrument_id] = chosen;
            if (auto* client = get_client(chosen)) {
                client->subscribe_quotes(instrument_id);
                LOG_DEBUG("DataEngine", "Subscribed quotes for " + instrument_id + " via " + chosen);
            }
        } else {
            // Already subscribed; warn if different client requested
            const auto& pinned = quote_client_[instrument_id];
            if (!sub->client_id().empty() && sub->client_id() != pinned) {
                LOG_WARN("DataEngine", "SubscribeQuotes: " + instrument_id + 
                           " already subscribed via " + pinned + 
                           ", ignoring requested client " + sub->client_id());
            }
        }
        return;
    }
    
    if (auto unsub = std::dynamic_pointer_cast<UnsubscribeQuotes>(msg)) {
        const auto& instrument_id = unsub->instrument_id();
        auto it = quote_refcount_.find(instrument_id);
        if (it == quote_refcount_.end() || it->second <= 0) {
            LOG_WARN("DataEngine", "UnsubscribeQuotes: No active subscription for " + instrument_id);
            return;
        }
        
        it->second--;
        if (it->second == 0) {
            // Last subscriber: unsubscribe and clean up
            auto client_it = quote_client_.find(instrument_id);
            if (client_it != quote_client_.end()) {
                if (auto* client = get_client(client_it->second)) {
                    client->unsubscribe_quotes(instrument_id);
                    LOG_DEBUG("DataEngine", "Unsubscribed quotes for " + instrument_id);
                }
                quote_client_.erase(client_it);
            }
            quote_refcount_.erase(it);
        }
        return;
    }
    
    // ========================================================================
    // Trade subscriptions
    // ========================================================================
    if (auto sub = std::dynamic_pointer_cast<SubscribeTrades>(msg)) {
        const auto& instrument_id = sub->instrument_id();
        ClientId chosen = select_client(instrument_id, sub->client_id());
        
        if (chosen.empty()) {
            LOG_WARN("DataEngine", "SubscribeTrades: No client available for " + instrument_id);
            return;
        }
        
        int& refcount = trade_refcount_[instrument_id];
        refcount++;
        
        if (refcount == 1) {
            trade_client_[instrument_id] = chosen;
            if (auto* client = get_client(chosen)) {
                client->subscribe_trades(instrument_id);
                LOG_DEBUG("DataEngine", "Subscribed trades for " + instrument_id + " via " + chosen);
            }
        } else {
            const auto& pinned = trade_client_[instrument_id];
            if (!sub->client_id().empty() && sub->client_id() != pinned) {
                LOG_WARN("DataEngine", "SubscribeTrades: " + instrument_id + 
                           " already subscribed via " + pinned + 
                           ", ignoring requested client " + sub->client_id());
            }
        }
        return;
    }
    
    if (auto unsub = std::dynamic_pointer_cast<UnsubscribeTrades>(msg)) {
        const auto& instrument_id = unsub->instrument_id();
        auto it = trade_refcount_.find(instrument_id);
        if (it == trade_refcount_.end() || it->second <= 0) {
            LOG_WARN("DataEngine", "UnsubscribeTrades: No active subscription for " + instrument_id);
            return;
        }
        
        it->second--;
        if (it->second == 0) {
            auto client_it = trade_client_.find(instrument_id);
            if (client_it != trade_client_.end()) {
                if (auto* client = get_client(client_it->second)) {
                    client->unsubscribe_trades(instrument_id);
                    LOG_DEBUG("DataEngine", "Unsubscribed trades for " + instrument_id);
                }
                trade_client_.erase(client_it);
            }
            trade_refcount_.erase(it);
        }
        return;
    }
    
    // ========================================================================
    // Order book subscriptions (with max-depth tracking)
    // ========================================================================
    if (auto sub = std::dynamic_pointer_cast<SubscribeOrderBook>(msg)) {
        const auto& instrument_id = sub->instrument_id();
        int requested_depth = sub->depth();
        ClientId chosen = select_client(instrument_id, sub->client_id());
        
        if (chosen.empty()) {
            LOG_WARN("DataEngine", "SubscribeOrderBook: No client available for " + instrument_id);
            return;
        }
        
        int& refcount = book_refcount_[instrument_id];
        refcount++;
        
        // Track depth counts
        book_depth_counts_[instrument_id][requested_depth]++;
        
        // Compute new max depth
        int new_max_depth = 0;
        for (const auto& [depth, count] : book_depth_counts_[instrument_id]) {
            if (count > 0 && depth > new_max_depth) {
                new_max_depth = depth;
            }
        }
        
        int& active_depth = book_active_depth_[instrument_id];
        
        if (refcount == 1) {
            // First subscription
            book_client_[instrument_id] = chosen;
            active_depth = new_max_depth;
            if (auto* client = get_client(chosen)) {
                client->subscribe_order_book(instrument_id, active_depth);
                LOG_DEBUG("DataEngine", "Subscribed order book for " + instrument_id + 
                         " depth=" + std::to_string(active_depth) + " via " + chosen);
            }
        } else {
            // Check if max depth increased (need to resubscribe)
            if (new_max_depth > active_depth) {
                int old_depth = active_depth;
                active_depth = new_max_depth;
                auto client_it = book_client_.find(instrument_id);
                if (client_it != book_client_.end()) {
                    if (auto* client = get_client(client_it->second)) {
                        // Resubscribe with new depth
                        client->unsubscribe_order_book(instrument_id);
                        client->subscribe_order_book(instrument_id, active_depth);
                        LOG_DEBUG("DataEngine", "Resubscribed order book for " + instrument_id + 
                                 " depth " + std::to_string(old_depth) + " -> " + 
                                 std::to_string(active_depth));
                    }
                }
            }
            
            // Warn about client mismatch
            const auto& pinned = book_client_[instrument_id];
            if (!sub->client_id().empty() && sub->client_id() != pinned) {
                LOG_WARN("DataEngine", "SubscribeOrderBook: " + instrument_id + 
                           " already subscribed via " + pinned + 
                           ", ignoring requested client " + sub->client_id());
            }
        }
        return;
    }
    
    if (auto unsub = std::dynamic_pointer_cast<UnsubscribeOrderBook>(msg)) {
        const auto& instrument_id = unsub->instrument_id();
        auto it = book_refcount_.find(instrument_id);
        if (it == book_refcount_.end() || it->second <= 0) {
            LOG_WARN("DataEngine", "UnsubscribeOrderBook: No active subscription for " + instrument_id);
            return;
        }
        
        it->second--;
        
        // Note: We don't know which depth is being unsubscribed (Actor tracks by instrument only)
        // For safety, we just decrement the refcount and unsubscribe when it hits 0
        
        if (it->second == 0) {
            auto client_it = book_client_.find(instrument_id);
            if (client_it != book_client_.end()) {
                if (auto* client = get_client(client_it->second)) {
                    client->unsubscribe_order_book(instrument_id);
                    LOG_DEBUG("DataEngine", "Unsubscribed order book for " + instrument_id);
                }
                book_client_.erase(client_it);
            }
            book_refcount_.erase(it);
            book_active_depth_.erase(instrument_id);
            book_depth_counts_.erase(instrument_id);
        }
        return;
    }
    
    // ========================================================================
    // Bar subscriptions
    // ========================================================================
    if (auto sub = std::dynamic_pointer_cast<SubscribeBars>(msg)) {
        const auto& bar_type = sub->bar_type();
        std::string key = bar_key(bar_type);
        ClientId chosen = select_client(bar_type.instrument_id(), sub->client_id());
        
        if (chosen.empty()) {
            LOG_WARN("DataEngine", "SubscribeBars: No client available for " + key);
            return;
        }
        
        int& refcount = bar_refcount_[key];
        refcount++;
        
        if (refcount == 1) {
            bar_client_[key] = chosen;
            if (auto* client = get_client(chosen)) {
                client->subscribe_bars(bar_type);
                LOG_DEBUG("DataEngine", "Subscribed bars for " + key + " via " + chosen);
            }
        } else {
            const auto& pinned = bar_client_[key];
            if (!sub->client_id().empty() && sub->client_id() != pinned) {
                LOG_WARN("DataEngine", "SubscribeBars: " + key + 
                           " already subscribed via " + pinned + 
                           ", ignoring requested client " + sub->client_id());
            }
        }
        return;
    }
    
    if (auto unsub = std::dynamic_pointer_cast<UnsubscribeBars>(msg)) {
        const auto& bar_type = unsub->bar_type();
        std::string key = bar_key(bar_type);
        auto it = bar_refcount_.find(key);
        if (it == bar_refcount_.end() || it->second <= 0) {
            LOG_WARN("DataEngine", "UnsubscribeBars: No active subscription for " + key);
            return;
        }
        
        it->second--;
        if (it->second == 0) {
            auto client_it = bar_client_.find(key);
            if (client_it != bar_client_.end()) {
                if (auto* client = get_client(client_it->second)) {
                    client->unsubscribe_bars(bar_type);
                    LOG_DEBUG("DataEngine", "Unsubscribed bars for " + key);
                }
                bar_client_.erase(client_it);
            }
            bar_refcount_.erase(it);
        }
        return;
    }
    
    LOG_WARN("DataEngine", "handle_execute: Unknown command type: " + msg->type());
}

void DataEngine::handle_process(const std::shared_ptr<Message>& msg) {
    LOG_DEBUG("DataEngine", "Received data: " + msg->type());
    
    // ========================================================================
    // Quote tick: cache first, then publish
    // ========================================================================
    if (auto quote_msg = std::dynamic_pointer_cast<QuoteTickMessage>(msg)) {
        const auto& tick = quote_msg->tick();
        cache_->add_quote_tick(tick);
        
        std::string topic = "MarketData.Quote." + tick.instrument_id();
        msgbus_->publish(topic, msg);
        return;
    }
    
    // ========================================================================
    // Trade tick: cache first, then publish
    // ========================================================================
    if (auto trade_msg = std::dynamic_pointer_cast<TradeTickMessage>(msg)) {
        const auto& trade = trade_msg->trade();
        cache_->add_trade_tick(trade);
        
        std::string topic = "MarketData.Trade." + trade.instrument_id();
        msgbus_->publish(topic, msg);
        return;
    }
    
    // ========================================================================
    // Order book: cache first, then publish
    // ========================================================================
    if (auto book_msg = std::dynamic_pointer_cast<OrderBookMessage>(msg)) {
        const auto& book = book_msg->book();
        cache_->update_order_book(book);
        
        std::string topic = "MarketData.Book." + book.instrument_id();
        msgbus_->publish(topic, msg);
        return;
    }
    
    // ========================================================================
    // Bar: cache first, then publish
    // ========================================================================
    if (auto bar_msg = std::dynamic_pointer_cast<BarMessage>(msg)) {
        const auto& bar = bar_msg->bar();
        cache_->add_bar(bar);
        
        // Topic uses same key format as bar_key() for consistency
        std::string topic = "MarketData.Bar." + bar.bar_type().instrument_id() + 
                           "|" + bar.bar_type().spec();
        msgbus_->publish(topic, msg);
        return;
    }
    
    log_warning("handle_process: Unknown data type: " + msg->type());
}

std::shared_ptr<Response> DataEngine::handle_request(const std::shared_ptr<Request>& req) {
    log_debug("Received request: " + req->type());
    
    // ========================================================================
    // Request instrument specification
    // ========================================================================
    if (auto instr_req = std::dynamic_pointer_cast<RequestInstrument>(req)) {
        const auto& instrument_id = instr_req->instrument_id();
        const auto& venue = instr_req->venue();
        
        // Route by venue
        auto* client = get_client_for_venue(venue);
        if (client == nullptr) {
            // Fall back to default client
            client = get_client(default_client_id_);
        }
        
        if (client == nullptr) {
            log_warning("RequestInstrument: No client available for venue " + venue);
            return std::make_shared<InstrumentResponse>(
                std::nullopt, "No client available for venue: " + venue);
        }
        
        // Synchronous request to client
        auto instrument = client->request_instrument(instrument_id);
        
        if (instrument.has_value()) {
            // Cache first
            cache_->add_instrument(instrument.value());
            log_debug("Cached instrument: " + instrument_id);
        }
        
        return std::make_shared<InstrumentResponse>(instrument);
    }
    
    // ========================================================================
    // Request historical bars
    // ========================================================================
    if (auto bars_req = std::dynamic_pointer_cast<RequestBarsRange>(req)) {
        const auto& bar_type = bars_req->bar_type();
        const auto& instrument_id = bar_type.instrument_id();
        
        // Try to route by venue via cached instrument
        DataClient* client = nullptr;
        const auto* cached_instr = cache_->instrument(instrument_id);
        if (cached_instr != nullptr) {
            client = get_client_for_venue(cached_instr->venue());
        }
        
        // Fall back to default client
        if (client == nullptr) {
            client = get_client(default_client_id_);
        }
        
        if (client == nullptr) {
            log_warning("RequestBarsRange: No client available for " + instrument_id);
            return std::make_shared<BarsResponse>(
                std::vector<Bar>{}, "No client available for instrument: " + instrument_id);
        }
        
        // Synchronous request to client
        auto bars = client->request_bars(bar_type, bars_req->start(), bars_req->end());
        
        // Cache-first publish: add each bar to cache and publish
        for (const auto& bar : bars) {
            cache_->add_bar(bar);
            
            // Publish each bar to its topic (same as handle_process)
            std::string topic = "MarketData.Bar." + bar.bar_type().instrument_id() + 
                               "|" + bar.bar_type().spec();
            msgbus_->publish(topic, std::make_shared<BarMessage>(bar));
        }
        
        log_debug("Received and published " + std::to_string(bars.size()) + 
                 " bars for " + bar_key(bar_type));
        
        return std::make_shared<BarsResponse>(std::move(bars));
    }
    
    log_warning("handle_request: Unknown request type: " + req->type());
    return nullptr;
}

void DataEngine::handle_response(const std::shared_ptr<Message>& msg) {
    // TODO: Handle data responses
    log_debug("Received response: " + msg->type());
}

// LiveDataEngine implementation
LiveDataEngine::LiveDataEngine(MessageBus* msgbus, Cache* cache, Clock* clock,
                               const LiveDataEngineConfig& config)
    : DataEngine(msgbus, cache, clock, config), live_config_(config) {
}

void LiveDataEngine::on_start() {
    DataEngine::on_start();
    // TODO: Initialize live-specific features
}

void LiveDataEngine::on_stop() {
    DataEngine::on_stop();
    // TODO: Cleanup live-specific features
}

} // namespace npcTrading
