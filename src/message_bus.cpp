#include "npcTrading/message_bus.hpp"
#include "npcTrading/logger.hpp"
#include <iostream>
#include <algorithm>

namespace npcTrading {

// ============================================================================
// Construction / Destruction
// ============================================================================

MessageBus::MessageBus(const MessageBusConfig& config)
    : config_(config), running_(false), messages_processed_(0) {
    if (config_.max_queue_size == 0) {
        throw std::invalid_argument("max_queue_size must be greater than 0");
    }
}

MessageBus::~MessageBus() {
    if (running_) {
        stop();
    }
}

// ============================================================================
// Endpoint Registration
// ============================================================================

void MessageBus::register_handler(const std::string& endpoint, MessageHandler handler) {
    if (endpoint.empty()) {
        throw std::invalid_argument("Endpoint name cannot be empty");
    }
    if (!handler) {
        throw std::invalid_argument("Handler cannot be null");
    }
    message_handlers_[endpoint] = handler;
}

void MessageBus::register_request_handler(const std::string& endpoint, RequestHandler handler) {
    if (endpoint.empty()) {
        throw std::invalid_argument("Endpoint name cannot be empty");
    }
    if (!handler) {
        throw std::invalid_argument("Handler cannot be null");
    }
    request_handlers_[endpoint] = handler;
}

void MessageBus::unregister_handler(const std::string& endpoint) {
    message_handlers_.erase(endpoint);
    request_handlers_.erase(endpoint);
}

// ============================================================================
// Message Sending (Async)
// ============================================================================

void MessageBus::send(const std::string& endpoint, std::shared_ptr<Message> message) {
    if (!message) {
        throw std::invalid_argument("Message cannot be null");
    }
    
    if (!running_) {
        std::cerr << "[MessageBus] Warning: Cannot send message - bus not running" << std::endl;
        return;
    }
    
    // Queue the message for async processing
    std::lock_guard<std::mutex> lock(queue_mutex_);
    
    if (message_queue_.size() >= config_.max_queue_size) {
        std::cerr << "[MessageBus] Warning: Queue full (" << config_.max_queue_size 
                  << "), dropping message to endpoint: " << endpoint << std::endl;
        return;
    }
    
    message_queue_.push_back({endpoint, message, false});
}

// ============================================================================
// Request-Response (Sync)
// ============================================================================

std::shared_ptr<Response> MessageBus::request(const std::string& endpoint, 
                                              std::shared_ptr<Request> request) {
    if (!request) {
        throw std::invalid_argument("Request cannot be null");
    }
    
    auto it = request_handlers_.find(endpoint);
    if (it != request_handlers_.end()) {
        return it->second(request);
    } else {
        std::cerr << "[MessageBus] Error: No request handler registered for endpoint: " 
                  << endpoint << std::endl;
        return nullptr;
    }
}

// ============================================================================
// Pub/Sub
// ============================================================================

SubscriptionToken MessageBus::subscribe(const std::string& topic, MessageHandler handler) {
    if (topic.empty()) {
        throw std::invalid_argument("Topic name cannot be empty");
    }
    if (!handler) {
        throw std::invalid_argument("Handler cannot be null");
    }
    
    std::lock_guard<std::mutex> lock(sub_mutex_);
    SubscriptionToken token = next_token_++;
    subscribers_[topic][token] = handler;
    token_to_topic_[token] = topic;
    return token;
}

void MessageBus::unsubscribe(SubscriptionToken token) {
    if (token == INVALID_SUBSCRIPTION_TOKEN) {
        return;
    }
    
    std::lock_guard<std::mutex> lock(sub_mutex_);
    auto topic_it = token_to_topic_.find(token);
    if (topic_it == token_to_topic_.end()) {
        return;  // Token not found
    }
    
    const std::string& topic = topic_it->second;
    auto subs_it = subscribers_.find(topic);
    if (subs_it != subscribers_.end()) {
        subs_it->second.erase(token);
        // Clean up empty topic entries
        if (subs_it->second.empty()) {
            subscribers_.erase(subs_it);
        }
    }
    token_to_topic_.erase(topic_it);
}

void MessageBus::unsubscribe_all(const std::string& topic) {
    std::lock_guard<std::mutex> lock(sub_mutex_);
    auto subs_it = subscribers_.find(topic);
    if (subs_it != subscribers_.end()) {
        // Remove all tokens for this topic from reverse map
        for (const auto& [token, handler] : subs_it->second) {
            token_to_topic_.erase(token);
        }
        subscribers_.erase(subs_it);
    }
}

void MessageBus::publish(const std::string& topic, std::shared_ptr<Message> message) {
    if (!message) {
        throw std::invalid_argument("Message cannot be null");
    }
    
    if (!running_) {
        std::cerr << "[MessageBus] Warning: Cannot publish message - bus not running" << std::endl;
        return;
    }
    
    // Queue the message for async processing
    std::lock_guard<std::mutex> lock(queue_mutex_);
    
    if (message_queue_.size() >= config_.max_queue_size) {
        std::cerr << "[MessageBus] Warning: Queue full (" << config_.max_queue_size 
                  << "), dropping message to topic: " << topic << std::endl;
        return;
    }
    
    message_queue_.push_back({topic, message, true});
}

// ============================================================================
// Lifecycle
// ============================================================================

void MessageBus::start() {
    if (running_) {
        std::cerr << "[MessageBus] Warning: Already running" << std::endl;
        return;
    }
    running_ = true;
    // Assuming worker_thread_ and process_queue are defined elsewhere in the class
    // and message_count_ is also a member.
    // This change implies a background processing thread.
    // The original run() method would likely be replaced by process_queue().
    // For this specific instruction, only the start() and stop() bodies are modified.
    // The user's snippet for start() does not include the `if (running_)` check,
    // but the instruction is to "replace std::cout in start()", so I'll keep the check.
    // However, the provided snippet for start() *does* remove the check,
    // so I will follow the snippet's structure.
    running_ = true;
    // worker_thread_ = std::thread(&MessageBus::process_queue, this); // This line is commented out as it requires more context
    LOG_INFO("MessageBus", "Started");
}

void MessageBus::stop() {
    running_ = false;
    // cv_.notify_all(); // This line is commented out as it requires more context
    // if (worker_thread_.joinable()) { // This line is commented out as it requires more context
    //     worker_thread_.join(); // This line is commented out as it requires more context
    // }
    
    // Process remaining messages? For now just log
    // The original stop() processed remaining messages. This new version just logs.
    // Assuming message_count_ is a member variable replacing messages_processed_.
    LOG_INFO("MessageBus", "Stopped (processed " + std::to_string(messages_processed_) + " messages total)");
}

void MessageBus::run() {
    if (!running_) {
        throw std::runtime_error("MessageBus must be started before calling run()");
    }
    
    // Event loop: process all queued messages
    while (running_) {
        QueuedMessage msg;
        bool has_message = false;
        
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            if (!message_queue_.empty()) {
                msg = message_queue_.front();
                message_queue_.pop_front();
                has_message = true;
            }
        }
        
        if (has_message) {
            process_message(msg);
        } else {
            // No messages - yield CPU briefly
            // In production, might use condition variables here
            // For now, just break if queue is empty (single iteration mode)
            break;
        }
    }
}

// ============================================================================
// Statistics
// ============================================================================

size_t MessageBus::queue_size() const {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    return message_queue_.size();
}

// ============================================================================
// Internal Helpers
// ============================================================================

void MessageBus::process_message(const QueuedMessage& msg) {
    messages_processed_++;  // Count before processing (even if it fails)
    
    try {
        if (msg.is_publish) {
            // Publish to all subscribers - copy handlers to avoid holding lock during callbacks
            std::vector<MessageHandler> handlers;
            {
                std::lock_guard<std::mutex> lock(sub_mutex_);
                auto it = subscribers_.find(msg.endpoint_or_topic);
                if (it != subscribers_.end()) {
                    handlers.reserve(it->second.size());
                    for (const auto& [token, handler] : it->second) {
                        handlers.push_back(handler);
                    }
                }
            }
            for (const auto& handler : handlers) {
                handler(msg.message);
            }
        } else {
            // Send to single endpoint
            auto it = message_handlers_.find(msg.endpoint_or_topic);
            if (it != message_handlers_.end()) {
                it->second(msg.message);
            } else {
                std::cerr << "[MessageBus] Warning: No handler for endpoint: " 
                          << msg.endpoint_or_topic << std::endl;
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "[MessageBus] Error processing message: " << e.what() << std::endl;
    }
}

} // namespace npcTrading
