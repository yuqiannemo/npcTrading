#pragma once

#include <string>
#include <memory>
#include <mutex>
#include <iostream>
#include <sstream>

namespace npcTrading {

enum class LogLevel {
    LVL_DEBUG = 0,
    LVL_INFO = 1,
    LVL_WARN = 2,
    LVL_ERROR = 3,
    LVL_OFF = 4
};

class Logger {
public:
    static Logger& instance();
    
    void set_level(LogLevel level);
    LogLevel get_level() const;
    
    void log(LogLevel level, const std::string& tag, const std::string& message);
    
private:
    Logger() = default;
    ~Logger() = default;
    
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;
    
    LogLevel level_ = LogLevel::LVL_INFO;
    std::mutex mutex_;
};

} // namespace npcTrading

// Helper macros for easy logging
#define LOG_INTERNAL(level, level_enum, tag, msg) \
    if (::npcTrading::Logger::instance().get_level() <= level_enum) { \
        std::ostringstream oss; \
        oss << msg; \
        ::npcTrading::Logger::instance().log(level_enum, tag, oss.str()); \
    }

#define LOG_DEBUG(tag, msg) LOG_INTERNAL("DEBUG", ::npcTrading::LogLevel::LVL_DEBUG, tag, msg)
#define LOG_INFO(tag, msg)  LOG_INTERNAL("INFO",  ::npcTrading::LogLevel::LVL_INFO,  tag, msg)
#define LOG_WARN(tag, msg)  LOG_INTERNAL("WARN",  ::npcTrading::LogLevel::LVL_WARN,  tag, msg)
#define LOG_ERROR(tag, msg) LOG_INTERNAL("ERROR", ::npcTrading::LogLevel::LVL_ERROR, tag, msg)
