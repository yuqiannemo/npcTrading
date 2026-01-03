#include "npcTrading/logger.hpp"
#include <chrono>
#include <iomanip>
#include <ctime>

namespace npcTrading {

Logger& Logger::instance() {
    static Logger instance;
    return instance;
}

void Logger::set_level(LogLevel level) {
    level_ = level;
}

LogLevel Logger::get_level() const {
    return level_;
}

void Logger::log(LogLevel level, const std::string& tag, const std::string& message) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    
    std::string level_str;
    switch (level) {
        case LogLevel::LVL_DEBUG: level_str = "DEBUG"; break;
        case LogLevel::LVL_INFO:  level_str = "INFO "; break; // Pad for alignment
        case LogLevel::LVL_WARN:  level_str = "WARN "; break;
        case LogLevel::LVL_ERROR: level_str = "ERROR"; break;
        default: level_str = "UNKNOWN"; break;
    }
    
    // Format: [YYYY-MM-DD HH:MM:SS.mmm] [LEVEL] [TAG] Message
    std::cout << "[" << std::put_time(std::localtime(&time), "%Y-%m-%d %H:%M:%S")
              << "." << std::setfill('0') << std::setw(3) << ms.count() << "] "
              << "[" << level_str << "] "
              << "[" << tag << "] "
              << message << std::endl;
}

} // namespace npcTrading
