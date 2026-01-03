#include "npcTrading/clock.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

using namespace npcTrading;

namespace {
Timestamp ts_ms(int64_t ms) {
    return Timestamp(std::chrono::milliseconds(ms));
}
} // namespace

// -----------------------------------------------------------------------------
// LiveClock tests
// -----------------------------------------------------------------------------

TEST(LiveClockTest, NowCloseToSystemClock) {
    LiveClock clock;
    auto before = std::chrono::system_clock::now();
    auto clk = clock.now();
    auto after = std::chrono::system_clock::now();

    // clock.now() should be between before/after; tolerate small drift
    EXPECT_LE(before, clk);
    EXPECT_GE(after, clk);
}

TEST(LiveClockTest, FiresScheduledCallback) {
    LiveClock clock;
    std::atomic<int> fired{0};
    auto due = clock.now() + std::chrono::milliseconds(50);

    clock.schedule_callback(due, [&]() { fired.fetch_add(1); });

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_EQ(fired.load(), 1);
}

TEST(LiveClockTest, CancelPreventsCallback) {
    LiveClock clock;
    std::atomic<int> fired{0};
    auto due = clock.now() + std::chrono::milliseconds(100);

    int id = clock.schedule_callback(due, [&]() { fired.fetch_add(1); });
    clock.cancel_callback(id);

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_EQ(fired.load(), 0);
}

// -----------------------------------------------------------------------------
// SimulatedClock tests
// -----------------------------------------------------------------------------

TEST(SimulatedClockTest, NowAndTimestampNsAdvanceWithSetTime) {
    auto start = ts_ms(0);
    SimulatedClock clock(start);

    EXPECT_EQ(clock.now(), start);
    EXPECT_EQ(clock.timestamp_ns(), std::chrono::duration_cast<std::chrono::nanoseconds>(start.time_since_epoch()).count());

    auto later = ts_ms(1500);
    clock.set_time(later);
    EXPECT_EQ(clock.now(), later);
    EXPECT_EQ(clock.timestamp_ns(), std::chrono::duration_cast<std::chrono::nanoseconds>(later.time_since_epoch()).count());
}

TEST(SimulatedClockTest, ExecutesCallbacksWhenTimeAdvancesPastDue) {
    auto start = ts_ms(0);
    SimulatedClock clock(start);
    std::atomic<int> fired{0};

    clock.schedule_callback(ts_ms(1000), [&]() { fired.fetch_add(1); });
    clock.process_pending_callbacks();
    EXPECT_EQ(fired.load(), 0); // not yet due

    clock.set_time(ts_ms(1500));
    clock.process_pending_callbacks();
    EXPECT_EQ(fired.load(), 1); // now due
}

TEST(SimulatedClockTest, CancelSkipsCallback) {
    auto start = ts_ms(0);
    SimulatedClock clock(start);
    std::atomic<int> fired{0};

    int id = clock.schedule_callback(ts_ms(500), [&]() { fired.fetch_add(1); });
    clock.cancel_callback(id);
    clock.set_time(ts_ms(600));
    clock.process_pending_callbacks();

    EXPECT_EQ(fired.load(), 0);
}

TEST(SimulatedClockTest, ExecutesCallbacksInTimeOrder) {
    auto start = ts_ms(0);
    SimulatedClock clock(start);
    std::vector<int> order;

    clock.schedule_callback(ts_ms(300), [&]() { order.push_back(3); });
    clock.schedule_callback(ts_ms(100), [&]() { order.push_back(1); });
    clock.schedule_callback(ts_ms(200), [&]() { order.push_back(2); });

    clock.set_time(ts_ms(500));
    clock.process_pending_callbacks();

    ASSERT_EQ(order.size(), 3u);
    EXPECT_EQ(order[0], 1);
    EXPECT_EQ(order[1], 2);
    EXPECT_EQ(order[2], 3);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
