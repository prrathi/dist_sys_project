#ifndef SAFEQUEUE_H
#define SAFEQUEUE_H

#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>

template <typename T>
class SafeQueue {
public:

    void enqueue(const T& item) {
        std::lock_guard<std::mutex> lock(mu_);
        queue_.push(item);
        cv_.notify_one();
    }

    // false if the queue is finished or empty
    // pass some default T to it
    bool dequeue(T& item) {
        std::unique_lock<std::mutex> lock(mu_);
        // wait until the queue is not empty or finished is true
        cv_.wait(lock, [this]() { return !queue_.empty() || finished_; });

        if (queue_.empty()) {
            return false;
        }

        item = queue_.front();
        queue_.pop();
        return true;
    }

    bool peek(T& item) {
        std::lock_guard<std::mutex> lock(mu_);
        if (queue_.empty()) {
            return false;
        }
        item = queue_.front();
        return true;
    }

    // stop
    void set_finished() {
        std::lock_guard<std::mutex> lock(mu_);
        finished_ = true;
        cv_.notify_all();
    }

private:
    std::queue<T> queue_;
    std::mutex mu_;
    std::condition_variable cv_;
    bool finished_ = false;
};

#endif