//
// Created by xiaol on 11/12/2019.
//

#ifndef SPARKPP_EVENT_LOOP_HPP
#define SPARKPP_EVENT_LOOP_HPP

#include <atomic>
#include <stdexcept>
#include <thread>
#include "concurrentqueue/blockingconcurrentqueue.h"
using moodycamel::BlockingConcurrentQueue;

// DefaultConstructible
template <typename E>
struct EventLoop {
    BlockingConcurrentQueue<E> eventQueue;
    std::atomic<bool> stopped;
    std::thread thd;
    void onStart() {}
    void onStop() {}

    void onReceive(E event) {}
    void onError(std::exception_ptr) {}

    void start() {
        onStart();
        thd = std::thread{[this]() {
            while (!stopped.load()) {
                try {
                    E e;
                    eventQueue.wait_dequeue(e);
                    onReceive(e);
                } catch(...) {
                    onError(std::current_exception());
                }
            }
        }};
        thd.detach();
    }

    void stop() {
        bool v = false;
        if (stopped.compare_exchange_strong(v, true)) {
            thd.join();
        }
    }
};

#endif //SPARKPP_EVENT_LOOP_HPP
