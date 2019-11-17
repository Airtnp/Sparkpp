//
// Created by xiaol on 11/8/2019.
//

#ifndef SPARKPP_THREAD_POOL_HPP
#define SPARKPP_THREAD_POOL_HPP

#include <functional>
#include <thread>
#include <future>
#include <deque>

// ENHANCE: refer to rust [ThreadPool](https://docs.rs/threadpool/1.7.1/src/threadpool/lib.rs.html)
/// A thread pool from sn_Thread
class WorkQueue{
public:
    explicit WorkQueue(int numWorkers = -1) {
        if (numWorkers < 1) {
            numWorkers = std::thread::hardware_concurrency() - 1;
        }
        while (numWorkers--) {
            m_workers.emplace_back(std::thread(&WorkQueue::do_work, this));
        }
    }
    ~WorkQueue() {
        abort();
    }

    void abort() {
        m_exit = true;
        m_finish_work = false;
        m_signal.notify_all();
        join_all();

        {
            std::lock_guard<std::mutex> lg(m_mutex);
            m_work.clear();
        }
    }

    void stop() {
        m_exit = true;
        m_finish_work = true;
        m_signal.notify_all();
    }

    void wait_for_completion() {
        stop();
        join_all();
    }

    template<typename RETVAL>
    std::future<RETVAL> submit(std::function<RETVAL()>&& function) {
        if (m_exit) {
            throw std::runtime_error("Caught work submission to work queue that is desisting.");
        }

        // Workaround for lack of lambda move capture
        typedef std::pair<std::promise<RETVAL>, std::function<RETVAL()>> retpair_t;
        std::shared_ptr<retpair_t> data = std::make_shared<retpair_t>(std::promise<RETVAL>(), std::move(function));

        std::future<RETVAL> future = data->first.get_future();

        {
            std::lock_guard<std::mutex> lg(m_mutex);
            m_work.emplace_back([data](){
                try {
                    data->first.set_value(data->second());
                }
                catch (...) {
                    data->first.set_exception(std::current_exception());
                }
            });
        }
        m_signal.notify_one();
        return std::move(future);
    }

    template <typename F, typename ...Args>
    auto submit(F&& func, Args&&... args) {
        // maybe use std::packaged_task
        using result_t = std::result_of_t<F(Args...)>;
        std::function<result_t()> xfunc = std::bind(
                std::forward<F>(func),
                std::forward<Args>(args)...
        );
        return this->submit(std::move(xfunc));
    }

private:
    // maybe use boost::lockfree::queue
    // or some thread-safe queue
    std::deque<std::function<void()>> m_work;
    std::mutex m_mutex;
    // notice the thread to work
    std::condition_variable m_signal;
    std::atomic<bool> m_exit{false};
    std::atomic<bool> m_finish_work{true};
    // threads
    std::vector<std::thread> m_workers;

    void do_work(){
        std::unique_lock<std::mutex> ul(m_mutex);
        while (!m_exit || (m_finish_work && !m_work.empty())) {
            if (!m_work.empty()) {
                std::function<void()> work(std::move(m_work.front()));
                m_work.pop_front();
                ul.unlock();
                work();
                ul.lock();
            }
            else {
                m_signal.wait(ul);
            }
        }
    }

    void join_all(){
        for (auto& thread : m_workers) {
            thread.join();
        }
        m_workers.clear();
    }

    void operator=(const WorkQueue&) = delete;
    WorkQueue(const WorkQueue&) = delete;
};

template<>
std::future<void> WorkQueue::submit(std::function<void()>&& function) {
    if (m_exit) {
        throw std::runtime_error("Caught work submission to work queue that is desisting.");
    }
    // Workaround for lack of lambda move capture
    typedef std::pair<std::promise<void>, std::function<void()>> retpair_t;
    std::shared_ptr<retpair_t> data = std::make_shared<retpair_t>(std::promise<void>(), std::move(function));

    std::future<void> future = data->first.get_future();

    {
        std::lock_guard<std::mutex> lg(m_mutex);
        m_work.emplace_back([data](){
            try {
                data->second();
                data->first.set_value();
            }
            catch (...) {
                data->first.set_exception(std::current_exception());
            }
        });
    }
    m_signal.notify_one();

    return std::move(future);
}

#endif //SPARKPP_THREAD_POOL_HPP
