//
// Created by xiaol on 11/8/2019.
//

#ifndef SPARKPP_EXECUTOR_HPP
#define SPARKPP_EXECUTOR_HPP

#include <utility>

#include "common.hpp"
#include "serialize_wrapper.hpp"
#include "serialize_capnp.hpp"
#include "rdd/rdd.hpp"
#include <boost/asio.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/post.hpp>
#include "data.capnp.h"

using namespace boost::asio;

struct Executor {
    pair<string, uint16_t> masterAddress;
    uint16_t port;
    thread_pool pool{std::thread::hardware_concurrency()};
    Executor(pair<string, uint16_t> masterAddress_, uint16_t port_)
        : masterAddress{move(masterAddress_)}, port{port_} {}
    void run() {
        io_service ioc;
        ip::tcp::endpoint endpoint{ip::tcp::v4(), port};
        ip::tcp::acceptor acceptor{ioc, endpoint};
        while (true) {
            ip::tcp::socket socket{ioc};
            acceptor.accept(socket);
            post(pool, [socket = move(socket)]() mutable {
                // read & serialize
                int fd = socket.native_handle();
                ::capnp::PackedFdMessageReader message{fd};
                auto task = recvExecution(message);
                auto s = task->run(0);
                // write result back
                sendData<Result>(fd, s.v);
            });
        }
    }
};

#endif //SPARKPP_EXECUTOR_HPP
