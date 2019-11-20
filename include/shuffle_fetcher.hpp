//
// Created by xiaol on 11/14/2019.
//

#ifndef SPARKPP_SHUFFLE_FETCHER_HPP
#define SPARKPP_SHUFFLE_FETCHER_HPP

#include "common.hpp"
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/post.hpp>
#include <boost/serialization/vector.hpp>
#include <fmt/format.h>

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
using tcp = net::ip::tcp;

template <typename F>
struct GetType {
    using trait = function_traits<F>;
    using P = typename trait::template args<0>::type;
    using K = typename P::first_type;
    using V = typename P::second_type;
};


struct ParallelShuffleFetcher {
    template <typename F>
    void fetch(size_t shuffleId, size_t reduceId, F&& func);
};



#endif //SPARKPP_SHUFFLE_FETCHER_HPP
