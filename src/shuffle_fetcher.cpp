//
// Created by xiaol on 11/14/2019.
//

#include "shuffle_fetcher.hpp"
#include "spark_env.hpp"
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/post.hpp>
#include <boost/serialization/vector.hpp>
#include <fmt/include/fmt/format.h>

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


template<typename F>
void ParallelShuffleFetcher::fetch(size_t shuffleId, size_t reduceId, F &&func) {
    using K = typename GetType<F>::K;
    using V = typename GetType<F>::V;
    auto uris = env.mapOutputTracker->getServerUris(shuffleId);
    unordered_map<string, vector<size_t>> inputsByUri;
    size_t totalResults = 0;
    for (size_t i = 0; i < uris.size(); ++i) {
        inputsByUri[uris[i]].push_back(i);
    }
    // # of threads fetching blocks
    size_t parallelFetches = 10;
    BlockingConcurrentQueue<pair<string, vector<int>>> serverQueue;
    for (const auto& [k, v] : inputsByUri) {
        serverQueue.enqueue(make_pair(k, v));
        totalResults += v.size();
    }
    BlockingConcurrentQueue<vector<pair<K, V>>> resultQueue;
    net::thread_pool pool{parallelFetches};
    for (size_t i = 0; i < parallelFetches; ++i) {
        post(pool, [&]() {
            while (true) {
                pair<string, vector<int>> p;
                serverQueue.wait_dequeue(p);
                auto& host = p.first;
                auto& ids = p.second;
                for (int inputId : ids) {
                    string target = fmt::format("/shuffle/{}.{}.{}", shuffleId, inputId, reduceId);

                    net::io_context ioc;
                    tcp::resolver resolver{ioc};
                    beast::tcp_stream stream{ioc};
                    tcp::resolver::query query{host, "80"};
                    const auto results = resolver.resolve(query);
                    stream.connect(results);

                    http::request<http::string_body> req{
                        http::verb::get, target, 11
                    };
                    req.set(http::field::host, p.first);
                    req.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
                    http::write(stream, req);

                    beast::flat_buffer buffer;
                    http::response<http::dynamic_body> res;
                    http::read(stream, buffer, res);
                    beast::error_code ec;
                    stream.socket().shutdown(tcp::socket::shutdown_both, ec);
                    vector<pair<K, V>> data;
                    boost::iostreams::basic_array_source<char> device{
                        req.body().data(), req.body().size()
                    };
                    boost::iostreams::stream<boost::iostreams::basic_array_source<char>> s{device};
                    boost::archive::binary_iarchive ia{s};
                    ia >> data;
                    resultQueue.enqueue(move(data));
                }
            }
        });
    }
    size_t resultDone = 0;
    while (resultDone < totalResults) {
        vector<pair<K, V>> result;
        resultQueue.wait_dequeue(result);
        for (auto&& p : result) {
            invoke(forward<F>(func), move(p));
        }
        resultDone += 1;
    }
}