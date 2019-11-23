//
// Created by xiaol on 11/13/2019.
//

#ifndef SPARKPP_SPARK_ENV_HPP
#define SPARKPP_SPARK_ENV_HPP

#include "common.hpp"
#include "cache.hpp"
#include "cache_tracker.hpp"
#include "map_output_tracker.hpp"
#include "shuffle_fetcher.hpp"
#include "shuffle_manager.hpp"

struct SparkEnv {
    BoundedMemoryCache cache;
    unique_ptr<MapOutputTracker> mapOutputTracker;
    unique_ptr<CacheTracker> cacheTracker;
    unique_ptr<ParallelShuffleFetcher> shuffleFetcher;
    unique_ptr<ShuffleManager> shuffleManager;
    void init(int argc, char** argv, const addr_t& masterAddr);
};

extern SparkEnv env;


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
    size_t parallelFetches = 4;
    BlockingConcurrentQueue<pair<string, vector<size_t>>> serverQueue;
    for (const auto& [k, v] : inputsByUri) {
        serverQueue.enqueue(make_pair(k, v));
        totalResults += v.size();
    }
    BlockingConcurrentQueue<vector<pair<K, V>>> resultQueue;
    net::thread_pool pool{parallelFetches};
    for (size_t i = 0; i < parallelFetches; ++i) {
        post(pool, [&]() {
            while (true) {
                pair<string, vector<size_t>> p;
                auto found = serverQueue.try_dequeue(p);
                if (!found) {
                    break;
                }
                auto& host = p.first;
                auto& ids = p.second;
                for (int inputId : ids) {
                    string target = fmt::format("/shuffle/{}.{}.{}", shuffleId, inputId, reduceId);

                    net::io_context ioc;
                    tcp::resolver resolver{ioc};
                    beast::tcp_stream stream{ioc};
                    tcp::resolver::query query{host, "28001",
                                               boost::asio::ip::resolver_query_base::numeric_service};
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
                    // FIXME: lots of copies here
                    string body{
                        boost::asio::buffers_begin(res.body().data()),
                        boost::asio::buffers_end(res.body().data())
                    };
                    std::stringstream ss{move(body)};
                    boost::archive::binary_iarchive ia{ss, boost::archive::no_header | boost::archive::no_tracking};
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


#endif //SPARKPP_SPARK_EMV_HPP
