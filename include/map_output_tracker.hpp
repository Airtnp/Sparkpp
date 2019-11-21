//
// Created by xiaol on 11/14/2019.
//

#ifndef SPARKPP_MAP_OUTPUT_TRACKER_HPP
#define SPARKPP_MAP_OUTPUT_TRACKER_HPP

#include "common.hpp"
#include "serialize_capnp.hpp"
#include <boost/asio.hpp>

using namespace boost::asio;

struct MapOutputTracker {
    bool isMaster;
    parallel_flat_hash_map<size_t, vector<string>> server_urls;
    unordered_set<size_t> fetching;
    mutex fetching_lck;
    std::condition_variable cv;
    addr_t masterAddr;
    mutex generation_lck;
    int64_t generation;

    MapOutputTracker(bool isMaster_, addr_t masterAddr_)
        : isMaster{isMaster_}, masterAddr{move(masterAddr_)}, generation{0} {
        if (isMaster)
            server();
    }

    void server() {
        thread thd{
                [this]() {
                    io_service ioc;
                    ip::tcp::endpoint endpoint{ip::tcp::v4(), masterAddr.second};
                    ip::tcp::acceptor acceptor{ioc, endpoint};
                    while (true) {
                        ip::tcp::socket socket{ioc};
                        acceptor.accept(socket);
                        thread per_conn{
                                [this, socket = move(socket)]() mutable {
                                    int fd = socket.native_handle();
                                    ::capnp::PackedFdMessageReader message{fd};
                                    auto result = recvData<Message>(message);
                                    size_t shuffleId;
                                    deserialize(shuffleId, result);
                                    if (server_urls.find(shuffleId) != server_urls.end()) {
                                        auto v = server_urls[shuffleId];
                                        vector<char> bytes;
                                        serialize(v, bytes);
                                        sendData<Message>(fd, bytes);
                                    }
                                }
                        };
                        per_conn.detach();
                    }
                }
        };
        thd.detach();
    }

    vector<string> client(size_t shuffleId) {
        io_service ioc;
        ip::tcp::resolver resolver{ioc};
        ip::tcp::resolver::query query{masterAddr.first, std::to_string(masterAddr.second),
                                       boost::asio::ip::resolver_query_base::numeric_service};
        auto iter = resolver.resolve(query);
        ip::tcp::resolver::iterator end;
        ip::tcp::endpoint endpoint = *iter;
        ip::tcp::socket socket{ioc};
        boost::system::error_code ec;
        do {
            auto start_iter = iter;
            ec.clear();
            socket.close();
            std::this_thread::sleep_for(5ms);
            while (start_iter != end) {
                socket.connect(endpoint, ec);
                if (!ec) break;
                ++start_iter;
            }
        } while (ec);
        int fd = socket.native_handle();
        vector<char> bytes;
        serialize(shuffleId, bytes);
        sendData<Message>(fd, bytes);
        ::capnp::PackedFdMessageReader message{fd};
        auto reader = recvData<Message>(message);
        vector<string> res;
        deserialize(res, reader);
        return res;
    }

    void increaseGeneration() {
        unique_lock lk{generation_lck};
        generation += 1;
    }

    void registerShuffle(size_t shuffleId, size_t numMaps) {
        if (server_urls.find(shuffleId) == server_urls.end()) {
            server_urls[shuffleId].resize(numMaps);
        }
    }

    void registerMapOutput(size_t shuffleId, size_t mapId, string uri) {
        auto& v = server_urls[shuffleId];
        /// FIXME: lock each vector?
        v[mapId] = move(uri);
    }

    void registerMapOutputs(size_t shuffleId, vector<string> locs) {
        server_urls[shuffleId] = move(locs);
    }

    void unregisterMapOutput(size_t shuffleId, int mapId, string uri) {
        // assert(server_urls.find(shuffleId) != server_urls.end())
        auto& v = server_urls[shuffleId];
        v[mapId].clear();
        increaseGeneration();
    }

    vector<string> getServerUris(size_t shuffleId) {
        if (server_urls.count(shuffleId)) {
            return server_urls[shuffleId];
        }
        unique_lock lk{fetching_lck};
        cv.wait(lk, [this, shuffleId]() {
            return !fetching.count(shuffleId);
        });
        if (server_urls.count(shuffleId)) {
            return server_urls[shuffleId];
        }
        fetching.insert(shuffleId);
        auto v = client(shuffleId);
        server_urls[shuffleId] = v;
        fetching.erase(shuffleId);
        cv.notify_all();
        return v;
    }

    int64_t getGeneration() {
        unique_lock lk{generation_lck};
        return generation;
    }

    void updateGeneration(int64_t v) {
        unique_lock lk{generation_lck};
        if (v > generation) {
            server_urls.clear();
            generation = v;
        }
    }
};

#endif //SPARKPP_MAP_OUTPUT_TRACKER_HPP
