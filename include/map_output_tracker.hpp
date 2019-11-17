//
// Created by xiaol on 11/14/2019.
//

#ifndef SPARKPP_MAP_OUTPUT_TRACKER_HPP
#define SPARKPP_MAP_OUTPUT_TRACKER_HPP

#include "common.hpp"
#include "serialize_capnp.hpp"

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
                    auto listener = TcpListener::bind(masterAddr.second);
                    while (true) {
                        auto st = listener.accept();
                        thread per_conn{
                                [this, st = move(st)]() {
                                    auto result = recvData<Message>(st.fd);
                                    size_t shuffleId;
                                    deserialize(shuffleId, result);
                                    if (server_urls.find(shuffleId) != server_urls.end()) {
                                        auto v = server_urls[shuffleId];
                                        vector<char> bytes;
                                        serialize(v, bytes);
                                        sendData<Message>(st.fd, bytes);
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
        auto opt_st = TcpStream::connect(masterAddr.first.c_str(), masterAddr.second);
        do {
            opt_st = TcpStream::connect(masterAddr.first.c_str(), masterAddr.second);
        } while (!opt_st.is_initialized());
        auto st = move(opt_st.value());
        vector<char> bytes;
        serialize(shuffleId, bytes);
        sendData<Message>(st.fd, bytes);
        auto reader = recvData<Message>(st.fd);
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
