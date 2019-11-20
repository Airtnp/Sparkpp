//
// Created by xiaol on 11/13/2019.
//

#ifndef SPARKPP_CACHE_TRACKER_HPP
#define SPARKPP_CACHE_TRACKER_HPP

#include "common.hpp"
#include "cache.hpp"
#include "rdd/rdd.hpp"
#include "serialize_wrapper.hpp"
#include "serialize_capnp.hpp"
#include <boost/asio.hpp>

using namespace boost::asio;

struct CacheTrackerMessage {
    struct AddedToCache {
        int rddId;
        int partition;
        host_t host;
        size_t size;
        SN_BOOST_SERIALIZE_MEMBERS_IN(rddId, partition, host, size);
    };
    struct DroppedFromCache {
        int rddId;
        int partition;
        host_t host;
        size_t size;
        SN_BOOST_SERIALIZE_MEMBERS_IN(rddId, partition, host, size);
    };
    struct MemoryCacheLost {
        host_t host;
        SN_BOOST_SERIALIZE_MEMBERS_IN(host);
    };
    struct RegisterRDD {
        int rddId;
        int numPartitions;
        SN_BOOST_SERIALIZE_MEMBERS_IN(rddId, numPartitions);
    };
    struct SlaveCacheStarted {
        host_t host;
        size_t size;
        SN_BOOST_SERIALIZE_MEMBERS_IN(host, size);
    };
    struct GetCacheStatus {
        SN_BOOST_SERIALIZE_EMPTY();
    };
    struct GetCacheLocations {
        SN_BOOST_SERIALIZE_EMPTY();
    };
    struct StopCacheTracker {
        SN_BOOST_SERIALIZE_EMPTY();
    };
    variant<AddedToCache,
            DroppedFromCache,
            MemoryCacheLost,
            RegisterRDD,
            SlaveCacheStarted,
            GetCacheLocations,
            GetCacheStatus,
            StopCacheTracker> vmember;
    auto& get() {
        return vmember;
    }
    const auto& get() const {
        return vmember;
    }
};
BOOST_IS_BITWISE_SERIALIZABLE(CacheTrackerMessage::AddedToCache);
BOOST_IS_BITWISE_SERIALIZABLE(CacheTrackerMessage::DroppedFromCache);
BOOST_IS_BITWISE_SERIALIZABLE(CacheTrackerMessage::MemoryCacheLost);
BOOST_IS_BITWISE_SERIALIZABLE(CacheTrackerMessage::RegisterRDD);
BOOST_IS_BITWISE_SERIALIZABLE(CacheTrackerMessage::SlaveCacheStarted);
BOOST_IS_BITWISE_SERIALIZABLE(CacheTrackerMessage::GetCacheStatus);
BOOST_IS_BITWISE_SERIALIZABLE(CacheTrackerMessage::GetCacheLocations);
BOOST_IS_BITWISE_SERIALIZABLE(CacheTrackerMessage::StopCacheTracker);


struct CacheTrackerReply {
    struct CacheLocations {
        unordered_map<size_t, vector<list<host_t>>> locs;
        SN_BOOST_SERIALIZE_MEMBERS_IN(locs);
    };
    struct CacheStatus {
        // host, capacity, usage
        vector<tuple<host_t, size_t, size_t>> status;
        SN_BOOST_SERIALIZE_MEMBERS_IN(status);
    };
    struct Ok {
        SN_BOOST_SERIALIZE_EMPTY();
    };
    variant<CacheLocations, CacheStatus, Ok> vmember;
    variant<CacheLocations, CacheStatus, Ok>& get() {
        return vmember;
    }
    const auto& get() const {
        return vmember;
    }
};
BOOST_IS_BITWISE_SERIALIZABLE(CacheTrackerReply::CacheLocations);
BOOST_IS_BITWISE_SERIALIZABLE(CacheTrackerReply::CacheStatus);
BOOST_IS_BITWISE_SERIALIZABLE(CacheTrackerReply::Ok);


struct CacheTracker {
    bool isMaster;
    // I hope there is something acts like Arc<T>...
    shared_mutex locs_lck;
    unordered_map<size_t, vector<list<host_t>>> locs;
    shared_mutex slaveCapacity_lck;
    unordered_map<host_t, size_t> slaveCapacity;
    shared_mutex slaveUsage_lck;
    unordered_map<host_t, size_t> slaveUsage;
    shared_mutex registeredRddIds_lck;
    unordered_set<size_t> registeredRddIds;
    std::condition_variable loading_cv;
    mutex loading_lck;
    unordered_set<pair<size_t, size_t>, pair_hash> loading;
    // per worker
    addr_t masterAddr;
    KeySpace cache;
    CacheTracker(bool isMaster_, addr_t masterAddr_, BoundedMemoryCache& cache_)
        : isMaster{isMaster_}, masterAddr{move(masterAddr_)},
        cache{cache_.newKeySpace()} {
        masterAddr.second += 1;
        if (isMaster) {
            server();
        }
        client(CacheTrackerMessage{
                .vmember = {
                        CacheTrackerMessage::SlaveCacheStarted{
                                .host = std::getenv("SPARK_LOCAL_IP"),
                                .size = cache.getCapacity()
                        }
                }
        });
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
                            ::capnp::PackedFdMessageReader xmessage{fd};
                            auto result = recvData<Message>(xmessage);
                            auto message = CacheTrackerMessage{};
                            deserialize(
                                message.get(),
                                result
                            );
                            auto reply = match(
                                    message.get(),
                                    [this](const CacheTrackerMessage::SlaveCacheStarted& msg) {
                                        unique_lock lk1{slaveCapacity_lck};
                                        unique_lock lk2{slaveUsage_lck};
                                        slaveCapacity[msg.host] = msg.size;
                                        return CacheTrackerReply{
                                            .vmember = CacheTrackerReply::Ok{}
                                        };
                                    },
                                    [this](const CacheTrackerMessage::RegisterRDD& msg) {
                                        unique_lock lk{locs_lck};
                                        locs[msg.rddId].resize(msg.numPartitions);
                                        return CacheTrackerReply{
                                                .vmember = CacheTrackerReply::Ok{}
                                        };
                                    },
                                    [this](const CacheTrackerMessage::AddedToCache& msg) {
                                        if (msg.size > 0) {
                                            unique_lock lk{slaveUsage_lck};
                                            slaveUsage[msg.host] += msg.size;
                                        }
                                        unique_lock lk{locs_lck};
                                        if (locs.find(msg.rddId) != locs.end()) {
                                            auto& v = locs[msg.rddId];
                                            if ((int)v.size() > msg.partition) {
                                                v[msg.partition].push_front(msg.host);
                                            }
                                        }
                                        return CacheTrackerReply{
                                                .vmember = CacheTrackerReply::Ok{}
                                        };
                                    },
                                    [this](const CacheTrackerMessage::DroppedFromCache& msg) {
                                        if (msg.size > 0) {
                                            unique_lock lk{slaveUsage_lck};
                                            slaveUsage[msg.host] -= msg.size;
                                        }
                                        unique_lock lk{locs_lck};
                                        if (locs.find(msg.rddId) != locs.end()) {
                                            auto& v = locs[msg.rddId];
                                            if ((int)v.size() > msg.partition) {
                                                auto& l = v[msg.partition];
                                                l.erase(remove_if(l.begin(), l.end(), [&](const host_t& h){
                                                    return h == msg.host;
                                                }));
                                            }
                                        }
                                        return CacheTrackerReply{
                                                .vmember = CacheTrackerReply::Ok{}
                                        };
                                    },
                                    [this](const CacheTrackerMessage::GetCacheLocations&) {
                                        shared_lock lk{locs_lck};
                                        return CacheTrackerReply{
                                                .vmember = CacheTrackerReply::CacheLocations{locs}
                                        };
                                    },
                                    [this](const CacheTrackerMessage::GetCacheStatus&) {
                                        shared_lock lk1{slaveCapacity_lck};
                                        shared_lock lk2{slaveUsage_lck};
                                        vector<tuple<host_t, size_t, size_t>> status;
                                        status.reserve(slaveCapacity.size());
                                        for (auto& v : slaveCapacity) {
                                            status.push_back(
                                                    make_tuple(v.first, v.second, slaveUsage[v.first])
                                            );
                                        }
                                        return CacheTrackerReply{
                                            .vmember = CacheTrackerReply::CacheStatus{move(status)}
                                        };
                                    }
                            );
                            vector<char> rbytes;
                            serialize(reply.get(), rbytes);
                            sendData<Message>(fd, rbytes);
                        }
                    };
                    per_conn.detach();
                }
            }
        };
        thd.detach();
    }

    CacheTrackerReply client(const CacheTrackerMessage& message) {
        io_service ioc;
        ip::tcp::resolver resolver{ioc};
        ip::tcp::resolver::query query{masterAddr.first, std::to_string(masterAddr.second)};
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
        serialize(message.get(), bytes);
        sendData<Message>(fd, bytes);
        ::capnp::PackedFdMessageReader xmessage{fd};
        auto reader = recvData<Message>(xmessage);
        CacheTrackerReply reply;
        deserialize(reply.get(), reinterpret_cast<const char*>(reader.asBytes().begin()), reader.size());
        return reply;
    }

    unordered_map<size_t, vector<vector<host_t>>> getLocationsSnapshot() {
        auto reply = client(CacheTrackerMessage{
            .vmember = CacheTrackerMessage::GetCacheLocations{}
        });
        auto xlocs = match(reply.get(), [](const CacheTrackerReply::CacheLocations& msg){
            return msg.locs;
        });
        unordered_map<size_t, vector<vector<host_t>>> res;
        for (auto& p : xlocs) {
            res[p.first].resize(p.second.size());
            for (size_t i = 0; i < p.second.size(); ++i) {
                res[p.first][i] = vector<host_t>{
                    std::make_move_iterator(p.second[i].begin()),
                    std::make_move_iterator(p.second[i].end())};
            }
        }
        return res;
    }

    auto getCacheStatus() {
        auto reply = client(CacheTrackerMessage{
                .vmember = CacheTrackerMessage::GetCacheStatus{}
        });
        return match(reply.get(), [](const CacheTrackerReply::CacheStatus& msg){
            return msg.status;
        });
    }

    void registerRDD(int rddId, int numPartitions) {
        {
            shared_lock lk{registeredRddIds_lck};
            if (registeredRddIds.count(rddId)) {
                return;
            }
        }
        unique_lock lk{registeredRddIds_lck};
        registeredRddIds.insert(rddId);
        client(CacheTrackerMessage{
            .vmember = CacheTrackerMessage::RegisterRDD {
                .rddId = static_cast<int>(rddId),
                .numPartitions = static_cast<int>(numPartitions)
            }
        });
    }

    // the rdd should be referenced, not holding owner
    template <typename T>
    auto getOrCompute(RDD<T>* rdd, unique_ptr<Split> split) -> unique_ptr<Iterator<T>> {
        auto key = make_pair(rdd->id(), split->index());
        auto val = cache.get(key.first, key.second);
        if (val.is_initialized()) {
            // need a owning iterator
            vector<T> vec;
            deserialize(vec, val->v.data(), val->v.size());
            return make_unique<OwnIterator<T>>(move(vec));
        }
        unique_lock lk{loading_lck};
        loading_cv.wait(lk, [this, key = key](){
            return !loading.count(key);
        });
        loading.insert(key);
        auto after_val = cache.get(key.first, key.second);
        if (after_val.is_initialized()) {
            vector<T> vec;
            deserialize(vec, after_val->v.data(), after_val->v.size());
            return make_unique<OwnIterator<T>>(move(vec));
        }
        auto iter = dynamic_unique_ptr_cast<Iterator<T>>(rdd->compute(move(split)));
        auto v = iter->collect();
        vector<char> bytes;
        serialize(v, bytes);
        cache.put(key.first, key.second, move(bytes));
        loading.erase(key);
        loading_cv.notify_all();
        return make_unique<OwnIterator<T>>(move(v));
    }
};









#endif //SPARKPP_CACHE_TRACKER_HPP
