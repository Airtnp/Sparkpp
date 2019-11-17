//
// Created by xiaol on 11/13/2019.
//

#ifndef SPARKPP_CACHE_HPP
#define SPARKPP_CACHE_HPP

#include "common.hpp"
#include "serialize_wrapper.hpp"

struct KeySpace;

// HACK: Currently unbounded
struct BoundedMemoryCache {
    atomic<size_t> nextKeySpaceId = 0;
    size_t maxBytes = 2000;
    mutex lock;
    size_t currentBytes = 0;
    // keyspaceId, rddId
    using datasetId_t = pair<size_t, size_t>;
    // datasetId, partitionId
    using key_t = pair<datasetId_t, size_t>;
    using value_t = Storage;
    unordered_map<key_t, Storage, pair_hash> map;

    KeySpace newKeySpace();
    optional<Storage> get(key_t key) {
        lock_guard lk{lock};
        if (map.find(key) != map.end()) {
            return {map[key]};
        } else {
            return {};
        }
    }
    optional<size_t> put(key_t key, Storage&& value) {
        // size in MBs
        size_t size = (value.v.size() + 2) / 128 / 1024;
        if (size > maxBytes) {
            return {};
        }
        lock_guard lk{lock};
        map.emplace(move(key), move(value));
        return {size};
    }
};

struct KeySpace {
    BoundedMemoryCache& cache;
    size_t keySpaceId;
    optional<Storage> get(size_t datasetId, size_t partition) {
        return cache.get(make_pair(make_pair(keySpaceId, datasetId), partition));
    }
    optional<size_t> put(size_t datasetId, size_t partition, Storage&& value) {
        return cache.put(make_pair(make_pair(keySpaceId, datasetId), partition), move(value));
    }
    size_t getCapacity() {
        return cache.maxBytes;
    }
};


#endif //SPARKPP_CACHE_HPP
