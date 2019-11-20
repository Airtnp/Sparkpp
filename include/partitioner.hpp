//
// Created by xiaol on 11/7/2019.
//

#ifndef SPARKPP_PARTITIONER_HPP
#define SPARKPP_PARTITIONER_HPP

#include "common.hpp"

struct Partitioner {
    virtual size_t numPartitions() const = 0;
    virtual size_t getPartition(const any&) const = 0;
    virtual void serialize_dyn(vector<char>&) const = 0;
    virtual void deserialize_dyn(const char*&, size_t&) = 0;
};

template <typename K>
struct HashPartitioner : Partitioner {
    size_t partitions;
    HashPartitioner(size_t p_) : partitions{p_} {}
    size_t numPartitions() const {
        return partitions;
    }
    size_t getPartition(const any& key) const {
        auto v = std::any_cast<K>(key);
        return std::hash<K>{}(v) % partitions;
    }
    void serialize_dyn(vector<char>& bytes) const override {
        size_t oldSize = bytes.size();
        bytes.resize(oldSize + sizeof(HashPartitioner));
        memcpy(bytes.data() + oldSize, reinterpret_cast<const char*>(this), sizeof(HashPartitioner));
    }
    void deserialize_dyn(const char*& bytes, size_t& size) override {
        // plain_copy
        bytes += sizeof(HashPartitioner);
        size -= sizeof(HashPartitioner);
    }
};



#endif //SPARKPP_PARTITIONER_HPP
