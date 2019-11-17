//
// Created by xiaol on 11/7/2019.
//

#ifndef SPARKPP_AGGREGATOR_HPP
#define SPARKPP_AGGREGATOR_HPP

#include "common.hpp"
#include "serialize_wrapper.hpp"

struct AggregatorBase {
    virtual any createCombiner(any) = 0;
    virtual any mergeValue(any, any) = 0;
    virtual any mergeCombiners(any, any) = 0;
    virtual void serialize_dyn(vector<char>& bytes) const = 0;
    virtual void deserialize_dyn(const char*&, size_t&) = 0;
};


template <typename K, typename V, typename C>
struct Aggregator : AggregatorBase {
    /// force function pointer
    using createCombiner_t = C(*)(V);
    using mergeValue_t = C(*)(C, V);
    using mergeCombiners_t = C(*)(C, C);
    /// V -> C
    createCombiner_t f_createCombiner;
    /// C, V -> C
    mergeValue_t f_mergeValue;
    /// C, C -> C
    mergeCombiners_t f_mergeCombiners;

    // FIXME: how to fix copy overhead here?
    any createCombiner(any x) override {
        return f_createCombiner(move(x));
    }
    any mergeValue(any c, any v) override {
        return f_mergeValue(move(c), move(v));
    }
    any mergeCombiners(any c1, any c2) override {
        return f_mergeCombiners(move(c1), move(c2));
    }

    void serialize_dyn(vector<char>& bytes) const override {
        size_t oldSize = bytes.size();
        bytes.resize(oldSize + sizeof(Aggregator));
        memcpy(bytes.data() + oldSize, reinterpret_cast<const char*>(this), sizeof(Aggregator));
    }
    void deserialize_dyn(const char*& bytes, size_t& size) override {
        // plain_copy
        bytes += sizeof(Aggregator);
        size -= sizeof(Aggregator);
    }
};

#endif //SPARKPP_AGGREGATOR_HPP
