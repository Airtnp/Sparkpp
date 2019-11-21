//
// Created by xiaol on 11/7/2019.
//

#ifndef SPARKPP_AGGREGATOR_HPP
#define SPARKPP_AGGREGATOR_HPP

#include "common.hpp"
#include "serialize_wrapper.hpp"

struct AggregatorBase {
    virtual void* createCombiner() = 0;
    virtual void* mergeValue() = 0;
    virtual void* mergeCombiners() = 0;
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

    Aggregator(createCombiner_t cc, mergeValue_t mv, mergeCombiners_t mc)
        : f_createCombiner{cc}, f_mergeValue{mv}, f_mergeCombiners{mc} {}

    void* createCombiner() override {
        return (void*)f_createCombiner;
    }
    void* mergeValue() override {
        return (void*)f_mergeValue;
    }
    void* mergeCombiners() override {
        return (void*)f_mergeCombiners;
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
