//
// Created by xiaol on 11/7/2019.
//

#ifndef SPARKPP_DEPENDENCY_HPP
#define SPARKPP_DEPENDENCY_HPP

#include "common.hpp"
#include "serialize_wrapper.hpp"
#include "aggregator.hpp"
#include "partitioner.hpp"
#include "split.hpp"

struct RDDBase;
template <typename T>
struct RDD;

struct Dependency {
    virtual RDDBase* rdd() const = 0;
    virtual ~Dependency() = default;
};

struct NarrowDependency : Dependency {
    RDDBase* m_rdd;
    NarrowDependency(RDDBase* rdd_) : m_rdd(rdd_) {}
    RDDBase* rdd() const override {
        return m_rdd;
    }
    virtual vector<size_t> getParents(size_t partitionId) = 0;
};

struct ShuffleDependencyBase : Dependency {
    virtual string runShuffle(RDDBase* rdd, unique_ptr<Split> split, size_t partition) = 0;
    virtual size_t shuffle_id() const = 0;
    virtual void serialize_dyn(vector<char>&) const = 0;
    virtual void deserialize_dyn(const char*&, size_t&) = 0;
};

template <typename K, typename V, typename C>
struct ShuffleDependency : ShuffleDependencyBase {
    size_t shuffleId;
    RDD<pair<K, V>>* m_rdd;
    Partitioner* partitioner;
    AggregatorBase* aggregator;
    ShuffleDependency(size_t shuffleId_, RDD<pair<K, V>>* rdd_,
            Partitioner* partitioner_, AggregatorBase* aggregator_)
        : shuffleId{shuffleId_}, m_rdd{rdd_}, partitioner{partitioner_}, aggregator{aggregator_} {}
    RDDBase* rdd() const override {
        return m_rdd;
    }
    string runShuffle(RDDBase* rdd, unique_ptr<Split> split, size_t partition);
    size_t shuffle_id() const override {
        return shuffleId;
    }
    virtual void serialize_dyn(vector<char>& bytes) const;
    virtual void deserialize_dyn(const char*&, size_t&);
};

ShuffleDependencyBase* dep_from_reader(::capnp::Data::Reader reader);


struct OneToOneDependency : NarrowDependency {
    using NarrowDependency::NarrowDependency;
    vector<size_t> getParents(size_t partitionId) override {
        return { partitionId };
    }
};

struct RangeDependency : NarrowDependency {
    size_t inStart, outStart, length;
    RangeDependency(RDDBase* rdd_, size_t inStart_, size_t outStart_, size_t length_)
        : NarrowDependency{rdd_}, inStart{inStart_}, outStart{outStart_}, length{length_} {}
    Seq<size_t> getParents(size_t partitionId) override {
        if (partitionId >= outStart && partitionId < outStart + length) {
            return { partitionId - outStart + inStart };
        } else {
            return {};
        }
    }
};




#endif //SPARKPP_DEPENDENCY_HPP
