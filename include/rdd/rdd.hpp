//
// Created by xiaol on 11/7/2019.
//

#ifndef SPARKPP_RDD_HPP
#define SPARKPP_RDD_HPP

#include "common.hpp"
#include "split.hpp"
#include "dependency.hpp"
#include "partition.hpp"
#include "partitioner.hpp"
#include "serialize_wrapper.hpp"
#include <capnp/message.h>
#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/serialization/vector.hpp>

struct SparkContext;

// FIXME: currently all data in a RDD is passed (like Spark-0.5 does)
// FIXME: do we really need boost::serializaton + capnproto? (replace everything with boost only)
/// For non-trivial objects, we store them into pimpl mode + boost::serialization.
/// Since C++ has no support for `mem::forget`, \
/// we need to place objects like `std::vector` to pimpl to avoid undefined destructors.
/// Fortunately, the size of pimpl objects are usually small, since they don't store value inside them.
/// For trivial objects, just directly copy based on class bytes.
/// Only derived classes virtual methods know how to change offsets; (CRTP won't work)
struct RDDBase {
    virtual size_t id() = 0;
    virtual unique_ptr<IterBase> compute(unique_ptr<Split> split) = 0;
    virtual unique_ptr<IterBase> iterator(unique_ptr<Split> split) = 0;
    // virtual vector<Partition> getPartitions() = 0;
    virtual void serialize_dyn(vector<char>&) const = 0;
    virtual void deserialize_dyn(const char*&, size_t&) = 0;
    virtual size_t numOfSplits() = 0;
    // virtual vector<unique_ptr<Split>> splits() = 0;
    virtual unique_ptr<Split> split(size_t partitionId) = 0;
    virtual span<Dependency*> dependencies() = 0;
};

RDDBase* rdd_from_reader(::capnp::Data::Reader reader);

// Transformations

template <typename T, typename F>
struct MappedRDD;

template <typename K, typename V, typename F>
struct PairRDD;

template <typename T>
struct RDD : RDDBase {
    SparkContext& sc;
    vector<Dependency*> deps;
    size_t m_id;
    bool shouldCache = false;

    RDD(SparkContext& sc_);
    size_t id() override {
        return m_id;
    }
    RDD& cache() {
        shouldCache = true;
        return *this;
    }

    unique_ptr<IterBase> compute(unique_ptr<Split> split) override = 0;
    unique_ptr<IterBase> iterator(unique_ptr<Split> split) override;

    span<Dependency*> dependencies() override {
        return make_span(deps);
    }

    // TODO: persist, cache, unpersist, storageLevel

    // Transformations, Lazy
    // HACK: this requires lifetime to continue. better use `enable_shared_from_this` + `shared_from_this`
    // But this will cause extra overhead in type serialization & type system
    // Currently every RDD<T> should live long through the program.

    // Invocable<T> F
    template <typename F>
    auto map(F f) -> MappedRDD<T, F> {
        return MappedRDD{this, move(f)};
    }

    template <typename F,
            typename R = typename function_traits<F>::result_type,
            typename K = typename R::first_type, typename V = typename R::second_type>
    auto mapPair(F f) -> PairRDD<K, V, F> {
        return PairRDD{this, move(f)};
    }

    // Actions, Eager

    template <typename F>
    T reduce(F&& f);

    auto collect();
};

#include "rdd/mapped_rdd.hpp"
#include "rdd/pair_rdd.hpp"


#endif //SPARKPP_RDD_HPP
