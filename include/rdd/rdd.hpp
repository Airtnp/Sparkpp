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
    virtual const vector<Partition>& getPartitions() = 0;
    virtual void serialize_dyn(vector<char>&) const = 0;
    virtual void deserialize_dyn(const char*&, size_t&) = 0;
    virtual size_t numOfSplits() = 0;
    virtual vector<unique_ptr<Split>> splits() = 0;
    virtual span<Dependency*> dependencies() = 0;
};

struct MockRDD : RDDBase {
    struct MockRDDData {
        vector<int> v;
    };
    unique_ptr<MockRDDData> pimpl;
    vector<Partition> t;
    unique_ptr<IterBase> compute(unique_ptr<Split> split) override {
        return make_unique<Iterator<int>>(pimpl->v.data(), pimpl->v.data() + pimpl->v.size());
    }
    // useless
    const vector<Partition>& getPartitions() override {
        return t;
    }
    void serialize_dyn(vector<char>& bytes) const override {
        bytes.resize(sizeof(MockRDD));
        memcpy(bytes.data(), reinterpret_cast<const char*>(this), sizeof(MockRDD));

        boost::iostreams::back_insert_device<vector<char>> sink{bytes};
        boost::iostreams::stream<boost::iostreams::back_insert_device<vector<char>>> s{sink};
        boost::archive::binary_oarchive oa{s};
        oa << pimpl->v;
        s.flush();
    }
    void deserialize_dyn(const char*& bytes, size_t& size) override {
        bytes += sizeof(MockRDD);
        size -= sizeof(MockRDD);

        boost::iostreams::basic_array_source<char> device{bytes, size};
        boost::iostreams::stream<boost::iostreams::basic_array_source<char>> s{device};
        boost::archive::binary_iarchive ia{s};
        pimpl = make_unique<MockRDDData>();
        ia >> pimpl->v;
    }
};

RDDBase* rdd_from_reader(::capnp::Data::Reader reader);

// Transformations

template <typename T, typename U, typename F>
struct MapPartitionRDD;

template <typename T, typename U, typename F>
struct FlatMapPartitionRDD;



template <typename T>
struct RDD : RDDBase {
    SparkContext& sc;
    vector<Dependency*> deps;
    int m_id;

    RDD(SparkContext& sc_);

    unique_ptr<Iterator<T>> compute(unique_ptr<Split> split) override = 0;

    span<Dependency*> dependencies() override {
        return make_span(deps);
    }

    virtual optional<Partitioner*> getPartitioner() {
        return {};
    }

    // TODO: persist, cache, unpersist, storageLevel

    // Transformations, Lazy

    template <Invocable<T> F, typename U = std::invoke_result_t<F, T>>
    auto map(F f) -> MapPartitionRDD<T, U, F> {
        return MapPartitionRDD{*this, f};
    }

    template <Invocable<T> F, typename SU = std::invoke_result_t<F, T>,
            typename U = typename SU::value_type>
    auto flat_map(F f) -> MapPartitionRDD<T, U, F> {
        return FlatMapPartitionRDD{*this, f};
    }

    // Actions, Eager

    // TODO: complete actions
    template <Fn<T, T, T> F>
    auto reduce(F f) -> optional<T> {
        return {};
    }
};


#endif //SPARKPP_RDD_HPP
