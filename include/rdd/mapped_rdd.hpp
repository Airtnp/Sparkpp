//
// Created by xiaol on 11/17/2019.
//

#ifndef SPARKPP_MAPPED_RDD_HPP
#define SPARKPP_MAPPED_RDD_HPP

#include "common.hpp"
#include "rdd/rdd.hpp"
#include "serialize_wrapper.hpp"
#include "serialize_capnp.hpp"

template <typename T, typename U, typename F>
struct MappedRDD : RDD<U> {
    RDD<T>* prev;
    F func;
    OneToOneDependency dep;
    Dependency* depP;
    MappedRDD(RDD<T>* p_, F f_) : RDD<U>{p_->sc}, prev{p_}, func{move(f_)}, dep{p_}, depP{&dep} {}
    size_t numOfSplits() override {
        return prev->numOfSplits();
    }
    unique_ptr<Split> split(size_t partitionId) override {
        return prev->split(partitionId);
    }
    span<Dependency*> dependencies() override {
        return make_span<Dependency*>(&depP, 1);
    };

    unique_ptr<Iterator<U>> compute(unique_ptr<Split> split) {
        return make_unique<MapIterator<T, U, F>>(
            move(prev->iterator(move(split))),
            func
        );
    };

    void serialize_dyn(vector<char>& bytes) const {
        size_t oldSize = bytes.size();
        bytes.resize(oldSize + sizeof(MappedRDD));
        memcpy(bytes.data() + oldSize, reinterpret_cast<const char*>(this), sizeof(MappedRDD));
        prev->serialize_dyn(bytes);
    };

    void deserialize_dyn(const char*& bytes, size_t& size) {
        bytes += sizeof(MappedRDD);
        size -= sizeof(MappedRDD);
        prev = reinterpret_cast<RDD<T>*>(const_cast<char*>(bytes));
        prev->deserialize_dyn(bytes, size);
    };
};

template <typename T, typename U, typename F>
struct FlatMappedRDD : RDD<U> {
    RDD<T>* prev;
    F func;
    OneToOneDependency dep;
    Dependency* depP;
    FlatMappedRDD(RDD<T>* p_, F f_) : RDD<U>{p_->sc}, prev{p_}, func{move(f_)}, dep{p_}, depP{&dep} {}
    size_t numOfSplits() override {
        return prev->numOfSplits();
    }
    unique_ptr<Split> split(size_t partitionId) override {
        return prev->split(partitionId);
    }
    span<Dependency*> dependencies() override {
        return make_span<Dependency*>(&depP, 1);
    };

    unique_ptr<Iterator<U>> compute(unique_ptr<Split> split) {
        return make_unique<FlatMapIterator<T, U, F>>(
                move(prev->iterator(move(split))),
                func
        );
    };

    void serialize_dyn(vector<char>& bytes) const {
        size_t oldSize = bytes.size();
        bytes.resize(oldSize + sizeof(FlatMappedRDD));
        memcpy(bytes.data() + oldSize, reinterpret_cast<const char*>(this), sizeof(FlatMappedRDD));
        prev->serialize_dyn(bytes);
    };

    void deserialize_dyn(const char*& bytes, size_t& size) {
        bytes += sizeof(FlatMappedRDD);
        size -= sizeof(FlatMappedRDD);
        prev = reinterpret_cast<RDD<T>*>(const_cast<char*>(bytes));
        prev->deserialize_dyn(bytes, size);
    };
};

template <typename T, typename F>
struct FilterRDD : RDD<T> {
    RDD<T>* prev;
    F func;
    OneToOneDependency dep;
    Dependency* depP;
    FilterRDD(RDD<T>* p_, F f_) : RDD<T>{p_->sc}, prev{p_}, func{move(f_)}, dep{p_}, depP{&dep} {}
    size_t numOfSplits() override {
        return prev->numOfSplits();
    }
    unique_ptr<Split> split(size_t partitionId) override {
        return prev->split(partitionId);
    }
    span<Dependency*> dependencies() override {
        return make_span<Dependency*>(&depP, 1);
    };

    unique_ptr<Iterator<T>> compute(unique_ptr<Split> split) {
        return make_unique<FilterIterator<T, F>>(
                move(prev->iterator(move(split))),
                func
        );
    };

    void serialize_dyn(vector<char>& bytes) const {
        size_t oldSize = bytes.size();
        bytes.resize(oldSize + sizeof(FilterRDD));
        memcpy(bytes.data() + oldSize, reinterpret_cast<const char*>(this), sizeof(FilterRDD));
        prev->serialize_dyn(bytes);
    };

    void deserialize_dyn(const char*& bytes, size_t& size) {
        bytes += sizeof(FilterRDD);
        size -= sizeof(FilterRDD);
        prev = reinterpret_cast<RDD<T>*>(const_cast<char*>(bytes));
        prev->deserialize_dyn(bytes, size);
    };
};



#endif //SPARKPP_MAPPED_RDD_HPP
