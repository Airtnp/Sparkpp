//
// Created by xiaol on 11/17/2019.
//

#ifndef SPARKPP_MAPPED_RDD_HPP
#define SPARKPP_MAPPED_RDD_HPP

#include "common.hpp"
#include "rdd/rdd.hpp"
#include "serialize_wrapper.hpp"
#include "serialize_capnp.hpp"

template <typename T, typename F>
struct MappedRDD : RDD<T> {
    RDD<T>* prev;
    F func;
    OneToOneDependency dep;
    MappedRDD(RDD<T>* p_, F f_) : RDD<T>{prev->sc}, prev{p_}, f{move(f_)}, dep{p_} {}
    size_t numOfSplits() override {
        return prev->numOfSplits();
    }
    unique_ptr<Split> split(size_t partitionId) override {
        return prev->split(partitionId);
    }
    span<Dependency*> dependencies() override {
        span<Dependency*> span {
            .ptr = &dep,
            .len = 1
        };
    };

    unique_ptr<IterBase> compute(unique_ptr<Split> split) {
        return make_unique<MapIterator>(
            prev->iterator(move(split)),
            func
        );
    };

    void serialize_dyn(vector<char>& bytes) const {
        size_t oldSize = bytes.size();
        bytes.resize(oldSize + sizeof(MappedRDD));
        memcpy(bytes.data() + oldSize, reinterpret_cast<const char*>(this), sizeof(MappedRDD));
        prev->serialize_dyn(bytes);
    };

    void deserialize_dyn(const char*&, size_t&) {
        bytes += sizeof(MappedRDD);
        size -= sizeof(MappedRDD);
        prev = reinterpret_cast<RDD<T>*>(const_cast<char*>(bytes));
        prev->deserialize_dyn(bytes, size);
    };

};


#endif //SPARKPP_MAPPED_RDD_HPP
