//
// Created by xiaol on 11/19/2019.
//

#ifndef SPARKPP_PAIR_RDD_HPP
#define SPARKPP_PAIR_RDD_HPP

#include "common.hpp"
#include "rdd/rdd.hpp"




template <typename K, typename V, typename F>
struct MappedValueRDD;

template <typename K, typename V, typename F>
struct PairRDD : RDD<pair<K, V>> {
    using pair_t = pair<K, V>;
    RDD<pair_t>* prev;
    F func;
    OneToOneDependency dep;
    Dependency* depP;
    PairRDD(RDD<pair_t>* p_, F f_)
        : RDD<pair_t>{p_->sc}, prev{p_}, func{move(f_)}, dep{p_}, depP{&dep} {}
    size_t numOfSplits() override {
        return prev->numOfSplits();
    }
    unique_ptr<Split> split(size_t partitionId) override {
        return prev->split(partitionId);
    }
    span<Dependency*> dependencies() override {
        return make_span<Dependency*>(&depP, 1);
    };

    unique_ptr<IterBase> compute(unique_ptr<Split> split) {
        return make_unique<MapIterator<pair_t, F>>(
                dynamic_unique_ptr_cast<Iterator<pair_t>>(prev->iterator(move(split))),
                func
        );
    };

    void serialize_dyn(vector<char>& bytes) const {
        size_t oldSize = bytes.size();
        bytes.resize(oldSize + sizeof(PairRDD));
        memcpy(bytes.data() + oldSize, reinterpret_cast<const char*>(this), sizeof(PairRDD));
        prev->serialize_dyn(bytes);
    };

    void deserialize_dyn(const char*& bytes, size_t& size) {
        bytes += sizeof(PairRDD);
        size -= sizeof(PairRDD);
        prev = reinterpret_cast<RDD<pair_t>*>(const_cast<char*>(bytes));
        prev->deserialize_dyn(bytes, size);
    };

    template <typename G, typename U = typename function_traits<G>::result_type>
    auto mapValues(G f) -> MappedValueRDD<K, U, G> {
        return MappedValueRDD{this, move(f)};
    }
};




template <typename K, typename V, typename F>
struct MappedValuesRDD : RDD<pair<K, V>> {
    using pair_t = pair<K, V>;
    RDD<pair<K, V>>* prev;
    F func;
    OneToOneDependency dep;
    Dependency* depP;
    MappedValuesRDD(RDD<pair<K, V>>* p, F f)
        : RDD<pair<K, V>>{p->sc}, prev{p}, func{move(f)}, dep{p}, depP{dep} {}
    size_t numOfSplits() override {
        return prev->numOfSplits();
    }
    unique_ptr<Split> split(size_t partitionId) override {
        return prev->split(partitionId);
    }
    span<Dependency*> dependencies() override {
        return make_span<Dependency*>(&depP, 1);
    };
    unique_ptr<IterBase> compute(unique_ptr<Split> split) {
        return make_unique<MapValueIterator<K, V, F>>(
                dynamic_unique_ptr_cast<Iterator<pair<K, V>>>(prev->iterator(move(split))),
                func
        );
    };

    void serialize_dyn(vector<char>& bytes) const {
        size_t oldSize = bytes.size();
        bytes.resize(oldSize + sizeof(MappedValuesRDD));
        memcpy(bytes.data() + oldSize, reinterpret_cast<const char*>(this), sizeof(MappedValuesRDD));
        prev->serialize_dyn(bytes);
    };

    void deserialize_dyn(const char*& bytes, size_t& size) {
        bytes += sizeof(MappedValuesRDD);
        size -= sizeof(MappedValuesRDD);
        prev = reinterpret_cast<RDD<pair<K, V>>*>(const_cast<char*>(bytes));
        prev->deserialize_dyn(bytes, size);
    };
};



#endif //SPARKPP_PAIR_RDD_HPP
