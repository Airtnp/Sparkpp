//
// Created by xiaol on 11/19/2019.
//

#ifndef SPARKPP_PAIR_RDD_HPP
#define SPARKPP_PAIR_RDD_HPP

#include "common.hpp"
#include "rdd/rdd.hpp"


template <typename K, typename V, typename U, typename F>
struct MappedValueRDD;

template <typename K, typename V, typename C>
struct ShuffleRDD;


template <typename K, typename V>
struct PairRDD : RDD<pair<K, V>> {
    PairRDD(SparkContext& sc) : RDD<pair<K, V>>{sc} {}

    template <typename G, typename U = typename function_traits<G>::result_type>
    auto mapValues(G f) -> MappedValueRDD<K, V, U, G> {
        return MappedValueRDD{this, move(f)};
    }

    template <typename C>
    ShuffleRDD<K, V, C> combineByKey(unique_ptr<AggregatorBase> aggregator, unique_ptr<Partitioner> partitioner) {
        return ShuffleRDD<K, V, C>{this, move(aggregator), move(partitioner)};
    }

    // Invocable<V, V> F
    template <typename F>
    auto reduceByKey(F func, size_t numSplits) {
        auto part = make_unique<HashPartitioner<K>>(numSplits);
        auto createCombiner = +[](V v) { return v; };
        auto aggr = make_unique<Aggregator>(createCombiner, +func, +func);
        return combineByKey<V>(move(aggr), move(part));
    }

    auto groupByKey(size_t numSplits) {
        auto part = make_unique<HashPartitioner<K>>(numSplits);
        // NOTE: no bitwise copy performed on these results (Shuffle)
        auto createCombiner = +[](V v) { return vector<V>{move(v)}; };
        auto mergeValue = +[](vector<V> vv, V v) {
            vv.push_back(move(v));
            return vv;
        };
        auto mergeCombiners = +[](vector<V> v1, vector<V> v2) {
            v1.insert(v1.end(),
                    std::make_move_iterator(v2.begin()),
                    std::make_move_iterator(v2.end()));
            return v1;
        };
        auto aggr = make_unique<Aggregator<K, V, vector<V>>>(
                createCombiner, mergeValue, mergeCombiners);
        return combineByKey<vector<V>>(move(aggr), move(part));
    }
};




template <typename T, typename K, typename V, typename F>
struct MapPairRDD : PairRDD<K, V> {
    using pair_t = pair<K, V>;
    RDD<T>* prev;
    F func;
    OneToOneDependency dep;
    Dependency* depP;
    MapPairRDD(RDD<T>* p_, F f_)
        : PairRDD<K, V>{p_->sc}, prev{p_}, func{move(f_)}, dep{p_}, depP{&dep} {}
    size_t numOfSplits() override {
        return prev->numOfSplits();
    }
    unique_ptr<Split> split(size_t partitionId) override {
        return prev->split(partitionId);
    }
    span<Dependency*> dependencies() override {
        return make_span<Dependency*>(&depP, 1);
    };

    unique_ptr<Iterator<pair<K, V>>> compute(unique_ptr<Split> split) {
        return make_unique<MapIterator<T, pair_t, F>>(
                dynamic_unique_ptr_cast<Iterator<T>>(prev->iterator(move(split))),
                func
        );
    };

    void serialize_dyn(vector<char>& bytes) const {
        size_t oldSize = bytes.size();
        bytes.resize(oldSize + sizeof(MapPairRDD));
        memcpy(bytes.data() + oldSize, reinterpret_cast<const char*>(this), sizeof(MapPairRDD));
        prev->serialize_dyn(bytes);
    };

    void deserialize_dyn(const char*& bytes, size_t& size) {
        bytes += sizeof(MapPairRDD);
        size -= sizeof(MapPairRDD);
        prev = reinterpret_cast<RDD<T>*>(const_cast<char*>(bytes));
        prev->deserialize_dyn(bytes, size);
    };

};




template <typename K, typename V, typename U, typename F>
struct MappedValuesRDD : PairRDD<K, U> {
    using pair_t = pair<K, V>;
    RDD<pair<K, V>>* prev;
    F func;
    OneToOneDependency dep;
    Dependency* depP;
    MappedValuesRDD(RDD<pair<K, V>>* p, F f)
        : PairRDD<K, V>{p->sc}, prev{p}, func{move(f)}, dep{p}, depP{dep} {}
    size_t numOfSplits() override {
        return prev->numOfSplits();
    }
    unique_ptr<Split> split(size_t partitionId) override {
        return prev->split(partitionId);
    }
    span<Dependency*> dependencies() override {
        return make_span<Dependency*>(&depP, 1);
    };
    unique_ptr<Iterator<pair<K, U>>> compute(unique_ptr<Split> split) {
        return make_unique<MapValueIterator<K, V, U, F>>(
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

template <typename K, typename V, typename C>
struct ShuffleRDD : PairRDD<K, C> {
    RDD<pair<K, V>>* parent;
    size_t shuffleId;
    unique_ptr<AggregatorBase> aggregator;
    unique_ptr<Partitioner> partitioner;
    ShuffleDependency<K, V, C> dep;
    Dependency* depP;
    ShuffleRDD(RDD<pair<K, V>>* p, unique_ptr<AggregatorBase> a, unique_ptr<Partitioner> pt)
        : PairRDD<K, C>{p->sc}, parent{p}, shuffleId{p->sc.newShuffleId()},
        aggregator{move(a)}, partitioner{move(pt)},
        dep{shuffleId, p, partitioner.get(), aggregator.get()}, depP{&dep} {}
    size_t numOfSplits() override {
        return partitioner->numPartitions();
    }
    unique_ptr<Split> split(size_t partitionId) override {
        return make_unique<Split>(partitionId);
    }
    span<Dependency*> dependencies() override {
        return make_span<Dependency*>(&depP, 1);
    };
    unique_ptr<Iterator<pair<K, C>>> compute(unique_ptr<Split> split);

    void serialize_dyn(vector<char>& bytes) const {
        size_t oldSize = bytes.size();
        bytes.resize(oldSize + sizeof(ShuffleRDD));
        memcpy(bytes.data() + oldSize, reinterpret_cast<const char*>(this), sizeof(ShuffleRDD));
        parent->serialize_dyn(bytes);
        aggregator->serialize_dyn(bytes);
        // covered in Dependency
        // partitioner->serialize_dyn(bytes);
        // dep.serialize_dyn(bytes);
    };

    void deserialize_dyn(const char*& bytes, size_t& size) {
        bytes += sizeof(ShuffleRDD);
        size -= sizeof(ShuffleRDD);
        parent = reinterpret_cast<RDD<pair<K, V>>*>(const_cast<char*>(bytes));
        parent->deserialize_dyn(bytes, size);
        // plain type
        aggregator.release();
        auto aggr = reinterpret_cast<AggregatorBase*>(const_cast<char*>(bytes));
        aggr->deserialize_dyn(bytes, size);
        aggregator.reset(aggr);
    };
};



#endif //SPARKPP_PAIR_RDD_HPP
