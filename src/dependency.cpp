//
// Created by xiaol on 11/16/2019.
//

#include "common.hpp"
#include "dependency.hpp"
#include "spark_env.hpp"
#include <fstream>
#include <boost/archive/binary_oarchive.hpp>


template<typename K, typename V, typename C>
string ShuffleDependency<K, V, C>::runShuffle(RDDBase* rdd, unique_ptr<Split> split, size_t partition) {
    size_t numOutputSplits = partitioner->numPartitions();
    vector<unordered_map<K, C>> buckets{numOutputSplits};
    auto iter = rdd->compute(move(split));
    while (true) {
        auto s = iter->next();
        if (!s.is_initialized()) {
            break;
        }
        auto p = static_cast<pair<K, V>>(s);
        auto bucketId = partitioner->getPartition(p.first);
        auto& bucket = buckets[bucketId];
        if (!bucket.count(p.first)) {
            bucket[p.first] = aggregator->createCombiner(move(p.second));
        } else {
            bucket[p.first] = aggregator->mergeValue(move(bucket[p.first]), move(p.second));
        }
    }
    for (size_t i = 0; i < numOutputSplits; ++i) {
        string file_path = fmt::format("/tmp/sparkpp/shuffle/{}.{}.{}", shuffleId, partition, i);
        std::ofstream ofs{file_path, std::ofstream::binary};
        boost::archive::binary_oarchive ar{ofs};
        vector<pair<K, C>> v{
            std::make_move_iterator(buckets[i].begin()),
            std::make_move_iterator(buckets[i].end())
        };
        ar << v;
    }
    return env.shuffleManager->serverUri;
}

template<typename K, typename V, typename C>
void ShuffleDependency<K, V, C>::serialize_dyn(vector<char> &bytes) const {
    size_t oldSize = bytes.size();
    bytes.resize(oldSize + sizeof(ShuffleDependency));
    memcpy(bytes.data() + oldSize, reinterpret_cast<const char*>(this), sizeof(ShuffleDependency));
    partitioner->serialize_dyn(bytes);
    aggregator->serialize_dyn(bytes);
}


template<typename K, typename V, typename C>
void ShuffleDependency<K, V, C>::deserialize_dyn(const char*& bytes, size_t& size) {
    bytes += sizeof(ShuffleDependency);
    size -= sizeof(ShuffleDependency);
    partitioner = reinterpret_cast<Partitioner*>(const_cast<char*>(bytes));
    partitioner->deserialize_dyn(bytes, size);
    aggregator = reinterpret_cast<AggregatorBase*>(const_cast<char*>(bytes));
    aggregator->deserialize_dyn(bytes, size);
}

ShuffleDependencyBase* dep_from_reader(::capnp::Data::Reader reader) {
    const char* bytes = reinterpret_cast<const char*>(reader.asBytes().begin());
    size_t size = reader.size();
    auto base = reinterpret_cast<ShuffleDependencyBase*>(
            const_cast<unsigned char*>(reader.asBytes().begin()));
    base->deserialize_dyn(bytes, size);
    return base;
}

