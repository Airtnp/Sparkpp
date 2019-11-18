//
// Created by xiaol on 11/17/2019.
//

#ifndef SPARKPP_PARALLEL_COLLECTION_HPP
#define SPARKPP_PARALLEL_COLLECTION_HPP

#include "common.hpp"
#include "rdd/rdd.hpp"
#include "split.hpp"
#include "serialize_capnp.hpp"
#include "serialize_wrapper.hpp"

template <typename T>
struct ParallelCollectionSplit : Split {
    size_t slice;
    size_t rddId;
    vector<T> values;
    ParallelCollectionSplit(size_t s, size_t rid, vector<T> v) : Split{s}, slice{s}, rddId{rid}, values{move(v)} {}
};

template <typename T>
struct ParallelCollection : RDD<T> {
    // Use pimpl to avoid extra undefined destructor
    struct PCVal {
        vector<T> data;
        vector<vector<T>> splits;
        PCVal() = default;
        PCVal(vector<T> d_, vector<vector<T>> s_) : data{move(d_)}, splits{move(s_)} {}
        SN_BOOST_SERIALIZE_MEMBERS_IN(data, splits);
    };
    unique_ptr<PCVal> pimpl;
    size_t numSlices;
    ParallelCollection(SparkContext& sc, vector<T> data, size_t n) : RDD<T>{sc}, numSlices{n} {
        auto splits = slice(data, n);
        pimpl = make_unique<PCVal>(move(data), move(splits));
    }
    vector<vector<T>> slice(const vector<T>& seq, size_t n) {
        vector<vector<T>> slice(n);
        size_t jmp = seq.size() / n;
        for (size_t i = 0; i < n; ++i) {
            slice[i].assign(seq.begin() + i * jmp, seq.begin() + (i + 1) * jmp);
        }
        size_t start = n * jmp;
        for (size_t i = start; i < seq.size(); ++i) {
            slice[i - start].push_back(seq[i]);
        }
        return slice;
    }
    size_t numOfSplits() override {
        return numSlices;
    }
    unique_ptr<Split> split(size_t partitionId) override {
        return make_unique<ParallelCollectionSplit<T>>(
                partitionId, this->m_id, pimpl->splits[partitionId]);
    }
    unique_ptr<IterBase> compute(unique_ptr<Split> split) {
        auto pcSplit = dynamic_unique_ptr_cast<ParallelCollectionSplit<T>>(move(split));
        return make_unique<OwnIterator<T>>(move(pcSplit->values));
    };

    void serialize_dyn(vector<char>& bytes) const {
        size_t oldSize = bytes.size();
        bytes.resize(oldSize + sizeof(ParallelCollection));
        memcpy(bytes.data() + oldSize, reinterpret_cast<const char*>(this), sizeof(ParallelCollection));
        SerialGuard gd{bytes};
        gd << *pimpl;
    };

    void deserialize_dyn(const char*& bytes, size_t& size) {
        bytes += sizeof(ParallelCollection);
        size -= sizeof(ParallelCollection);
        pimpl = make_unique<PCVal>();
        DeserialGuard gd{bytes, size};
        gd >> *pimpl;
    };
};


#endif //SPARKPP_PARALLEL_COLLECTION_HPP
