//
// Created by xiaol on 11/5/2019.
//

#ifndef SPARKPP_SPARK_CONTEXT_HPP
#define SPARKPP_SPARK_CONTEXT_HPP

#include <utility>

#include "common.hpp"
#include "executor.hpp"
#include "spark_env.hpp"
#include "scheduler/dag_scheduler.hpp"
#include "rdd/parallel_collection.hpp"

enum class SparkContextType {
    Local,
    Distributed,
    NumOfSparkContextType
};

enum class SparkDistributeType {
    Master,
    Slave,
    NumOfSparkDistributeType
};

struct SparkConfig {
    SparkContextType mode;
    SparkDistributeType type;
    uint16_t port;
    vector<pair<string, uint16_t>> addr;
    SparkConfig(
            SparkContextType mode_,
            SparkDistributeType type_,
            uint16_t port_,
            vector<pair<string, uint16_t>> addr_
    ) : mode{mode_}, type{type_}, port{port_}, addr{move(addr_)} {}
};

struct SparkContext {
    std::atomic<size_t> nextRddId{0};
    std::atomic<size_t> nextShuffleId{0};
    SparkConfig config;
    vector<pair<string, uint16_t>> address;
    DAGScheduler scheduler;
    // scheduler

    size_t newRddId() {
        return nextRddId.fetch_add(1);
    }
    size_t newShuffleId() {
        return nextShuffleId.fetch_add(1);
    }

    SparkConfig getConfig(char** argv, addr_t masterAddr, vector<addr_t> slaveAddrs) {
        auto ty = string{argv[1]};
        if (ty == "master") {
            return SparkConfig{
                    SparkContextType::Distributed,
                    SparkDistributeType::Master,
                    masterAddr.second,
                    move(slaveAddrs)
            };
        } else {
            return SparkConfig{
                    SparkContextType::Distributed,
                    SparkDistributeType::Slave,
                    slaveAddrs[0].second,
                    {move(masterAddr)}
            };
        }
    }

    SparkContext(int argc, char** argv, addr_t masterAddr, vector<addr_t> slaveAddrs)
        : config{getConfig(argv, move(masterAddr), move(slaveAddrs))}, scheduler{config.addr} {
        switch (config.mode) {
            case SparkContextType::Distributed: {
                switch (config.type) {
                    case SparkDistributeType::Master: {
                        address = move(config.addr);
                        break;
                    }
                    case SparkDistributeType::Slave: {
                        auto executor = Executor{config.addr[0], config.port};
                        executor.run();
                        exit(0);
                    }
                    default: __builtin_unreachable();
                }
                break;
            }
            case SparkContextType::Local: {
                break;
            }
            default: __builtin_unreachable();
        }
    }

    template <typename T>
    ParallelCollection<T> parallelize(vector<T> data, size_t numSlices = 8) {
        return ParallelCollection<T>{*this, move(data), numSlices};
    }

    template <typename T, typename F, typename U = typename function_traits<F>::result_type>
    vector<U> runJob(RDD<T>* rdd, F&& f, const vector<size_t>& partitions) {
        return scheduler.runJob(forward<F>(f), rdd, partitions);
    }

    template <typename T, typename F, typename U = typename function_traits<F>::result_type>
    vector<U> runJob(RDD<T>* rdd, F&& f) {
        vector<size_t> partitions(rdd->numOfSplits());
        std::iota(partitions.begin(), partitions.end(), 0);
        return scheduler.runJob(forward<F>(f), rdd, partitions);
    }
};


// HACK: weird solution for circular import dependency
template<typename T>
template<typename F>
T RDD<T>::reduce(F&& f) {
    auto rf = [f = f](unique_ptr<Iterator<T>> iter) mutable {
        T acc{};
        while (iter->hasNext()) {
            acc = std::invoke(forward<F>(f), acc, static_cast<T>(iter->next().value()));
        }
        return acc;
    };
    auto result = sc.runJob(this, move(rf));
    T acc{};
    for (auto&& r : result) {
        acc = std::invoke(forward<F>(f), acc, move(r));
    }
    return acc;
}


template<typename F, typename T, typename U>
vector<U> DAGScheduler::runJob(F &&func, RDD<T> *finalRdd, const vector<size_t>& partitions) {
    // using U = typename function_traits<F>::result_type;
    size_t numFinished = 0;
    unordered_set<Stage*> waiting;
    unordered_set<Stage*> running;
    unordered_set<Stage*> failed;
    unordered_map<Stage*, unordered_set<size_t>> pendingTasks;
    [[maybe_unused]] size_t lastFetchFailureTime = 0;
    size_t runId = nextRunId.fetch_add(1);
    size_t numOutputParts = partitions.size();
    auto finalStage = newStage(finalRdd, {});
    vector<U> results(numOutputParts);
    vector<bool> finished(numOutputParts);
    FnWrapper funcWrapper{forward<F>(func)};

    updateCacheLocs();
    eventQueues[runId];

    submitStage(runId, finalRdd, &funcWrapper,
                pendingTasks, partitions, finished, finalStage.get(), waiting, running, finalStage.get());
    while (numFinished != numOutputParts) {
        CompletionEvent event;
        bool v = eventQueues[runId].wait_dequeue_timed(event, 500ms);

        if (v) {
            auto stage = idToStage[event.task->stage_id()];
            pendingTasks[stage.get()].erase(event.task->task_id());
            match(event.reason.get(),
                  [&](const TaskEndReason::Success&) {
                      if (auto rt = dynamic_cast<ResultTask*>(event.task.get())) {
                          U result;
                          deserialize(result, event.result.to_reader());
                          results[rt->outputId] = move(result);
                          finished[rt->outputId] = true;
                          numFinished += 1;
                      } else {
                          auto smt = dynamic_cast<ShuffleMapTask*>(event.task.get());
                          auto stage = idToStage[smt->stageId];
                          string result{event.result.v.begin(), event.result.v.end()};
                          stage->addOutputLoc(smt->partition, result);
                          if (running.count(stage.get()) && pendingTasks[stage.get()].empty()) {
                              running.erase(stage.get());
                              if (stage->shuffleDep.is_initialized()) {
                                  vector<host_t> locs;
                                  for (auto& v: stage->outputLocs) {
                                      locs.push_back(v[0]);
                                  }
                                  env.mapOutputTracker->registerMapOutputs(
                                          stage->shuffleDep.value()->shuffle_id(), locs);
                              }
                              updateCacheLocs();
                              vector<Stage*> newlyRunnable;
                              for (auto& s : waiting) {
                                  if (getMissingParentStages(*s).empty()) {
                                      newlyRunnable.push_back(s);
                                  }
                              }
                              for (auto& s : newlyRunnable) {
                                  waiting.erase(s);
                                  running.insert(s);
                              }
                              for (auto& s : newlyRunnable) {
                                  submitMissingTasks(
                                          runId, finalRdd, &funcWrapper,
                                          pendingTasks, partitions, finished,
                                          s, finalStage.get());
                              }
                          }
                      }
                  },
                  [&](const TaskEndReason::FetchFailed& f) {
                      // TODO: handle failure
                      ;
                  }
            );
        }
        // TODO: handle resubmit timeout
        if (!failed.empty()) {
            updateCacheLocs();
            for (auto ps : failed) {
                submitStage(runId, finalRdd, &funcWrapper,
                            pendingTasks, partitions, finished, finalStage.get(), waiting, running, ps);
            }
            failed.clear();
        }
    }
    eventQueues.erase(runId);
    return results;
}


template <typename T>
RDD<T>::RDD(SparkContext &sc_) : sc{sc_} {
    m_id = sc.newRddId();
}

template<typename T>
Iterator<T>* RDD<T>::iterator_impl(unique_ptr<Split> split) {
    if (shouldCache) {
        auto p = env.cacheTracker->getOrCompute(this, move(split));
        return p.release();
    } else {
        auto p = compute(move(split));
        return p.release();
    }
}

template<typename T>
vector<T> RDD<T>::collect() {
    auto cf = [](unique_ptr<Iterator<T>> iter) {
        vector<T> result;
        while (iter->hasNext()) {
            result.push_back(static_cast<T>(iter->next().value()));
        }
        return result;
    };
    auto v = sc.runJob(this, move(cf));
    return flatten(move(v));
}

template<typename K, typename V, typename C>
unique_ptr<Iterator<pair<K, C>>> ShuffleRDD<K, V, C>::compute(unique_ptr<Split> split) {
    unordered_map<K, C> combiners;
    auto mergePair = [&](pair<K, C> c) {
        if (!combiners.count(c.first)) {
            combiners.insert(move(c));
        } else {
            using mc_t = C(*)(C, C);
            auto f_mc = reinterpret_cast<mc_t>(aggregator->mergeCombiners());
            combiners[c.first] = f_mc(move(combiners[c.first]), move(c.second));
        }
    };
    env.shuffleFetcher->fetch(dep.shuffleId, split->m_index, move(mergePair));
    return make_unique<HashIterator<K, C>>(move(combiners));
}


#include <fstream>
#include <boost/archive/binary_oarchive.hpp>


template<typename K, typename V, typename C>
string ShuffleDependency<K, V, C>::runShuffle(RDDBase* rdd, unique_ptr<Split> split, size_t partition) {
    size_t numOutputSplits = partitioner->numPartitions();
    vector<unordered_map<K, C>> buckets{numOutputSplits};
    auto* rddkv = dynamic_cast<RDD<pair<K, V>>*>(rdd);
    auto iter = rddkv->compute(move(split));
    while (true) {
        auto s = iter->next();
        if (!s.is_initialized()) {
            break;
        }
        auto p = static_cast<pair<K, V>>(s.value());
        auto bucketId = partitioner->getPartition(p.first);
        auto& bucket = buckets[bucketId];
        if (!bucket.count(p.first)) {
            using fcc_t = C(*)(V);
            auto f_cc = reinterpret_cast<fcc_t>(aggregator->createCombiner());
            bucket[p.first] = f_cc(move(p.second));
        } else {
            using mv_t = C(*)(C, V);
            auto f_mv = reinterpret_cast<mv_t>(aggregator->mergeValue());
            bucket[p.first] = f_mv(move(bucket[p.first]), move(p.second));
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





#endif //SPARKPP_SPARK_CONTEXT_HPP
