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
    std::atomic<int> nextRddId{0};
    std::atomic<int> nextShuffleId{0};
    SparkConfig config;
    vector<pair<string, uint16_t>> address;
    DAGScheduler scheduler;
    // scheduler

    int newRddId() {
        return nextRddId.fetch_add(1);
    }
    int newShuffleId() {
        return nextShuffleId.fetch_add(1);
    }

    SparkConfig getConfig(char** argv) {
        auto ty = string{argv[1]};
        if (ty == "master") {
            return SparkConfig{
                    SparkContextType::Distributed,
                    SparkDistributeType::Master,
                    30001,
                    {{"127.0.0.1", 27001}}
            };
        } else {
            return SparkConfig{
                    SparkContextType::Distributed,
                    SparkDistributeType::Slave,
                    27001,
                    {{"localhost", 30001}}
            };
        }
    }

    SparkContext(int argc, char** argv) : config{getConfig(argv)}, scheduler{config.addr} {
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
unique_ptr<IterBase> RDD<T>::iterator(unique_ptr<Split> split) {
    if (shouldCache) {
        return env.cacheTracker->getOrCompute(this, move(split));
    } else {
        return compute(move(split));
    }
}

template<typename T>
auto RDD<T>::collect() {
    auto cf = [](unique_ptr<Iterator<T>> iter) {
        vector<T> result;
        while (iter->hasNext()) {
            result.push_back(static_cast<T>(iter->next().value()));
        }
        return result;
    };
    sc.runJob(this, move(cf));
}

#endif //SPARKPP_SPARK_CONTEXT_HPP
