//
// Created by xiaol on 11/12/2019.
//

#include "common.hpp"
#include "serialize_capnp.hpp"
#include "scheduler/dag_scheduler.hpp"
#include "spark_env.hpp"

shared_ptr<Stage> DAGScheduler::newStage(RDDBase *rdd, optional<ShuffleDependencyBase*> shuffleDep) {
    env.cacheTracker->registerRDD(rdd->id(), rdd->numOfSplits());
    if (shuffleDep.is_initialized()) {
        env.mapOutputTracker->registerShuffle(shuffleDep.value()->shuffle_id(), rdd->numOfSplits());
    }
    size_t id = nextStageId.fetch_add(1);
    idToStage[id] = std::make_shared<Stage>(id, rdd, shuffleDep, getParentStages(rdd));
    return idToStage[id];
}

void DAGScheduler::visitMissingParent(
        unordered_set<Stage*>& missing, unordered_set<RDDBase*>& visited, RDDBase* r) {
    if (!visited.count(r)) {
        visited.insert(r);
        auto locs = getCacheLocs(r);
        for (size_t p = 0; p < r->numOfSplits(); ++p) {
            if (locs[p].empty()) {
                auto s = r->dependencies();
                for (size_t i = 0; i < s.len; ++i) {
                    auto dep = s.ptr[i];
                    if (auto shufDep = dynamic_cast<ShuffleDependencyBase*>(dep)) {
                        auto stage = getShuffleMapStage(shufDep);
                        if (!stage->isAvailable()) {
                            missing.insert(stage.get());
                        }
                    } else if (auto narrowDep = dynamic_cast<NarrowDependency*>(dep)) {
                        visitMissingParent(missing, visited, narrowDep->rdd());
                    }
                }
            }
        }
    }
}


vector<Stage*> DAGScheduler::getMissingParentStages(const Stage& stage) {
    unordered_set<Stage*> missing;
    unordered_set<RDDBase*> visited;
    visitMissingParent(missing, visited, stage.rdd);
    return {std::make_move_iterator(missing.begin()), std::make_move_iterator(missing.end())};
}

void DAGScheduler::submitMissingTasks(
        size_t runId,
        RDDBase* finalRdd,
        FnBase* func,
        unordered_map<Stage*, unordered_set<size_t>>& pendingTasks,
        const vector<size_t>& partitions,
        const vector<bool>& finished,
        Stage* stage, Stage* finalStage) {
    auto& pending = pendingTasks[stage];
    size_t numOutputParts = partitions.size();
    if (stage == finalStage) {
        for (size_t id = 0; id < numOutputParts; ++id) {
            if (!finished[id]) {
                size_t partitionId = partitions[id];
                auto locs = getPreferredLocs(finalRdd, partitionId);
                size_t taskId = nextTaskId.fetch_add(1);
                pending.insert(taskId);

                auto task = make_unique<ResultTask>(
                    taskId, runId, finalStage->id, finalRdd,
                    func, partitionId, move(locs), id);
                submitTasks(move(task));
            }
        }
    } else {
        for (size_t p = 0; p < stage->numPartitions; ++p) {
            if (stage->outputLocs[p].empty()) {
                auto locs = getPreferredLocs(stage->rdd, p);
                size_t taskId = nextTaskId.fetch_add(1);
                pending.insert(taskId);

                auto task = make_unique<ShuffleMapTask>(
                        taskId, runId, stage->id, stage->rdd,
                        stage->shuffleDep.value(), p, move(locs));
                submitTasks(move(task));
            }
        }
    }
}

void DAGScheduler::submitStage(
        size_t runId, RDDBase *finalRdd, FnBase *func,
        unordered_map<Stage *, unordered_set<size_t>>& pendingTasks,
        const vector<size_t>& partitions,
        const vector<bool>& finished, Stage *finalStage,
        unordered_set<Stage*> waiting,
        unordered_set<Stage*> running,
        Stage* stage
        ) {
    if (!waiting.count(stage) && !running.count(stage)) {
        auto missing = getMissingParentStages(*stage);
        if (missing.empty()) {
            submitMissingTasks(runId, finalRdd, func,
                    pendingTasks, partitions, finished, stage, finalStage);
            running.insert(stage);
        } else {
            for (auto& s : missing) {
                submitStage(runId, finalRdd, func,
                        pendingTasks, partitions, finished, finalStage, waiting, running, s);
            }
            waiting.insert(stage);
        }
    }
}

void DAGScheduler::submitTasks(unique_ptr<Task> task) {
    // round-robin
    auto [host, port] = address.back();
    address.pop_back();
    address.insert(address.begin(), make_pair(host, port));
    boost::asio::post(pool, [this, host = host, port = port, task = move(task)]() mutable {
        io_service ioc;
        ip::tcp::resolver resolver{ioc};
        ip::tcp::resolver::query query{host, std::to_string(port)};
        auto iter = resolver.resolve(query);
        ip::tcp::resolver::iterator end;
        ip::tcp::endpoint endpoint = *iter;
        ip::tcp::socket socket{ioc};
        boost::system::error_code ec;
        do {
            auto start_iter = iter;
            ec.clear();
            socket.close();
            std::this_thread::sleep_for(5ms);
            while (start_iter != end) {
                socket.connect(endpoint, ec);
                if (!ec) break;
                ++start_iter;
            }
        } while (ec);
        int fd = socket.native_handle();
        sendExecution(fd, task.get());
        ::capnp::PackedFdMessageReader message{fd};
        auto reader = recvData<Result>(message);
        Storage s{reader_to_vec(reader)};
        size_t runId = task->run_id();
        // FIXME: dispatch to taskEnded
        CompletionEvent event {
            move(task),
            {TaskEndReason::Success{}},
            move(s)
        };
        eventQueues[runId].enqueue(move(event));
    });
}

void DAGScheduler::updateCacheLocs() {
    cacheLocs = env.cacheTracker->getLocationsSnapshot();
}

void DAGScheduler::taskEnded(unique_ptr<Task> task, TaskEndReason reason, Storage result) {
    size_t id = task->run_id();
    if (eventQueues[id].size_approx() != 0) {
        eventQueues[id].enqueue(CompletionEvent{
            move(task),
            move(reason),
            move(result)
        });
    }
}

vector<Stage*> DAGScheduler::getParentStages(RDDBase *rdd) {
    unordered_set<Stage*> parents;
    unordered_set<RDDBase*> visited;
    visitParent(parents, visited, rdd);
    return {std::make_move_iterator(parents.begin()), std::make_move_iterator(parents.end())};
}

shared_ptr<Stage> DAGScheduler::getShuffleMapStage(ShuffleDependencyBase* shuffleDep) {
    size_t id = shuffleDep->shuffle_id();
    if (shuffleToMapStage.count(id)) {
        return shuffleToMapStage[id];
    }
    shuffleToMapStage[id] = newStage(shuffleDep->rdd(), {shuffleDep});
    return shuffleToMapStage[id];
}

void DAGScheduler::visitParent(
        unordered_set<Stage*>& parents, unordered_set<RDDBase*>& visited, RDDBase* r) {
    if (!visited.count(r)) {
        visited.insert(r);
        env.cacheTracker->registerRDD(r->id(), r->numOfSplits());
        auto s = r->dependencies();
        for (size_t i = 0; i < s.len; ++i) {
            auto dep = s.ptr[i];
            if (auto shufDep = dynamic_cast<ShuffleDependencyBase*>(dep)) {
                parents.insert(getShuffleMapStage(shufDep).get());
            } else {
                visitParent(parents, visited, dep->rdd());
            }
        }
    }
}

vector<host_t> DAGScheduler::getPreferredLocs(RDDBase* rdd, size_t partitionId) {
    auto& cached = getCacheLocs(rdd)[partitionId];
    if (!cached.empty()) {
        return cached;
    }
    // TODO: add RDD placement preferences
    // auto rddPrefs = rdd->preferredLocations();
    auto dep = rdd->dependencies();
    for (size_t i = 0; i < dep.len; ++i) {
        if (auto n = dynamic_cast<NarrowDependency*>(dep.ptr[i])) {
            for (auto& inPart : n->getParents(partitionId)) {
                auto locs = getPreferredLocs(n->rdd(), inPart);
                if (!locs.empty())
                    return locs;
            }
        }
    }
    return {};
}

