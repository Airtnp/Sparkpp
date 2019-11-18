//
// Created by xiaol on 11/12/2019.
//

#ifndef SPARKPP_TASK_HPP
#define SPARKPP_TASK_HPP

#include "common.hpp"
#include "rdd/rdd.hpp"
#include "serialize_wrapper.hpp"
#include "dependency.hpp"

struct TaskContext {
    size_t runId;
    size_t stageId;
    // int attemptId;
};

struct Task {
    virtual Storage run(size_t id) = 0;
    virtual vector<host_t> preferredLocations() {
        return {};
    }
    virtual optional<size_t> generation() {
        return {};
    }
    virtual size_t task_id() const = 0;
    virtual size_t run_id() const = 0;
    virtual size_t stage_id() const = 0;
};

struct ResultTask : Task {
    size_t taskId = 0;
    size_t runId = 0;
    size_t stageId = 0;
    RDDBase* rdd;
    FnBase* func;
    size_t partition;
    vector<host_t> locs;
    size_t outputId = 0;

    ResultTask(size_t pid, RDDBase* r, FnBase* f)
        : rdd{r}, func{f}, partition{pid} {}

    ResultTask(size_t tid, size_t rid, size_t sid, RDDBase* r, FnBase* f, size_t pid, vector<host_t> l, size_t oid)
        : taskId{tid}, runId{rid}, stageId{sid}, rdd{r}, func{f}, partition{pid}, locs{move(l)}, outputId{oid} {}
    Storage run(size_t attemptId) {
        unique_ptr<Split> split = move(rdd->split(partition));
        return func->call(rdd->compute(move(split)));
    }
    vector<host_t> preferredLocations() override {
        return locs;
    }
    size_t task_id() const override {
        return taskId;
    }
    size_t run_id() const override {
        return runId;
    }
    size_t stage_id() const override {
        return stageId;
    }
};

struct ShuffleMapTask : Task {
    size_t taskId = 0;
    size_t runId = 0;
    size_t stageId = 0;
    RDDBase* rdd;
    ShuffleDependencyBase* dep;
    size_t partition;
    vector<host_t> locs;

    ShuffleMapTask(size_t pid, RDDBase* r, ShuffleDependencyBase* d_)
        : rdd{r}, dep{d_}, partition{pid} {

    }

    ShuffleMapTask(size_t tid, size_t rid, size_t sid, RDDBase* r, ShuffleDependencyBase* d_, size_t pid, vector<host_t> l)
        : taskId{tid}, runId{rid}, stageId{sid}, rdd{r}, dep{d_}, partition{pid}, locs{move(l)} {}
    vector<host_t> preferredLocations() override {
        return locs;
    }
    Storage run(size_t attemptId) {
        unique_ptr<Split> split = rdd->split(partition);
        auto s = dep->runShuffle(rdd, move(split), partition);
        Storage st{
            {s.begin(), s.end()}
        };
        return st;
    }
    size_t task_id() const override {
        return taskId;
    }
    size_t run_id() const override {
        return runId;
    }
    size_t stage_id() const override {
        return stageId;
    }
};

#endif //SPARKPP_TASK_HPP
