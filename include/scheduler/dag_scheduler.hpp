//
// Created by xiaol on 11/11/2019.
//

#ifndef SPARKPP_DAG_SCHEDULER_HPP
#define SPARKPP_DAG_SCHEDULER_HPP

#include "common.hpp"
#include "rdd/rdd.hpp"
#include "serialize_wrapper.hpp"
#include "serialize_capnp.hpp"
#include "scheduler/scheduler.hpp"
#include "scheduler/task.hpp"
#include "scheduler/stage.hpp"
#include "scheduler/active_job.hpp"
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/post.hpp>

struct TaskEndReason {
    struct Success {
        SN_BOOST_SERIALIZE_EMPTY();
    };
    struct FetchFailed {
        string addr;
        size_t shuffleId;
        size_t mapId;
        size_t reduceId;
        SN_BOOST_SERIALIZE_MEMBERS_IN(addr, shuffleId, mapId, reduceId);
    };
    struct Error {
        string reason;
        SN_BOOST_SERIALIZE_MEMBERS_IN(reason);
    };
    variant<Success, FetchFailed, Error> vmember;
    auto& get() {
        return vmember;
    }
    const auto& get() const {
        return vmember;
    }
};

struct CompletionEvent {
    unique_ptr<Task> task;
    TaskEndReason reason;
    Storage result;
    CompletionEvent() noexcept {}
    CompletionEvent(unique_ptr<Task>&& task_, TaskEndReason reason, Storage result) noexcept
        : task{move(task_)}, reason{move(reason)}, result{move(result)} {}
    CompletionEvent(CompletionEvent&& rhs) noexcept
    : task{move(rhs.task)}, reason{move(rhs.reason)}, result{move(rhs.result)} {}
    CompletionEvent& operator=(CompletionEvent&& rhs) noexcept {
        task = move(rhs.task);
        reason = move(rhs.reason);
        result = move(rhs.result);
        return *this;
    }
    // accumUpdates
};

/*
struct DAGEventLoop : EventLoop<DAGSchedulerEvent> {
    DAGScheduler& dagScheduler;
    DAGEventLoop(DAGScheduler& schd) : dagScheduler{schd} {}
    void onReceive(DAGSchedulerEvent event);
};
 */

// Spark master-version Job path:
//      runJob -> submitJob -> JobWaiter ->
//      handleJobSubmitted -> ActiveJob -> submitStage ->
//      submitTasks -> launchTasks -> handleSuccessfulTask -> 
//      CompletionEvent -> handleTaskCompletion ->
//      job.taskSucceeded -> submitJob -> runJob
// Spark branch-0.5 Job path
//      runJob -> submitStage -> submitMissingTasks ->
//      submitTasks -> taskEnded -> CompletionEvent ->
//      waitForEvent -> submitMissingTasks
/// Basically DAGScheduler + TaskScheduler
struct DAGScheduler : Scheduler {
    atomic<size_t> nextJobId;
    atomic<size_t> nextStageId;
    atomic<size_t> nextRunId;
    atomic<size_t> nextTaskId;
    // NOTE: only accessible from main thread
    unordered_map<size_t, shared_ptr<Stage>> idToStage;
    unordered_map<size_t, shared_ptr<Stage>> shuffleToMapStage;
    unordered_map<size_t, vector<vector<host_t>>> cacheLocs;
    unordered_map<size_t, BlockingConcurrentQueue<CompletionEvent>> eventQueues;
    vector<addr_t> address;
    boost::asio::thread_pool pool{4};

    DAGScheduler(vector<addr_t> addr) : address{move(addr)} {}

    shared_ptr<Stage> newStage(RDDBase* rdd, optional<ShuffleDependencyBase*> shuffleDep);
    shared_ptr<Stage> getShuffleMapStage(ShuffleDependencyBase* shuffleDep);

    vector<host_t> getPreferredLocs(RDDBase* rdd, size_t partitionId);
    vector<Stage*> getMissingParentStages(const Stage& stage);
    vector<Stage*> getParentStages(RDDBase* rdd);


    auto& getCacheLocs(RDDBase* rdd) {
        return cacheLocs[rdd->id()];
    }

    void updateCacheLocs();

    template <typename F, typename T, typename U = typename function_traits<F>::result_type>
    vector<U> runJob(F&& func, RDD<T>* finalRdd, const vector<size_t>& partitions);


    void visitMissingParent(unordered_set<Stage *> &missing, unordered_set<RDDBase *> &visited, RDDBase *r);
    void visitParent(unordered_set<Stage*>& parents, unordered_set<RDDBase*>& visited, RDDBase* r);

    /// when a stage's parents are available, do the task
    void submitMissingTasks(size_t runId, RDDBase *finalRdd, FnBase *func,
                            unordered_map<Stage *, unordered_set<size_t>>& pendingTasks,
                            const vector<size_t>& partitions,
                            const vector<bool>& finished, Stage *stage, Stage *finalStage);

    void submitTasks(unique_ptr<Task> task);

    void
    submitStage(size_t runId, RDDBase *finalRdd, FnBase *func,
                unordered_map<Stage *, unordered_set<size_t>> &pendingTasks,
                const vector<size_t> &partitions, const vector<bool> &finished, Stage *finalStage,
                unordered_set<Stage *> waiting, unordered_set<Stage *> running, Stage *stage);

    void taskEnded(unique_ptr<Task> task, TaskEndReason reason, Storage result);
};






#endif //SPARKPP_DAG_SCHEDULER_HPP
