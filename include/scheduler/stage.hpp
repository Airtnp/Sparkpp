//
// Created by xiaol on 11/12/2019.
//

#ifndef SPARKPP_STAGE_HPP
#define SPARKPP_STAGE_HPP

#include <utility>

#include "common.hpp"
#include "rdd/rdd.hpp"
#include "dependency.hpp"

struct Stage {
    size_t id;
    RDDBase* rdd;
    optional<ShuffleDependencyBase*> shuffleDep;
    vector<Stage*> parents;
    vector<vector<host_t>> outputLocs;
    size_t numAvailableOutputs = 0;
    size_t numPartitions;

    Stage(size_t id_, RDDBase* rdd_, optional<ShuffleDependencyBase*> shuffleDep_, vector<Stage*> stage_)
        : id{id_}, rdd{rdd_}, shuffleDep{shuffleDep_}, parents{std::move(stage_)} {
        outputLocs.resize(rdd->numOfSplits());
        numPartitions = rdd->numOfSplits();
    }

    bool isShuffleMap() {
        return shuffleDep.has_value();
    }
    bool isAvailable() {
        if (parents.empty() && !isShuffleMap()) {
            return true;
        }
        return numAvailableOutputs == rdd->numOfSplits();
    }

    void addOutputLoc(size_t partition, host_t host) {
        if (outputLocs[partition].empty()) {
            numAvailableOutputs += 1;
        }
        outputLocs[partition].push_back(move(host));
    }
    void removeOutputLoc(size_t partition, host_t host) {
        size_t old = outputLocs[partition].size();
        outputLocs[partition].erase(
                remove_if(outputLocs[partition].begin(), outputLocs[partition].end(), [&](const host_t& h) {
                    return h == host;
        }), outputLocs[partition].end());
        if (old != 0 && outputLocs[partition].empty()) {
            numAvailableOutputs -= 1;
        }
    }
};


#endif //SPARKPP_STAGE_HPP
