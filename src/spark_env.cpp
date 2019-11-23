//
// Created by xiaol on 11/14/2019.
//

#include "common.hpp"
#include "spark_env.hpp"

void SparkEnv::init([[maybe_unused]] int argc, char **argv, const addr_t& masterAddr) {
    bool isMaster = false;
    if (!strcmp(argv[1], "master")) {
        isMaster = true;
    }
    mapOutputTracker = make_unique<MapOutputTracker>(isMaster, masterAddr);
    cacheTracker = make_unique<CacheTracker>(isMaster, masterAddr, cache);
    shuffleManager = make_unique<ShuffleManager>();
    shuffleFetcher = make_unique<ParallelShuffleFetcher>();
}

