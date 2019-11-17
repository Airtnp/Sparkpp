//
// Created by xiaol on 11/14/2019.
//

#include "common.hpp"
#include "spark_env.hpp"

void SparkEnv::init(int argc, char **argv, const addr_t& masterAddr) {
    if (!strcmp(argv[1], "master")) {
        mapOutputTracker = make_unique<MapOutputTracker>(true, masterAddr);
        cacheTracker = make_unique<CacheTracker>(true, masterAddr, cache);
        shuffleManager = make_unique<ShuffleManager>();
        shuffleFetcher = make_unique<ParallelShuffleFetcher>();
    }
}

