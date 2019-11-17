//
// Created by xiaol on 11/13/2019.
//

#ifndef SPARKPP_SPARK_ENV_HPP
#define SPARKPP_SPARK_ENV_HPP

#include "common.hpp"
#include "cache.hpp"
#include "cache_tracker.hpp"
#include "map_output_tracker.hpp"
#include "shuffle_fetcher.hpp"
#include "shuffle_manager.hpp"

struct SparkEnv {
    BoundedMemoryCache cache;
    unique_ptr<MapOutputTracker> mapOutputTracker;
    unique_ptr<CacheTracker> cacheTracker;
    unique_ptr<ParallelShuffleFetcher> shuffleFetcher;
    unique_ptr<ShuffleManager> shuffleManager;
    void init(int argc, char** argv, const addr_t& masterAddr);
};

extern SparkEnv env;


#endif //SPARKPP_SPARK_EMV_HPP
