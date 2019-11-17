//
// Created by xiaol on 11/14/2019.
//

#ifndef SPARKPP_SHUFFLE_FETCHER_HPP
#define SPARKPP_SHUFFLE_FETCHER_HPP

#include "common.hpp"

struct ParallelShuffleFetcher {
    template <typename F>
    void fetch(size_t shuffleId, size_t reduceId, F&& func);
};



#endif //SPARKPP_SHUFFLE_FETCHER_HPP
