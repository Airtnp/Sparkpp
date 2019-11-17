//
// Created by xiaol on 11/13/2019.
//

#ifndef SPARKPP_PAIR_HASH_HPP
#define SPARKPP_PAIR_HASH_HPP

#include <utility>
#include <cstdint>
#include <functional>
#include <boost/functional/hash.hpp>

struct pair_hash {
    template <typename T, typename U>
    std::size_t operator()(const std::pair<T, U>& x) const {
        return boost::hash_value(x);
    }
};

#endif //SPARKPP_PAIR_HASH_HPP
