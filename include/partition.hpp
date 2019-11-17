//
// Created by xiaol on 11/7/2019.
//

#ifndef SPARKPP_PARTITION_HPP
#define SPARKPP_PARTITION_HPP

#include "common.hpp"

struct Partition {
    int index;
    bool operator==(const Partition& rhs) {
        return index == rhs.index;
    }
};

namespace std {
    template <>
    struct hash<Partition> {
        std::size_t operator()(const Partition& p) const noexcept {
            return p.index;
        }
    };
}

#endif //SPARKPP_PARTITION_HPP
