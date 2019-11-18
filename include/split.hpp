//
// Created by xiaol on 11/13/2019.
//

#ifndef SPARKPP_SPLIT_HPP
#define SPARKPP_SPLIT_HPP

#include "common.hpp"

struct Split {
    size_t m_index;
    Split(size_t idx) : m_index{idx} {}
    virtual size_t index() {
        return m_index;
    }
};



#endif //SPARKPP_SPLIT_HPP
