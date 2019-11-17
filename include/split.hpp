//
// Created by xiaol on 11/13/2019.
//

#ifndef SPARKPP_SPLIT_HPP
#define SPARKPP_SPLIT_HPP

#include "common.hpp"

struct Split {
    int m_index;
    virtual int index() {
        return m_index;
    }
};



#endif //SPARKPP_SPLIT_HPP
