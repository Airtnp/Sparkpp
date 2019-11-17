//
// Created by xiaol on 11/14/2019.
//

#ifndef SPARKPP_SPAN_HPP
#define SPARKPP_SPAN_HPP

template <typename T>
struct span {
    T* ptr;
    size_t len;
};

template <typename C>
auto make_span(C& c) {
    return span<typename C::value_type>{
        .ptr = c.data(),
        .len = c.size()
    };
}


#endif //SPARKPP_SPAN_HPP
