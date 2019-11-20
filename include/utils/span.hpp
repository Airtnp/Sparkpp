//
// Created by xiaol on 11/14/2019.
//

#ifndef SPARKPP_SPAN_HPP
#define SPARKPP_SPAN_HPP

#include <vector>
#include <algorithm>

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

template <typename T>
auto make_span(T* ptr, size_t len) {
    return span<T>{ptr, len};
}

template <typename T>
std::vector<T> flatten(const std::vector<std::vector<T>>& v) {
    std::size_t total_size = 0;
    for (const auto& sub : v)
        total_size += sub.size();
    std::vector<T> result;
    result.reserve(total_size);
    for (const auto& sub : v)
        result.insert(result.end(), sub.begin(), sub.end());
    return result;
}


#endif //SPARKPP_SPAN_HPP
