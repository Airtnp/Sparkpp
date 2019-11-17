//
// Created by xiaol on 11/7/2019.
//

#ifndef SPARKPP_TRAITS_HPP
#define SPARKPP_TRAITS_HPP

#include <functional>
#include <utility>

template <typename K, typename V, typename C, class T>
concept AggregatorTrait = requires(T a, K k, V v, C c) {
    { a.createCombiner(v) } -> C;
    { a.mergeValue(c, v) } -> C;
    { a.mergeCombiners(c, c) } -> C
};


template <class F, class ...Args>
concept Invocable = requires(F&& f, Args&&... args) {
    std::invoke(std::forward<F>(f), std::forward<Args>(args)...);
};

template <class F, class R, class ...Args>
concept Fn = requires(F&& f, Args&&... args) {
    { std::invoke(std::forward<F>(f), std::forward<Args>(args)...) } -> R;
};

#endif //SPARKPP_TRAITS_HPP
