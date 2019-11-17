//
// Created by xiaol on 11/12/2019.
//

#ifndef SPARKPP_MATCH_BASE_HPP
#define SPARKPP_MATCH_BASE_HPP

#include <variant>
#include <utility>
#include <cstdint>
#include <functional>
#include <type_traits>
#include "utils/serde.hpp"
#include "utils/function_signature.hpp"
#include <boost/variant/apply_visitor.hpp>
#include <boost/serialization/variant.hpp>

template <typename ...Args>
struct TypeList {};

template <typename T, std::size_t N>
struct TypeAt;

template <std::size_t N>
struct TypeAt<TypeList<>, N> {
    using type = void;
};

template <typename H, typename ...T, std::size_t N>
struct TypeAt<TypeList<H, T...>, N> {
    using type = std::conditional_t<N == 0, H, typename TypeAt<TypeList<T...>, N - 1>::type>;
};

template <typename T, std::size_t N>
using TypeAt_t = typename TypeAt<T, N>::type;

template <typename T, typename ST>
struct TypeIndex {
    constexpr static const int value = -1;
};

template <typename ...Args, typename ST>
struct TypeIndex<TypeList<ST, Args...>, ST> {
    constexpr static const int value = 0;
};

template <typename H, typename ...T, typename ST>
struct TypeIndex<TypeList<H, T...>, ST> {
    constexpr static const int value = (TypeIndex<TypeList<T...>, ST>::value == -1) ? -1 : 1 + (TypeIndex<TypeList<T...>, ST>::value);
};

template <typename L, typename ST>
constexpr std::size_t TypeIndex_v = TypeIndex<L, ST>::value;

template<class... Ts> struct overloaded : Ts... {
    using Ts::operator()...;
};
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

template <typename V, typename F>
struct GetIndex;

template <typename ...Args, typename F>
struct GetIndex<boost::variant<Args...>, F> {
    using arg_t = std::decay_t<typename function_traits<F>::template args<0>::type>;
    constexpr static const int value = TypeIndex_v<TypeList<Args...>, arg_t>;
};

// FIXME: add a fallback option
template <typename VT, typename F, typename R = typename function_traits<F>::result_type>
R match(VT&& v, F&& f) {
    using GI = GetIndex<std::decay_t<VT>, std::decay_t<F>>;
    using T = typename GI::arg_t;
    return std::invoke(std::forward<F>(f), std::forward<T>(boost::get<T>(std::forward<VT>(v))));
}

/// We need to dynamically match serialized variant, not statically call visitors
/// The base case compiles so the return type must be default-constructible
template <typename VT, typename F, typename ...Fs, typename R = typename function_traits<F>::result_type>
R match(VT&& v, F&& f, Fs&&... fs) {
    using GI = GetIndex<std::decay_t<VT>, std::decay_t<F>>;
    using T = typename GI::arg_t;
    if (v.which() == GI::value) {
        return std::invoke(std::forward<F>(f), std::forward<T>(boost::get<T>(std::forward<VT>(v))));
    }
    return match(std::forward<VT>(v), std::forward<Fs>(fs)...);
}

#endif //SPARKPP_MATCH_BASE_HPP
