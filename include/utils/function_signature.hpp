//
// Created by xiaol on 11/10/2019.
//

#ifndef SPARKPP_FUNCTION_SIGNATURE_HPP
#define SPARKPP_FUNCTION_SIGNATURE_HPP

template <typename T>
struct function_traits;

//simple function
template <typename R, typename... Args>
struct function_traits<R(Args...)> {
    enum {
        args_size = sizeof...(Args)
    };
    typedef R type(Args...);
    using result_type = R;
    using pointer = R(*)(Args...);
    using function_type = R(Args...);
    using stl_function_type = std::function<function_type>;

    template <size_t I>
    struct args {
        using type = typename std::tuple_element<I, std::tuple<Args...>>::type;
    };

};

// function pointer
template <typename R, typename... Args>
struct function_traits<R(*)(Args...)> : public function_traits<R(Args...)> {
    using type = R(*)(Args...);
};

//const, volatile specialization member function
template <typename R, typename C, typename... Args>
struct function_traits<R(C::*)(Args...)> : public function_traits<R(Args...)> {
    using class_type = C;
    using type = R(C::*)(Args...);
};

template <typename R, typename C, typename... Args>
struct function_traits<R(C::*)(Args...) const> : public function_traits<R(Args...)> {
    using class_type = C;
    using type = R(C::*)(Args...);
};

// function pointer
template <typename R, typename... Args>
struct function_traits<R(*)(Args...) noexcept> : public function_traits<R(Args...)> {
    using type = R(*)(Args...);
};

//const, volatile specialization member function
template <typename R, typename C, typename... Args>
struct function_traits<R(C::*)(Args...) noexcept> : public function_traits<R(Args...)> {
    using class_type = C;
    using type = R(C::*)(Args...);
};

template <typename R, typename C, typename... Args>
struct function_traits<R(C::*)(Args...) const noexcept> : public function_traits<R(Args...)> {
    using class_type = C;
    using type = R(C::*)(Args...);
};

//std::function
template <typename R, typename... Args>
struct function_traits<std::function<R(Args...)>> : public function_traits<R(Args...)> {
    using type = std::function<R(Args...)>;
};

//function object / functor / lambda
template <typename F>
struct function_traits : public function_traits<decltype(&std::decay_t<F>::operator())> {
    using type = decltype(&std::decay_t<F>::operator());
};

#endif //SPARKPP_FUNCTION_SIGNATURE_HPP
