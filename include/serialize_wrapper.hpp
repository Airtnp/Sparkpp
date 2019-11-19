//
// Created by xiaol on 11/10/2019.
//

#ifndef SPARKPP_SERIALIZE_WRAPPER_HPP
#define SPARKPP_SERIALIZE_WRAPPER_HPP

#include "common.hpp"
#include <capnp/message.h>

struct Storage {
    vector<char> v;
    Storage() = default;
    Storage(vector<char> v_) : v{move(v_)} {};
    Storage(const Storage& rhs) = default;
    Storage(Storage&& rhs) = default;
    Storage& operator=(const Storage&) = default;
    Storage& operator=(Storage&&) = default;
    // value-semantic T
    template <typename T>
    explicit operator T() {
        return *reinterpret_cast<T*>(v.data());
    }
    ::capnp::Data::Reader to_reader() {
        return {reinterpret_cast<unsigned char*>(v.data()), v.size()};
    }
};

struct IterBase {
    // FIXME: lots of virtual calling to this method to get data...
    virtual optional<Storage> next() = 0;
    virtual bool hasNext() = 0;
    virtual ~IterBase() = default;
};

/// `slice::Iter`
template <typename T>
struct Iterator : IterBase {
    const T* ptr;
    const T* end;
    Iterator(const T* p, const T* e) : ptr{p}, end{e} {};
    optional<Storage> next() override {
        if (ptr != end) {
            auto pv = reinterpret_cast<const char*>(ptr);
            vector<char> v{pv, pv + sizeof(T)};
            ++ptr;
            return {Storage{move(v)}};
        }
        return {};
    }
    bool hasNext() override {
        return ptr != end;
    }
    vector<T> collect() {
        return {ptr, end};
    }
};

/// T must be a standard container type
template <typename T>
struct OwnIterator : Iterator<T> {
    vector<T> data;
    // @ref: https://stackoverflow.com/questions/41384793/does-stdmove-invalidate-iterators
    // move constructor will not invalidate iterators
    OwnIterator(vector<T> data_) : Iterator<T>{data_.data(), data_.data() + data_.size()}, data{move(data_)} {}
};

template <typename F, typename T>
struct MapIterator : Iterator<T> {
    unique_ptr<Iterator<T>> prev;
    F func;
    MapIterator(unique_ptr<Iterator<T>> prev, F func)
        : Iterator<T>{prev->ptr, prev->end}, prev{move(prev)}, func{move(func)} {}
    optional<Storage> next() override {
        auto s = Iterator<T>::next();
        return s.map(func);
    }
};




struct FnBase {
    virtual Storage call(unique_ptr<IterBase> ib) = 0;
    virtual ::capnp::Data::Reader to_reader() = 0;
    virtual ~FnBase() = default;
};


template <typename T>
void serialize(const T& v, vector<char>& bytes);

/// The wrapped function must have no reference / environment dependency (value semantics)
template <typename F>
struct FnWrapper : FnBase {
    F f;
    using FuncSig = function_traits<F>;
    // NOTE: R should be serializable and call serial() function
    using R = typename FuncSig::result_type;
    // single input, unique_ptr<Iterator<Ty>>
    using T = typename decay_t<typename FuncSig::template args<0>::type>::element_type;
    FnWrapper(F f_) : f{move(f_)} {};

    // input type could be any, but output type should be trivially_constructible
    Storage call(unique_ptr<IterBase> ib) override {
        unique_ptr<T> iter = dynamic_unique_ptr_cast<T>(move(ib));
        auto result = f(move(iter));
        vector<char> bytes;
        serialize(result, bytes);
        return Storage{move(bytes)};
    }
    ::capnp::Data::Reader to_reader() override {
        return {reinterpret_cast<unsigned char*>(this), sizeof(FnWrapper)};
    }
};

template <typename F>
FnWrapper(F&&) -> FnWrapper<F>;

FnBase* fn_from_reader(::capnp::Data::Reader reader);

template <typename T>
void serialize(const T& v, vector<char>& bytes) {
    SerialGuard gd{bytes};
    gd << v;
}

template <typename T>
void deserialize(T& v, const char* bytes, size_t size) {
    DeserialGuard gd{bytes, size};
    gd >> v;
}

template <typename T>
void deserialize(T& v, ::capnp::Data::Reader reader) {
    DeserialGuard gd{reinterpret_cast<const char*>(reader.asBytes().begin()), reader.size()};
    gd >> v;
}










#endif //SPARKPP_SERIALIZE_WRAPPER_HPP
