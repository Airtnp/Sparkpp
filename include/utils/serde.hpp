//
// Created by xiaol on 11/14/2019.
//

#ifndef SPARKPP_SERDE_HPP
#define SPARKPP_SERDE_HPP

#include <cstdint>
#include <vector>
#include <utility>
#include <tuple>
#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/serialization/unordered_set.hpp>
#include <boost/serialization/variant.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/optional.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/list.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>

/// @ref: https://github.com/Sydius/serialize-tuple/blob/master/serialize_tuple.h
namespace boost {
    namespace serialization {

        template<uint N>
        struct Serialize
        {
            template<class Archive, typename... Args>
            static void serialize(Archive & ar, std::tuple<Args...> & t, const unsigned int version)
            {
                ar & std::get<N-1>(t);
                Serialize<N-1>::serialize(ar, t, version);
            }
        };

        template<>
        struct Serialize<0>
        {
            template<class Archive, typename... Args>
            static void serialize(Archive & ar, std::tuple<Args...> & t, const unsigned int version)
            {
                (void) ar;
                (void) t;
                (void) version;
            }
        };

        template<class Archive, typename... Args>
        void serialize(Archive & ar, std::tuple<Args...> & t, const unsigned int version)
        {
            Serialize<sizeof...(Args)>::serialize(ar, t, version);
        }

    }
}


struct SerialGuard {
    boost::iostreams::back_insert_device<vector<char>> sink;
    boost::iostreams::stream<boost::iostreams::back_insert_device<vector<char>>> s;
    boost::archive::binary_oarchive oa;
    SerialGuard(std::vector<char>& bytes)
        : sink{bytes}, s{sink}, oa{s, boost::archive::no_header | boost::archive::no_tracking} {}
    template <typename T>
    SerialGuard& operator<<(T&& t) {
        oa << std::forward<T>(t);
        return *this;
    }
    template <typename T>
    SerialGuard& operator&(T&& t) {
        oa & std::forward<T>(t);
        return *this;
    }
    ~SerialGuard() {
        s.flush();
    }
};

struct DeserialGuard {
    boost::iostreams::basic_array_source<char> device;
    boost::iostreams::stream<boost::iostreams::basic_array_source<char>> s;
    boost::archive::binary_iarchive ia;
    DeserialGuard(const char* bytes, std::size_t size)
        : device{bytes, size}, s{device}, ia{s, boost::archive::no_header | boost::archive::no_tracking} {}
    template <typename T>
    DeserialGuard& operator>>(T& t) {
        ia >> t;
        return *this;
    }
    template <typename T>
    DeserialGuard& operator&(T& t) {
        ia & t;
        return *this;
    }
    ~DeserialGuard() = default;
};



#endif //SPARKPP_SERDE_HPP
