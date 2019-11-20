//
// Created by xiaol on 11/20/2019.
//

#include "dependency.hpp"

ShuffleDependencyBase* dep_from_reader(::capnp::Data::Reader reader) {
    const char* bytes = reinterpret_cast<const char*>(reader.asBytes().begin());
    size_t size = reader.size();
    auto base = reinterpret_cast<ShuffleDependencyBase*>(
            const_cast<unsigned char*>(reader.asBytes().begin()));
    base->deserialize_dyn(bytes, size);
    return base;
}