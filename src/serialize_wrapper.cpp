//
// Created by xiaol on 11/11/2019.
//

#include "serialize_wrapper.hpp"

FnBase* fn_from_reader(::capnp::Data::Reader reader) {
    return reinterpret_cast<FnBase*>(
            const_cast<unsigned char*>(reader.asBytes().begin()));
}
