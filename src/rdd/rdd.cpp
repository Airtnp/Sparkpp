//
// Created by xiaol on 11/10/2019.
//

#include "rdd/rdd.hpp"
#include "spark_context.hpp"
#include "spark_env.hpp"

RDDBase* rdd_from_reader(::capnp::Data::Reader reader) {
    const char* bytes = reinterpret_cast<const char*>(reader.asBytes().begin());
    size_t size = reader.size();
    auto* rdd = reinterpret_cast<RDDBase*>(const_cast<char*>(bytes));
    rdd->deserialize_dyn(bytes, size);
    return rdd;
}
