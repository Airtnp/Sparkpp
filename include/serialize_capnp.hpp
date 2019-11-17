//
// Created by xiaol on 11/14/2019.
//

#ifndef SPARKPP_SERIALIZE_CAPNP_HPP
#define SPARKPP_SERIALIZE_CAPNP_HPP

#include "common.hpp"
#include "rdd/rdd.hpp"
#include "serialize_wrapper.hpp"
#include "data.capnp.h"
#include <capnp/serialize-packed.h>
#include <capnp/message.h>

template <typename T>
void sendData(int fd, ::capnp::Data::Reader reader);

template <typename T>
void sendData(int fd, vector<char>& bytes);

template <typename T>
typename ::capnp::Data::Reader recvData(int fd);

template <typename F, typename R>
void sendTask(int fd, const F& func, const R& rdd);

pair<FnBase*, RDDBase*> recvTask(int fd);



#endif //SPARKPP_SERIALIZE_CAPNP_HPP
