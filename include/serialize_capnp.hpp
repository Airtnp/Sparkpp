//
// Created by xiaol on 11/14/2019.
//

#ifndef SPARKPP_SERIALIZE_CAPNP_HPP
#define SPARKPP_SERIALIZE_CAPNP_HPP

#include "common.hpp"
#include "scheduler/task.hpp"
#include "serialize_wrapper.hpp"
#include "data.capnp.h"
#include <capnp/serialize-packed.h>
#include <capnp/message.h>

::capnp::Data::Reader vec_to_reader(vector<char>&);
vector<char> reader_to_vec(::capnp::Data::Reader);

template <typename T>
void sendData(int fd, ::capnp::Data::Reader reader);

template <typename T>
void sendData(int fd, vector<char>& bytes);

template <typename T>
typename ::capnp::Data::Reader recvData(int fd);

void sendExecution(int fd, Task* task);

unique_ptr<Task> recvExecution(int fd);

template <typename T>
void sendData(int fd, ::capnp::Data::Reader reader) {
    ::capnp::MallocMessageBuilder builder;
    typename T::Builder data = builder.initRoot<T>();
    data.setMsg(reader);
    ::capnp::writeMessageToFd(fd, builder);
}

template <typename T>
void sendData(int fd, vector<char>& bytes) {
    ::capnp::Data::Reader reader{
            reinterpret_cast<unsigned char*>(bytes.data()),
            bytes.size()
    };
    sendData<T>(fd, reader);
}

template <typename T>
typename ::capnp::Data::Reader recvData(int fd) {
    ::capnp::PackedFdMessageReader message{fd};
    typename T::Reader result = message.getRoot<T>();
    return result.getMsg();
}

#endif //SPARKPP_SERIALIZE_CAPNP_HPP
