//
// Created by xiaol on 11/14/2019.
//

#include "serialize_capnp.hpp"

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

template <typename F, typename R>
void sendTask(int fd, const F& func, const R& rdd) {
    /*
    ::capnp::MallocMessageBuilder builder;
    Task::Builder task = builder.initRoot<Task>();
    task.setFunc(func.to_reader());
    vector<char> v;
    rdd.serialize_dyn(v);
    task.setRdd(::capnp::Data::Reader{
            reinterpret_cast<unsigned char*>(v.data()),
            v.size()
    });
    ::capnp::writePackedMessageToFd(fd, builder);
    */
}

pair<FnBase*, RDDBase*> recvTask(int fd) {
    /*
    ::capnp::PackedFdMessageReader message{fd};
    Task::Reader task = message.getRoot<Task>();
    FnBase* func = fn_from_reader(task.getFunc());
    RDDBase* rdd = rdd_from_reader(task.getRdd());
    return make_pair(func, rdd);
     */
}

