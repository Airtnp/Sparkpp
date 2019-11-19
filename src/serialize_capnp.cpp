//
// Created by xiaol on 11/14/2019.
//

#include "serialize_capnp.hpp"

::capnp::Data::Reader vec_to_reader(vector<char>& v) {
    return ::capnp::Data::Reader{
            reinterpret_cast<unsigned char*>(v.data()),
            v.size()
    };
}

vector<char> reader_to_vec(::capnp::Data::Reader reader) {
    char* bytes = reinterpret_cast<char*>(const_cast<unsigned char*>(reader.asBytes().begin()));
    size_t size = reader.size();
    return {
        bytes,
        bytes + size
    };
}


void sendExecution(int fd, Task* task) {
    ::capnp::MallocMessageBuilder builder;
    Execution::Builder exec = builder.initRoot<Execution>();
    if (auto rt = dynamic_cast<ResultTask*>(task)) {
        exec.setIsShuffle(false);
        exec.setPartitionId(rt->partition);
        exec.setFuncOrDep(rt->func->to_reader());
        vector<char> v;
        rt->rdd->serialize_dyn(v);
        exec.setRdd(vec_to_reader(v));
    } else {
        auto smt = dynamic_cast<ShuffleMapTask*>(task);
        exec.setIsShuffle(true);
        exec.setPartitionId(smt->partition);
        vector<char> depV;
        smt->dep->serialize_dyn(depV);
        exec.setFuncOrDep(vec_to_reader(depV));
        vector<char> rddV;
        smt->rdd->serialize_dyn(rddV);
        exec.setRdd(vec_to_reader(rddV));
    }
    ::capnp::writePackedMessageToFd(fd, builder);
}

unique_ptr<Task> recvExecution(::capnp::PackedFdMessageReader& message) {
    Execution::Reader exec = message.getRoot<Execution>();
    if (!exec.getIsShuffle()) {
        RDDBase* rdd = rdd_from_reader(exec.getRdd());
        FnBase* func = fn_from_reader(exec.getFuncOrDep());
        auto task = make_unique<ResultTask>(
            exec.getPartitionId(), rdd, func
        );
        return task;
    } else {
        RDDBase* rdd = rdd_from_reader(exec.getRdd());
        ShuffleDependencyBase* dep = dep_from_reader(exec.getFuncOrDep());
        auto task = make_unique<ShuffleMapTask>(
            exec.getPartitionId(), rdd, dep
        );
        return task;
    }
}

