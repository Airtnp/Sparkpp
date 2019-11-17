#include <cstdlib>
#include <memory>
#include <iostream>
#include "spark_env.hpp"
#include "spark_context.hpp"
#include "serialize_capnp.hpp"
#include <capnp/message.h>
#include <capnp/serialize-packed.h>
#include <kj/io.h>

SparkEnv env;

int main(int argc, char** argv) {
    env.init(argc, argv, make_pair("127.0.0.1", 30001));
    auto sc = SparkContext{argc, argv};
    auto opt_st = TcpStream::connect("127.0.0.1", 27001);
    MockRDD rdd;
    auto l = FnWrapper{
        [](Iterator<int>& iter) mutable {
            int v = 0;
            while (true) {
                auto o = iter.next();
                if (o.is_initialized()) {
                    v += (int) o.value();
                } else {
                    break;
                }
            }
            return v;
        }
    };
    // kj::HandleOutputStream out{reinterpret_cast<HANDLE>(st.fd)};
    sendTask(opt_st->fd, l, rdd);

    auto result = recvData<Result>(opt_st->fd);
    using T = int;
    const T* v = reinterpret_cast<const T*>(result.asBytes().begin());
    std::cout << *v << '\n';
    return 0;
}