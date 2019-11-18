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
    vector<int> values = {1, 2, 3, 4, 5, 6, 7};
    auto rdd = sc.parallelize(values, 1);
    auto v = rdd.reduce([](int x, int y) {
        return x + y;
    });
    std::cout << v << '\n';
    return 0;
}