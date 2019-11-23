#include <iostream>
#include <fstream>
#include <chrono>
#include "spark_env.hpp"
#include "spark_context.hpp"
#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <fmt/format.h>

using namespace std::chrono;

SparkEnv env;

int main(int argc, char** argv) {
    addr_t masterAddr = make_pair("18.188.215.139", 25544);
    vector<addr_t> slaveAddrs = {
            {"18.218.54.64", 24457},
            {"3.17.81.214", 24457}
    };
    env.init(argc, argv, masterAddr);
    // spark-shell --master
    auto sc = SparkContext{argc, argv, masterAddr, slaveAddrs};
    vector<size_t> files(40);
    // val files = 0 until n;
    std::iota(files.begin(), files.end(), 0);

    // spark.time(sc
    //      .parallelize(files, files.length)
    //      .flatMap(idx => Source.fromFile(f"/home/ubuntu/Sparkpp/examples/input/input_$idx")
    //                              .getLines
    //                              .flatMap(l => l.split(' ')))
    //      .map(s => (s, 1))
    //      .reduceByKey(_ + _, 2)
    //      .collect())
    auto t_begin = steady_clock::now();
    auto rdd = sc.parallelize(files, files.size());
    auto rdd2 = rdd.flatMap([](size_t v) noexcept {
        auto path = fmt::format("/home/ubuntu/Sparkpp/examples/input/input_{}", v);
        std::ifstream ifs{path};
        vector<vector<string>> words;
        for (string line; getline(ifs, line);) {
            vector<string> w;
            boost::algorithm::split(w, move(line), [](const char c) {
                return c == ' ';
            });
            words.push_back(move(w));
        }
        return flatten(words);
    });
    auto rdd3 = rdd2.mapPair([](string&& w) noexcept -> pair<string, int> {
        return make_pair(move(w), 1);
    });
    auto rdd4 = rdd3.reduceByKey([](int a, int b) noexcept {
        return a + b;
    }, 8);
    auto result = rdd4.collect();
    auto t_end = steady_clock::now();

    std::cout << result.size() << '\n';
    std::cout << "Elapsed time in milliseconds: "
              << duration_cast<milliseconds>(t_end - t_begin).count() << " ms\n";
    return 0;
}