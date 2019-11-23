#include <iostream>
#include <chrono>
#include "spark_env.hpp"
#include "spark_context.hpp"
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
    auto sc = SparkContext{argc, argv, masterAddr, slaveAddrs};
    // https://cts.instructure.com/courses/172185/pages/word-count-examples
    vector<string> lines = {
            "and of I friend great spark master follower",
            "of I friend great spark master follower and",
            "I friend great spark master follower and of",
            "friend great spark master follower and of I",
            "great spark master follower and of I friend",
            "spark master follower and of I friend great",
            "master follower and of I friend great spark",
            "follower and of I friend great spark master",
            "and of I friend great spark master follower",
            "spark master follower and of I friend great",
            "master follower and of I friend great spark",
            "follower and of I friend great spark master",
            "and of I friend great spark master follower",
            "spark master follower and of I friend great",
            "master follower and of I friend great spark",
            "follower and of I friend great spark master",
            "and of I friend great spark master follower",
            "spark master follower and of I friend great",
            "master follower and of I friend great spark",
            "follower and of I friend great spark master",
            "and of I friend great spark master follower",
            "spark master follower and of I friend great",
            "master follower and of I friend great spark",
            "follower and of I friend great spark master",
            "and of I friend great spark master follower",
            "spark master follower and of I friend great",
            "master follower and of I friend great spark",
            "follower and of I friend great spark master",
            "and of I friend great spark master follower",
            "spark master follower and of I friend great",
            "master follower and of I friend great spark",
            "follower and of I friend great spark master",
            "and of I friend great spark master follower",
            "spark master follower and of I friend great",
            "master follower and of I friend great spark",
            "follower and of I friend great spark master",
            "and of I friend great spark master follower",
            "spark master follower and of I friend great",
            "master follower and of I friend great spark",
            "follower and of I friend great spark master",
            "and of I friend great spark master follower",
            "spark master follower and of I friend great",
            "master follower and of I friend great spark",
            "follower and of I friend great spark master",
            "and of I friend great spark master follower"
    };
    auto t_begin = steady_clock::now();
    auto rdd = sc.parallelize(lines, lines.size());
    auto rdd2 = rdd.flatMap([](string&& s) {
        vector<string> v;
        boost::algorithm::split(v, move(s), boost::algorithm::is_space());
        return v;
    });
    rdd2.cache();
    auto rdd3 = rdd2.mapPair([](string&& w) -> pair<string, int> {
        return make_pair(move(w), 1);
    });
    auto rdd4 = rdd3.reduceByKey([](int a, int b) {
        return a + b;
    }, 2);
    auto result = rdd4.collect();
    auto t_end = steady_clock::now();
    for (auto& [k, v] : result) {
        std::cout << k << ": " << v << '\n';
    }
    std::cout << "Elapsed time in milliseconds: "
              << duration_cast<milliseconds>(t_end - t_begin).count() << " ms\n";
    return 0;
}