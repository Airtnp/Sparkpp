//
// Created by xiaol on 11/5/2019.
//

#ifndef SPARKPP_SPARK_CONTEXT_HPP
#define SPARKPP_SPARK_CONTEXT_HPP

#include <utility>

#include "common.hpp"
#include "executor.hpp"
#include "scheduler/dag_scheduler.hpp"

enum class SparkContextType {
    Local,
    Distributed,
    NumOfSparkContextType
};

enum class SparkDistributeType {
    Master,
    Slave,
    NumOfSparkDistributeType
};

struct SparkConfig {
    SparkContextType mode;
    SparkDistributeType type;
    uint16_t port;
    vector<pair<string, uint16_t>> addr;
    SparkConfig(
            SparkContextType mode_,
            SparkDistributeType type_,
            uint16_t port_,
            vector<pair<string, uint16_t>> addr_
    ) : mode{mode_}, type{type_}, port{port_}, addr{move(addr_)} {}
};

struct SparkContext {
    std::atomic<int> nextRddId{0};
    std::atomic<int> nextShuffleId{0};
    SparkConfig config;
    vector<pair<string, uint16_t>> address;
    DAGScheduler scheduler;
    // scheduler

    int newRddId() {
        return nextRddId.fetch_add(1);
    }
    int newShuffleId() {
        return nextShuffleId.fetch_add(1);
    }

    SparkConfig getConfig(char** argv) {
        auto ty = string{argv[1]};
        if (ty == "master") {
            return SparkConfig{
                    SparkContextType::Distributed,
                    SparkDistributeType::Master,
                    30001,
                    {{"127.0.0.1", 27001}}
            };
        } else {
            return SparkConfig{
                    SparkContextType::Distributed,
                    SparkDistributeType::Slave,
                    27001,
                    {{"localhost", 30001}}
            };
        }
    }

    SparkContext(int argc, char** argv) : config{getConfig(argv)}, scheduler{config.addr} {
        switch (config.mode) {
            case SparkContextType::Distributed: {
                switch (config.type) {
                    case SparkDistributeType::Master: {
                        address = move(config.addr);
                        break;
                    }
                    case SparkDistributeType::Slave: {
                        auto executor = Executor{config.addr[0], config.port};
                        executor.run();
                        exit(0);
                    }
                    default: __builtin_unreachable();
                }
                break;
            }
            case SparkContextType::Local: {
                break;
            }
            default: __builtin_unreachable();
        }
    }
};

#endif //SPARKPP_SPARK_CONTEXT_HPP
