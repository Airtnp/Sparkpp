@0xad095e939d603738;

struct Execution {
    isShuffle @0: Bool;
    partitionId @1: UInt32;
    rdd @2: Data;
    funcOrDep @3: Data;
}

struct Result {
    msg @0: Data;
}

struct Message {
    msg @0: Data;
}