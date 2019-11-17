@0xad095e939d603738;

struct Execution {
    isShuffle @0: Bool;
    partitionId @1: UInt32;
    func @2: Data;
    rdd @3: Data;
    dep @4: Data;
}

struct Result {
    msg @0: Data;
}

struct Message {
    msg @0: Data;
}