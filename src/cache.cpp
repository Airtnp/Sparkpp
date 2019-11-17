//
// Created by xiaol on 11/13/2019.
//

#include <cache.hpp>

KeySpace BoundedMemoryCache::newKeySpace() {
    size_t keySpaceId = nextKeySpaceId.fetch_add(1);
    return KeySpace{
        .cache = *this,
        .keySpaceId = keySpaceId
    };
}

