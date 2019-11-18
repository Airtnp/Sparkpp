//
// Created by xiaol on 11/17/2019.
//

#ifndef SPARKPP_PTR_CAST_HPP
#define SPARKPP_PTR_CAST_HPP

#include <memory>

template<typename Derived, typename Base>
std::unique_ptr<Derived>
static_unique_ptr_cast(std::unique_ptr<Base>&& p) {
    auto d = static_cast<Derived *>(p.release());
    return std::unique_ptr<Derived>(d);
}

template<typename Derived, typename Base>
std::unique_ptr<Derived> dynamic_unique_ptr_cast(std::unique_ptr<Base>&& p) {
    if (Derived *result = dynamic_cast<Derived*>(p.get())) {
        p.release();
        return std::unique_ptr<Derived>(result);
    }
    return std::unique_ptr<Derived>(nullptr);
}

#endif //SPARKPP_PTR_CAST_HPP
