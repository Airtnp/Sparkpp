//
// Created by xiaol on 11/15/2019.
//

#ifndef SPARKPP_MACROS_HPP
#define SPARKPP_MACROS_HPP

#define MACRO_EXPAND(...) __VA_ARGS__

#define MACRO_CONCAT_IMPL(A, B) A##_##B
#define MACRO_CONCAT(A, B) MACRO_CONCAT_IMPL(A, B)

#define SN_REVERSE_SEQ_N() \
    20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10 \
    10,  9,  8,  7,  6,  5,  4,  3,  2,  1,  0

#define SN_SEQ_N( \
    _1, _2, _3, _4, _5, _6, _7, _8, _9, _10, \
    _11, _12, _13, _14, _15, _16, _17, _18, _19, _20, N, ...) N

#define SN_GET_ARG_IMPL(...) MACRO_EXPAND(SN_SEQ_N(__VA_ARGS__))
#define SN_GET_ARG_N(...) SN_GET_ARG_IMPL(__VA_ARGS__, SN_REVERSE_SEQ_N())


#define SN_REGISTER_ARG_LIST_1(op, arg, ...) op(arg)
#define SN_REGISTER_ARG_LIST_2(op, arg, ...) op(arg), MACRO_EXPAND(SN_REGISTER_ARG_LIST_1(op, __VA_ARGS__))
#define SN_REGISTER_ARG_LIST_3(op, arg, ...) op(arg), MACRO_EXPAND(SN_REGISTER_ARG_LIST_2(op, __VA_ARGS__))
#define SN_REGISTER_ARG_LIST_4(op, arg, ...) op(arg), MACRO_EXPAND(SN_REGISTER_ARG_LIST_3(op, __VA_ARGS__))
#define SN_REGISTER_ARG_LIST_5(op, arg, ...) op(arg), MACRO_EXPAND(SN_REGISTER_ARG_LIST_4(op, __VA_ARGS__))
#define SN_REGISTER_ARG_LIST_6(op, arg, ...) op(arg), MACRO_EXPAND(SN_REGISTER_ARG_LIST_5(op, __VA_ARGS__))
#define SN_REGISTER_ARG_LIST_7(op, arg, ...) op(arg), MACRO_EXPAND(SN_REGISTER_ARG_LIST_6(op, __VA_ARGS__))
#define SN_REGISTER_ARG_LIST_8(op, arg, ...) op(arg), MACRO_EXPAND(SN_REGISTER_ARG_LIST_7(op, __VA_ARGS__))
#define SN_REGISTER_ARG_LIST_9(op, arg, ...) op(arg), MACRO_EXPAND(SN_REGISTER_ARG_LIST_8(op, __VA_ARGS__))
#define SN_REGISTER_ARG_LIST_10(op, arg, ...) op(arg), MACRO_EXPAND(SN_REGISTER_ARG_LIST_9(op, __VA_ARGS__))
#define SN_REGISTER_ARG_LIST_11(op, arg, ...) op(arg), MACRO_EXPAND(SN_REGISTER_ARG_LIST_10(op, __VA_ARGS__))
#define SN_REGISTER_ARG_LIST_12(op, arg, ...) op(arg), MACRO_EXPAND(SN_REGISTER_ARG_LIST_11(op, __VA_ARGS__))
#define SN_REGISTER_ARG_LIST_13(op, arg, ...) op(arg), MACRO_EXPAND(SN_REGISTER_ARG_LIST_12(op, __VA_ARGS__))
#define SN_REGISTER_ARG_LIST_14(op, arg, ...) op(arg), MACRO_EXPAND(SN_REGISTER_ARG_LIST_13(op, __VA_ARGS__))
#define SN_REGISTER_ARG_LIST_15(op, arg, ...) op(arg), MACRO_EXPAND(SN_REGISTER_ARG_LIST_14(op, __VA_ARGS__))
#define SN_REGISTER_ARG_LIST_16(op, arg, ...) op(arg), MACRO_EXPAND(SN_REGISTER_ARG_LIST_15(op, __VA_ARGS__))
#define SN_REGISTER_ARG_LIST_17(op, arg, ...) op(arg), MACRO_EXPAND(SN_REGISTER_ARG_LIST_16(op, __VA_ARGS__))
#define SN_REGISTER_ARG_LIST_18(op, arg, ...) op(arg), MACRO_EXPAND(SN_REGISTER_ARG_LIST_17(op, __VA_ARGS__))
#define SN_REGISTER_ARG_LIST_19(op, arg, ...) op(arg), MACRO_EXPAND(SN_REGISTER_ARG_LIST_18(op, __VA_ARGS__))
#define SN_REGISTER_ARG_LIST_20(op, arg, ...) op(arg), MACRO_EXPAND(SN_REGISTER_ARG_LIST_19(op, __VA_ARGS__))

#define SN_REGISTER_ID(ID) ID

#define SN_REGISTER_ARG_LIST(N, op, arg, ...) \
    MACRO_CONCAT(SN_REGISTER_ARG_LIST, N)(op, arg, __VA_ARGS__)

#define SN_SEPERATOR ;
#define SN_CONCAT_INIT_LIST_1(element, ...) ar & element
#define SN_CONCAT_INIT_LIST_2(element, ...) ar & element SN_SEPERATOR MACRO_EXPAND(SN_CONCAT_INIT_LIST_1(__VA_ARGS__))
#define SN_CONCAT_INIT_LIST_3(element, ...) ar & element SN_SEPERATOR MACRO_EXPAND(SN_CONCAT_INIT_LIST_2(__VA_ARGS__))
#define SN_CONCAT_INIT_LIST_4(element, ...) ar & element SN_SEPERATOR MACRO_EXPAND(SN_CONCAT_INIT_LIST_3(__VA_ARGS__))
#define SN_CONCAT_INIT_LIST_5(element, ...) ar & element SN_SEPERATOR MACRO_EXPAND(SN_CONCAT_INIT_LIST_4(__VA_ARGS__))
#define SN_CONCAT_INIT_LIST_6(element, ...) ar & element SN_SEPERATOR MACRO_EXPAND(SN_CONCAT_INIT_LIST_5(__VA_ARGS__))
#define SN_CONCAT_INIT_LIST_7(element, ...) ar & element SN_SEPERATOR MACRO_EXPAND(SN_CONCAT_INIT_LIST_6(__VA_ARGS__))
#define SN_CONCAT_INIT_LIST_8(element, ...) ar & element SN_SEPERATOR MACRO_EXPAND(SN_CONCAT_INIT_LIST_7(__VA_ARGS__))
#define SN_CONCAT_INIT_LIST_9(element, ...) ar & element SN_SEPERATOR MACRO_EXPAND(SN_CONCAT_INIT_LIST_8(__VA_ARGS__))
#define SN_CONCAT_INIT_LIST_10(element, ...) ar & element SN_SEPERATOR MACRO_EXPAND(SN_CONCAT_INIT_LIST_9(__VA_ARGS__))
#define SN_CONCAT_INIT_LIST_11(element, ...) ar & element SN_SEPERATOR MACRO_EXPAND(SN_CONCAT_INIT_LIST_10(__VA_ARGS__))
#define SN_CONCAT_INIT_LIST_12(element, ...) ar & element SN_SEPERATOR MACRO_EXPAND(SN_CONCAT_INIT_LIST_11(__VA_ARGS__))
#define SN_CONCAT_INIT_LIST_13(element, ...) ar & element SN_SEPERATOR MACRO_EXPAND(SN_CONCAT_INIT_LIST_12(__VA_ARGS__))
#define SN_CONCAT_INIT_LIST_14(element, ...) ar & element SN_SEPERATOR MACRO_EXPAND(SN_CONCAT_INIT_LIST_13(__VA_ARGS__))
#define SN_CONCAT_INIT_LIST_15(element, ...) ar & element SN_SEPERATOR MACRO_EXPAND(SN_CONCAT_INIT_LIST_14(__VA_ARGS__))
#define SN_CONCAT_INIT_LIST_16(element, ...) ar & element SN_SEPERATOR MACRO_EXPAND(SN_CONCAT_INIT_LIST_15(__VA_ARGS__))
#define SN_CONCAT_INIT_LIST_17(element, ...) ar & element SN_SEPERATOR MACRO_EXPAND(SN_CONCAT_INIT_LIST_16(__VA_ARGS__))
#define SN_CONCAT_INIT_LIST_18(element, ...) ar & element SN_SEPERATOR MACRO_EXPAND(SN_CONCAT_INIT_LIST_17(__VA_ARGS__))
#define SN_CONCAT_INIT_LIST_19(element, ...) ar & element SN_SEPERATOR MACRO_EXPAND(SN_CONCAT_INIT_LIST_18(__VA_ARGS__))
#define SN_CONCAT_INIT_LIST_20(element, ...) ar & element SN_SEPERATOR MACRO_EXPAND(SN_CONCAT_INIT_LIST_19(__VA_ARGS__))

#define SN_BOOST_SERIALIZE_MEMBERS_IMPL(N, ...) MACRO_EXPAND(MACRO_CONCAT(SN_CONCAT_INIT_LIST, N)(__VA_ARGS__))

#define SN_BOOST_SERIALIZE_MEMBERS_IN(...) \
friend class boost::serialization::access; \
template <class Archive> \
void serialize(Archive& ar, const unsigned int) { \
    SN_BOOST_SERIALIZE_MEMBERS_IMPL(SN_GET_ARG_N(__VA_ARGS__), __VA_ARGS__); \
} \

#define SN_BOOST_SERIALIZE_EMPTY() \
friend class boost::serialization::access; \
template <class Archive> \
void serialize(Archive&, const unsigned int) {} \





#endif //SPARKPP_MACROS_HPP
