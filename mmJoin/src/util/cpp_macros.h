#ifndef UTIL_CPP_MACROS_H
#define UTIL_CPP_MACROS_H

#if __cplusplus >= 201703L
#define if_constexpr if constexpr
#else
#define if_constexpr if
#endif

#endif // UTIL_CPP_MACROS_H
