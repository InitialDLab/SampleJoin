#ifndef UTIL_INT128_SUPPORT_H
#define UTIL_INT128_SUPPORT_H

#if defined(__GNUC__) && defined(__GNUC_MINOR__) && \
    (__GNUC__ > 4 || ((__GNUC__ == 4) && (__GNUC_MINOR__ >= 6)))

#include <iostream>
#include <iomanip>
#include <cstdint>

typedef __int128 int128_t;
typedef unsigned __int128 uint128_t;

template<typename T>
inline constexpr T static_pow_of_10(unsigned power) {
    if (power == 0) return (T) 1;
    T x = static_pow_of_10<T>(power >> 1);
    x = x * x;
    if (power & 1) {
        x = x * 10;
    }
    return x;
}


inline std::ostream& operator<<(std::ostream &out, int128_t x) {
    using std::uint64_t;
    if (x < 0) {
        out << '-';
        if (x == (((int128_t) 1) << 127)) {
            return out << "170141183460469231731687303715884105728";
        }
        x = -x;
    }
    char old_fill = out.fill();
    auto old_width = out.width();
    if (x >= static_pow_of_10<int128_t>(38)) {
        out << (int)(x / static_pow_of_10<int128_t>(38));
        x = x % static_pow_of_10<int128_t>(38);
        out << std::setfill('0') << std::setw(19);
    }
    if (x >= static_pow_of_10<int128_t>(19)) {
        out << (uint64_t)(x / static_pow_of_10<int128_t>(19));
        x = x % static_pow_of_10<int128_t>(19);
        out << std::setfill('0') << std::setw(19);
    }
    return out << (uint64_t) x
        << std::setfill(old_fill)
        << std::setw(old_width);
}

inline std::ostream& operator<<(std::ostream &out, uint128_t x) {
    using std::uint64_t;
    char old_fill = out.fill();
    auto old_width = out.width();
    if (x >= static_pow_of_10<uint128_t>(38)) {
        out << (int)(x / static_pow_of_10<uint128_t>(38));
        x = x % static_pow_of_10<uint128_t>(38);
        out << std::setfill('0') << std::setw(19);
    }
    if (x >= static_pow_of_10<uint128_t>(19)) {
        out << (uint64_t)(x / static_pow_of_10<uint128_t>(19));
        x = x % static_pow_of_10<uint128_t>(19);
        out << std::setfill('0') << std::setw(19);
    }
    return out << (uint64_t) x
        << std::setfill(old_fill)
        << std::setw(old_width);
}

#else
#error "This compiler may not have __int128 (requires GCC >= 4.6)."
#endif

#endif // UTIL_INT128_SUPPORT_H

