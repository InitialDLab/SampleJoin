#pragma once

#include <string>
#include <algorithm>
#include <type_traits>

struct datetime_t{};

template<typename T>
struct real_type {
    typedef T type;
};

template<>
struct real_type<datetime_t> {
    typedef time_t type;
};

template<typename T>
typename real_type<T>::type fromString(const std::string& line, std::string::size_type begin, std::string::size_type end);

template<>
inline std::string fromString<std::string>(const std::string& line, std::string::size_type begin, std::string::size_type end) {
    return line.substr(begin, end - begin);
}

template<>
inline uint64_t fromString<uint64_t>(const std::string &line, std::string::size_type begin, std::string::size_type end) {
    return std::stoull(line.substr(begin, end - begin));
}

template<>
inline int fromString<int>(const std::string &line, std::string::size_type begin, std::string::size_type end) {
    return std::stoi(line.substr(begin, end - begin));
}

template<>
inline double fromString<double>(const std::string &line, std::string::size_type begin, std::string::size_type end) {
    return std::stod(line.substr(begin, end - begin));
}

template<>
inline time_t fromString<datetime_t>(const std::string &line, std::string::size_type begin, std::string::size_type end) {
    tm t;
    t.tm_sec = 0; t.tm_min = 0; t.tm_hour = 0; t.tm_isdst = -1;
    strptime(line.substr(begin, end - begin).c_str(), "%Y-%m-%d", &t);
    time_t t2 = mktime(&t);
    return t2 / (60ll * 60 * 24);
}

template<unsigned v>
using Colno = std::integral_constant<unsigned, v>;

template<typename ...T>
struct ColumnExtractorImpl {};

template<typename R, unsigned C, typename ...T>
struct ColumnExtractorImpl<R, Colno<C>, T...> {
    template<typename ...U>
    auto operator()(
            char delim,
            const std::string& line,
            unsigned curcol,
            std::string::size_type i,
            U... cols) const {
        
        while (curcol < C) {
            i = line.find(delim, i);
            i += 1;
            curcol += 1;
        }
        auto j = line.find(delim, i);
        if (j == std::string::npos) j = line.length();
        return ColumnExtractorImpl<T...>()(delim, line, curcol + 1, j + 1, cols...,
                fromString<R>(line, i, j));
    }
};

template<>
struct ColumnExtractorImpl<> {
    template<typename ...U>
    std::tuple<U...> operator()(
            char delim,
            const std::string& line,
            unsigned curcol,
            std::string::size_type i,
            U... cols) const {

        return std::make_tuple(cols...);
    }
};

template<typename ...T>
struct ColumnExtractor {
    auto operator()(char delim, const std::string &line) const {
        return ColumnExtractorImpl<T...>()(delim, line, 1u, (std::string::size_type) 0);
    }
};

