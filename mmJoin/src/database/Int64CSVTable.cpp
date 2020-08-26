#include "Int64CSVTable.h"
#include <algorithm>
#include <fstream>
#include <cstdlib>
#include <errno.h>

bool Int64CSVTable::load(
    std::string file_name,
    std::vector<int> columns,
    char delimiter) {
    
    std::sort(columns.begin(), columns.end());
    m_n_cols = (int) columns.size();
    m_n_rows = 0;
    m_data.resize(m_n_cols);

    std::ifstream fin(file_name);
    if (!fin) {
        return false;
    }

    std::string line;
    while (std::getline(fin, line)) {
        if (line.empty()) continue;
        int data_col = 0;
        int csv_col = 1;
        std::string::size_type p = 0;
        while (data_col < m_n_cols) {
            if (p >= line.length()) {
                // missing field
                return false;
            }
            while (csv_col < columns[data_col]) {
                auto p2 = line.find(delimiter, p);
                if (p2 == std::string::npos) {
                    // missing field
                    return false;
                }
                p = p2 + 1;
                ++csv_col;
            }
            
            // found the current column
            auto p2 = line.find(delimiter, p);
            if (p2 == std::string::npos) {
                p2 = line.length();
            }

            line[p2] = '\0';
            char *str_end;
            unsigned long long value = std::strtoull(
                line.c_str() + p, &str_end, 0);
            if (errno == ERANGE || errno == EINVAL ||
                    str_end != line.c_str() + p2) {
                // invalid conversion
                return false;
            }
            p = p2 + 1;
            ++csv_col;

            m_data[data_col].push_back((int64_t) value);
            ++data_col;
        }
        ++m_n_rows;
    }

    return true;
}

