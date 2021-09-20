#include "Int64CSVTable.h"
#include <algorithm>
#include <fstream>
#include <cstdlib>
#include <errno.h>
#include <cassert>
#include <iostream>

static void error(unsigned type) {
    std::cerr << "type=" << type << std::endl;
}

bool Int64CSVTable::load(std::vector<std::vector<double>>& table, unsigned num_attributes) {
    std::cerr << "[Int64CSVTable::load] start" << std::endl;
    m_data.resize(num_attributes);
    m_n_cols = num_attributes;
    m_n_rows = table.size();
    
    assert(num_attributes == table.front().size());
    for (const auto& entry: table) {
        std::cerr << "tuple:";
        for (unsigned index = 0, limit = num_attributes; index != limit; ++index) {
            std::cerr << entry[index] << ",";
            m_data[index].push_back(static_cast<int64_t>(entry[index]));
        }
        std::cerr << std::endl;
    }
    std::cerr << "[Int64CSVTable::load] stop" << std::endl;
    return true;
}

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
        error(1);
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
                error(2);
                return false;
            }
            while (csv_col < columns[data_col]) {
                auto p2 = line.find(delimiter, p);
                if (p2 == std::string::npos) {
                    // missing field
                    error(3);
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
            std::cerr << std::string(line.c_str() + p, line.c_str() + line.size()) << std::endl; 
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

