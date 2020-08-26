#pragma once

#include "Table.h"
#include "DatabaseSharedTypes.h"
#include <string>
#include <vector>

// This is a very simple implementation of a CSV table where only specified
// columns are extracted and they must be or may be converted to int64.
class Int64CSVTable : public Table {
public:
    Int64CSVTable() {}

    ~Int64CSVTable() {}
    
    // Columns contains all the column numbers to be extracted, which
    // starts from 1. Empty lines are skipped in the file.
    //
    // The column numbers are not checked. Calling it with any
    // non-positive column number(s) is an undefined behavior.
    //
    // Returns false if any line has a field that is missing or
    // can't be parsed as an int64, or the file does not exist.
    // Otherwise, returns true.
    bool load(
        std::string file_name,
        std::vector<int> columns,
        char delimiter = ',');

    int column_count() override { return m_n_cols; }

    int64_t row_count() override { return m_n_rows; }

    DATABASE_DATA_TYPES getColumnTypes(int column) override {
        return INT64;
    }

    int64_t get_int64(int64_t row, int column) override {
        return m_data[column][row];
    }

    const std::vector<jfkey_t>::iterator get_key_iterator(int column) override {
        return m_data[column].begin();
    }

private:
    int m_n_cols;

    int64_t m_n_rows;
    
    // vector of columns
    std::vector<std::vector<int64_t>> m_data;
};

