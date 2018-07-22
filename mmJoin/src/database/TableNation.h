// Programmer: Robert Christensen
// email: robertc@cs.utah.edu
// -----------------------------
// implements a rigid implementation of the Nation table for the TPCH benchmark.
// this includes a schema as follows:
//
// NAME        | FIELD | TYPE
// N_NATIONKEY |   0   | Identifier (int)
// N_NAME      |   1   | Char[25]
// N_REGIONKEY |   2   | Identifier (int)
// N_COMMENT   |   3   | String[152]
#pragma once

#include <string>
#include <memory>
#include <vector>

#include "Table.h"

class Table_Nation :
    public Table
{
public:
    Table_Nation(std::string filename, size_t row_count);
    virtual ~Table_Nation();

    int column_count() {
        return 4;
    };

    int64_t row_count() {
        return mp_nationkey.size();
    }

    DATABASE_DATA_TYPES getColumnTypes(int column);

    int64_t get_int64(int64_t row, int column);
    const char* get_char(int64_t row, int column);

    std::shared_ptr < key_index > get_key_index(int column);

    const std::vector<jfkey_t>::iterator get_key_iterator(int column);
public:
    static const int N_NATIONKEY = 0;
    static const int N_NAME      = 1;
    static const int N_REGIONKEY = 2;
    static const int N_COMMENT   = 3;
private:
    void build_nation_index();
    void build_region_index();

    struct dummy {
        char str[26];
    };

    std::vector<jfkey_t> mp_nationkey;
    std::vector<dummy> mp_name;
    std::vector<jfkey_t> mp_regionkey;

    std::shared_ptr< key_index > mp_nationindex;
    std::shared_ptr< key_index > mp_regionindex;
};