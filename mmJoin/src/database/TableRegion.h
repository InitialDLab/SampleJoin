// Programmer: Robert Christensen
// email: robertc@cs.utah.edu
// -----------------------------
// implements a rigid implementation of the Region table for the TPCH benchmark.
// this includes a schema as follows:
//
// NAME        | FIELD | TYPE
// R_REGIONKEY |   0   | Identifier (int)
// R_NAME      |   1   | Char[25]
// R_COMMENT   |   2   | String

#pragma once

#include <string>
#include <memory>
#include <vector>
#include <array>

#include "Table.h"

class Table_Region :
    public Table
{
public:
    Table_Region(std::string filename, size_t row_count);
    virtual ~Table_Region();

    int column_count() {
        return 3;
    };

    int64_t row_count() {
        return mp_regionkey.size();
    }

    DATABASE_DATA_TYPES getColumnTypes(int column);

    int64_t get_int64(int64_t row, int column);
    const char* get_char(int64_t row, int column);

    std::shared_ptr < key_index > get_key_index(int column);

    const std::vector<jfkey_t>::iterator get_key_iterator(int column);
public:
    static const int R_REGIONKEY =   0;
    static const int R_NAME      =   1;
    static const int R_COMMENT   =   2;
private:
    void build_Region_index();
    // this is needed because I was unable to created a vector<char[26]>
    struct dummy{
        char str[26];
    };

    std::vector<jfkey_t> mp_regionkey;
    std::vector<dummy> mp_name;

    std::shared_ptr< key_index > mp_regionindex;
};
