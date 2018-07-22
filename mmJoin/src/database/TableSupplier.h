// Programer: Robert Christensen
// email: robertc@cs.utah.edu
// -----------------------------
// implements a rigid implementation of the Nation table for the TPCH benchmark.
// this includes a schema as follows:
//
// NAME        | FIELD | TYPE
// S_SUPPKEY   |   0   | Identifier (int)
// S_NAME      |   1   | Char[25]
// S_ADDRESS   |   2   | String[40]
// S_NATIONKEY |   3   | Identifier (int)
// S_PHONE     |   4   | Char[15]
// S_ACCTBAL   |   5   | decimal (float?)
// S_COMMENT   |   6   | String[101]

#pragma once

#include <string>
#include <memory>
#include <vector>

#include "Table.h"

class Table_Supplier :
    public Table
{
public:
    Table_Supplier(std::string filename, size_t row_count);
    virtual ~Table_Supplier();

    int column_count() {
        return 6;
    };
    int64_t row_count() {
        return m_suppkey.size();
    }

    DATABASE_DATA_TYPES getColumnTypes(int column);

    int64_t get_int64(int64_t row, int column);
    float get_float(int64_t row, int column);
    const char* get_char(int64_t row, int column);

    std::shared_ptr < key_index > get_key_index(int column);

    std::shared_ptr< float_index > get_float_index(int column);

    const std::vector<jfkey_t>::iterator get_key_iterator(int column);
public:
    static const int S_SUPPKEY   = 0; 
    static const int S_NAME      = 1;
    static const int S_ADDRESS   = 2;
    static const int S_NATIONKEY = 3;
    static const int S_PHONE     = 4;
    static const int S_ACCTBAL   = 5;
    static const int S_COMMENT   = 6;
private:
    void build_Supp_index();
    void build_Nation_index();

    void build_acctbal_index();

    struct dummyName{
        char str[26];
    };

    struct dummyPhone{
        char str[16];
    };

    std::vector<jfkey_t>      m_suppkey;
    std::vector<dummyName>  m_name;
    std::vector<jfkey_t>      m_nationkey;
    std::vector<dummyPhone> m_phone;
    std::vector<float>      m_acctbal;

    std::shared_ptr< key_index > m_suppindex;
    std::shared_ptr< key_index > m_nationindex;

    std::shared_ptr< float_index > mp_acctbalindex;
};