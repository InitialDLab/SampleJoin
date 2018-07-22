// Programmer: Robert Christensen
// email: robertc@cs.utah.edu
// ------------------------------
// implements a rigid implementation of the customer table for the TPCH benchmark
// this includes a schema as follows:
//
// NAME             |  FIELD | TYPE
// C_CUSTKEY        |    0   | Identifier (int)
// C_NAME           |    1   | String[25]
// C_ADDRESS        |    2   | String[40]
// C_NATIONKEY      |    3   | Identifier
// C_PHONE          |    4   | CHAR[15]
// C_ACCTBAL        |    5   | real
// C_MKTSEGMENT     |    6   | CHAR[10]
// C_COMMENT        |    7   | String[117]
#pragma once

#include <string>
#include <memory>
#include <vector>

#include "Table.h"

class Table_Customer :
    public Table
{
public:
    Table_Customer(std::string filename, size_t row_count);
    virtual ~Table_Customer();

    int column_count() {
        return 8;
    }

    int64_t row_count() {
        return m_custkey.size();
    }

    DATABASE_DATA_TYPES getColumnTypes(int column);

    int64_t get_int64(int64_t row, int column);
    const char* get_char(int64_t row, int column);
    float get_float(int64_t row, int column);

    std::shared_ptr< key_index > get_key_index(int column);

    std::shared_ptr< float_index > get_float_index(int column);

    const std::vector<jfkey_t>::iterator get_key_iterator(int column);
public:
    static const int C_CUSTKEY        =    0;
    static const int C_NAME           =    1;
    static const int C_ADDRESS        =    2;
    static const int C_NATIONKEY      =    3;
    static const int C_PHONE          =    4;
    static const int C_ACCTBAL        =    5;
    static const int C_MKTSEGMENT     =    6;
    static const int C_COMMENT        =    7;
private:
    void build_cust_index();
    void build_nation_index();
    void build_acctbal_index();

    struct phone_dummy{
        char str[16];
    };
    struct mktsegment_dummy{
        char str[11];
    };

    std::vector<jfkey_t> m_custkey;
    std::vector<jfkey_t> m_nationkey;
    std::vector<phone_dummy> m_phone;
    std::vector<float> m_acctbal;
    std::vector<mktsegment_dummy> m_mktsegment;

    std::shared_ptr< key_index > mp_custindex;
    std::shared_ptr< key_index > mp_nationindex;

    std::shared_ptr< float_index > mp_acctbalindex;
};