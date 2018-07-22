// Programmer: Robert Christensen
// email: robertc@cs.utah.edu
// -------------------------------
// implements a rigid implementation of the customer table for the TPCH benchmark
// this includes a schema as follows:
//
// NAME            | FIELD | TYPE
// O_ORDERKEY      |   0   | Identifier (int)
// O_CUSTKEY       |   1   | Identifier
// O_ORDERSTATUS   |   2   | char
// O_TOTALPRICE    |   3   | real
// O_ORDERDATE     |   4   | date (seconds since Epoch)
// O_ORDERPRIORITY |   5   | char[15]
// O_CLERK         |   6   | char[15]
// O_SHIPPRIORITY  |   7   | int
// O_COMMENT       |   8   | string[79]
#pragma once

#include <string>
#include <memory>
#include <vector>

#include "Table.h"

class Table_Orders :
    public Table
{
public:
    Table_Orders(std::string filename, size_t row_count);
    virtual ~Table_Orders();

    int column_count() {
        return 9;
    }

    int64_t row_count() {
        return m_orderkey.size();
    }

    DATABASE_DATA_TYPES getColumnTypes(int column);

    int64_t get_int64(int64_t row, int column);
    const char* get_char(int64_t row, int column);
    float get_float(int64_t row, int column);

    std::shared_ptr< key_index > get_key_index(int column);
    std::shared_ptr< int_index > get_int_index(int column);

    const std::vector<jfkey_t>::iterator get_key_iterator(int column);
public:
    static const int O_ORDERKEY      = 0;
    static const int O_CUSTKEY       = 1;
    static const int O_ORDERSTATUS   = 2;
    static const int O_TOTALPRICE    = 3;
    static const int O_ORDERDATE     = 4;
    static const int O_ORDERPRIORITY = 5;
    static const int O_CLERK         = 6;
    static const int O_SHIPPRIORITY  = 7;
    static const int O_COMMENT       = 8;
private:
    void build_orders_index();
    void build_cust_index();
    void build_orderdate_index();

    struct priority_dummy{
        char str[16];
    };
    struct clerk_dummy{
        char str[16];
    };

    std::vector<jfkey_t> m_orderkey;
    std::vector<jfkey_t> m_custkey;
    std::vector<char> m_orderstatus;
    std::vector<float> m_totalprice;
    std::vector<jefastTime_t> m_orderdate;
    std::vector<priority_dummy> m_orderpriority;
    std::vector<clerk_dummy> m_clerk;
    std::vector<int> m_shippriority;

    std::shared_ptr< key_index > mp_orderindex;
    std::shared_ptr< key_index > mp_custindex;
    std::shared_ptr< int_index > mp_orderdateindex;
};