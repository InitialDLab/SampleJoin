// Programmer: Robert Christensen
// email: robertc@cs.utah.edu
// ------------------------------
// implements a rigid implementation of the customer table for the TPCH benchmark
// this includes a schema as follows:
//
// NAME             | FIELD | TYPE
// L_ORDERKEY       |   0   | Identifier
// L_PARTKEY        |   1   | Identifier
// L_SUPPKEY        |   2   | Identifier
// L_LINENUMBER     |   3   | Integer
// L_QUANTITY       |   4   | real
// L_EXTENDEDPRICE  |   5   | real
// L_DISCOUNT       |   6   | real
// L_TAX            |   7   | real
// L_RETURNFLAG     |   8   | char[1]
// L_LINESTATUS     |   9   | char[1]
// L_SHIPDATE       |  10   | date (seconds since Epoch)
// L_COMMITDATE     |  11   | date (seconds since Epoch)
// L_RECEIPTDATE    |  12   | date (seconds since Epoch)
// L_SHIPINSTRUCT   |  13   | char[25]
// L_SHIPMODE       |  14   | char[10]
// L_COMMENT        |  15   | string[44]
#pragma once

#include <string>
#include <memory>
#include <vector>

#include "Table.h"

class Table_Lineitem :
    public Table
{
public:
    Table_Lineitem(std::string filename, size_t row_count);
    virtual ~Table_Lineitem();

    int column_count() {
        return 16;
    }

    int64_t row_count() {
        return m_orderkey.size();
    }

    DATABASE_DATA_TYPES getColumnTypes(int column);

    int64_t get_int64(int64_t row, int column);
    int32_t get_int32(int64_t row, int column);
    const char* get_char(int64_t row, int column);
    float get_float(int64_t row, int column);

    std::shared_ptr< key_index > get_key_index(int column);

    std::shared_ptr< int_index > get_int_index(int column);

    const std::vector<jfkey_t>::iterator get_key_iterator(int column);
public:
    static const int L_ORDERKEY       =   0;
    static const int L_PARTKEY        =   1;
    static const int L_SUPPKEY        =   2;
    static const int L_LINENUMBER     =   3;
    static const int L_QUANTITY       =   4;
    static const int L_EXTENDEDPRICE  =   5;
    static const int L_DISCOUNT       =   6;
    static const int L_TAX            =   7;
    static const int L_RETURNFLAG     =   8;
    static const int L_LINESTATUS     =   9;
    static const int L_SHIPDATE       =  10;
    static const int L_COMMITDATE     =  11;
    static const int L_RECEIPTDATE    =  12;
    static const int L_SHIPINSTRUCT   =  13;
    static const int L_SHIPMODE       =  14;
    static const int L_COMMENT        =  15;
private:
    void build_order_index();
    void build_part_index();
    void build_supp_index();

    void build_shipdate_index();
    void build_commitdate_index();
    void build_receiptdate_index();

    struct shipinstruct_dummy{
        char str[26];
    };
    struct shipmode_dummy{
        char str[11];
    };

    std::vector<jfkey_t> m_orderkey;
    std::vector<jfkey_t> m_partkey;
    std::vector<jfkey_t> m_suppkey;
    std::vector<int>   m_linenumber;
    std::vector<float> m_quantity;
    std::vector<float> m_extendedprice;
    std::vector<float> m_discount;
    std::vector<float> m_tax;
    std::vector<char>  m_returnflag;
    std::vector<char>  m_linestatus;
    std::vector<jefastTime_t> m_shipdate;
    std::vector<jefastTime_t> m_commitdate;
    std::vector<jefastTime_t> m_receiptdate;
    std::vector<shipinstruct_dummy> m_shipinstruct;
    std::vector<shipmode_dummy> m_shipmode;

    std::shared_ptr< key_index > mp_orderindex;
    std::shared_ptr< key_index > mp_partindex;
    std::shared_ptr< key_index > mp_suppindex;

    std::shared_ptr< int_index > mp_shipdateIndex;
    std::shared_ptr< int_index > mp_commitdateIndex;
    std::shared_ptr< int_index > mp_receiptdateIndex;
};