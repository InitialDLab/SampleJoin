 // Programer: Robert Christensen
// email: robertc@cs.utah.edu
// -----------------------------
// implements a rigid implementation of the Partsupp table for the TPCH benchmark.
// this includes a schema as follows:
//
// NAME          | FIELD | TYPE
// PS_PARTKEY    |   0   | Identifier (int)
// PS_SUPPKEY    |   1   | Identifier (int)
// PS_AVAILQTY   |   2   | int32
// PS_SUPPLYCOST |   3   | float
// PS_COMMENT    |   4   | string[199]

#pragma once

#include <string>
#include <memory>
#include <vector>

#include "Table.h"

class Table_Partsupp :
    public Table
{
public:
    Table_Partsupp(std::string filename, size_t row_count);
    virtual ~Table_Partsupp();

    int column_count() {
        return 5;
    };
    int64_t row_count() {
        return m_partskey.size();
    }

    DATABASE_DATA_TYPES getColumnTypes(int column);

    int32_t get_int32(int64_t row, int column);
    int64_t get_int64(int64_t row, int column);
    float get_float(int64_t row, int column);

    std::shared_ptr < key_index > get_key_index(int column);

    const std::vector<jfkey_t>::iterator get_key_iterator(int column);
public:
    static const int PS_PARTKEY    =   0;
    static const int PS_SUPPKEY    =   1;
    static const int PS_AVAILQTY   =   2;
    static const int PS_SUPPLYCOST =   3;
    static const int PS_COMMENT    =   4;
private:
    void build_parts_index();
    void build_supp_index();

    std::vector<jfkey_t> m_partskey;
    std::vector<jfkey_t> m_suppkey;
    std::vector<int32_t> m_availqty;
    std::vector<float> m_supplycost;

    std::shared_ptr< key_index > mp_partsindex;
    std::shared_ptr< key_index > mp_suppindex;
};