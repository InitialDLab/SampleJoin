// Programmer: Robert Christensen
// email: robertc@cs.utah.edu
// -------------------------------
// implememnts a rigid implememtation of the part table for the TPCH benchmark
// this includes a schema as follows:
//
// NAME          | FIELD | TYPE
// P_PARTKEY     |   0   | Identifier
// P_NAME        |   1   | string[55]
// P_MFGR        |   2   | char[25]
// P_BRAND       |   3   | char[10]
// P_TYPE        |   4   | string[25]
// P_SIZE        |   5   | int
// P_CONTAINER   |   6   | char[10]
// P_RETAILPRICE |   7   | float
// P_COMMENT     |   8   | string[23]
#pragma once

#include <string>
#include <memory>
#include <vector>

#include "Table.h"

class Table_Part :
    public Table
{
public:
    Table_Part(std::string filename, size_t row_count);
    virtual ~Table_Part();
    
    int column_count() {
        return 9;
    }

    int64_t row_count() {
        return m_partkey.size();
    }

    DATABASE_DATA_TYPES getColumnTypes(int column);

    int64_t get_int64(int64_t row, int column);
    float get_float(int64_t row, int column);

    std::shared_ptr< key_index > get_key_index(int column);

    const std::vector<jfkey_t>::iterator get_key_iterator(int column);
public:
    static const int P_PARTKEY     = 0;
    static const int P_NAME        = 1;
    static const int P_MFGR        = 2;
    static const int P_BRAND       = 3;
    static const int P_TYPE        = 4;
    static const int P_SIZE        = 5;
    static const int P_CONTAINER   = 6;
    static const int P_RETAILPRICE = 7;
    static const int P_COMMENT     = 8;
private:
    void build_partkey_index();

    std::vector<jfkey_t> m_partkey;
    std::vector<int> m_size;
    std::vector<float> m_retailprice;

    std::shared_ptr< key_index > mp_partindex;
};