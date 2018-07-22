//
// This is an instance of a table object which can be interacted with in our pretend database
#pragma once

#include <stdint.h>
#include <memory>
#include <unordered_map>
#include <map>

#include "DatabaseSharedTypes.h"

typedef std::multimap<jfkey_t, int64_t> key_index;
typedef std::multimap<int64_t, int64_t> int_index;
typedef std::multimap<float, int64_t> float_index;

class Table
{
public:
    virtual ~Table()
    {
    };

    virtual int column_count() = 0;
    virtual int64_t row_count() = 0;
    virtual DATABASE_DATA_TYPES getColumnTypes(int column) = 0;

    virtual int32_t get_int(int64_t row, int column);
    virtual int64_t get_int64(int64_t row, int column);
    virtual float get_float(int64_t row, int column);
    virtual const char* get_char(int64_t row, int column);

    virtual std::shared_ptr < key_index > get_key_index(int column);

    virtual std::shared_ptr < int_index > get_int_index(int column);

    virtual std::shared_ptr < float_index > get_float_index(int column);

    virtual const std::vector<jfkey_t>::iterator get_key_iterator(int column);

private:
};