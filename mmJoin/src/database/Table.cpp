// Programer: Robert Christensen
// email: robertc@cs.utah.edu
// Implements some basic functionality for Table

#include <iostream>
 
#include "Table.h"

using namespace std;

int32_t Table::get_int(int64_t row, int column)
{
    throw runtime_error("No columns have type int in this table");
}

int64_t Table::get_int64(int64_t row, int column)
{
    throw runtime_error("No columns have type int64 in this table");
}

float Table::get_float(int64_t row, int column)
{
    throw runtime_error("No columns have type float in this table");
}

const char* Table::get_char(int64_t row, int column)
{
    throw runtime_error("No columns have type char in this table");
}

std::shared_ptr < key_index > Table::get_key_index(int column)
{
    throw runtime_error("No columns have an index of this type");
}

std::shared_ptr < int_index > Table::get_int_index(int column)
{
    return this->get_key_index(column);
}

std::shared_ptr < float_index > Table::get_float_index(int column)
{
    throw runtime_error("No columns have an index of this type");
}

const std::vector<jfkey_t>::iterator Table::get_key_iterator(int column)
{
    return std::vector<jfkey_t>::iterator();
}
