// Programmer: Robert Christensen
// email: robertc@cs.utah.edu

#include <iostream>
#include <fstream>
#include <string>
#include <exception>
#include <cstring>
#include <cstdlib>

#include "TableCustomer.h"
#include "DatabaseSharedTypes.h"

using namespace std;

Table_Customer::Table_Customer(string filename, size_t row_count)
{
    m_custkey.resize(row_count);
    m_nationkey.resize(row_count);
    m_phone.resize(row_count);
    m_acctbal.resize(row_count);
    m_mktsegment.resize(row_count);

    size_t index = 0;
    string input_buffer;
    ifstream input_file(filename);
    while (getline(input_file, input_buffer))
    {
        size_t start = 0;
        size_t next = input_buffer.find(FILE_DELIMINTAR, 0);

        m_custkey[index] = strtoll(&input_buffer[start], nullptr, 10);
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        // WE WILL NOT BE STORING THE NAME VALUE NOW
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        //  WE WILL NOT BE STORING ADDRESS VALUE NOW
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        m_nationkey[index] = strtoll(&input_buffer[start], nullptr, 10);
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        strncpy(m_phone[index].str, &input_buffer[start], next - start);
        m_phone[index].str[next - start + 1] = 0;
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        m_acctbal[index] = strtof(&input_buffer[start], nullptr);
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        strncpy(m_mktsegment[index].str, &input_buffer[start], next - start);
        m_mktsegment[index].str[next - start + 1] = 0;

        // WE WILL NOT BE STORING COMMENT VALUE NOW
        ++index;
    }
    input_file.close();

    if (index != row_count)
        std::cerr << "The table is a different size than it was reported to expect";
}

Table_Customer::~Table_Customer()
{
}

DATABASE_DATA_TYPES Table_Customer::getColumnTypes(int column)
{
    switch (column) {
    case 0: case 3:
        return DATABASE_DATA_TYPES::INT64;
    case 1: case 2: case 7:
        return DATABASE_DATA_TYPES::STRING;
    case 4: case 6:
        return DATABASE_DATA_TYPES::CHAR;
    case 5:
        return DATABASE_DATA_TYPES::FLOAT;
    default:
        return DATABASE_DATA_TYPES::NONEXISTANT;
    }
}

int64_t Table_Customer::get_int64(int64_t row, int column)
{
    switch (column)
    {
    case 0:
        return m_custkey[row];
    case 3:
        return m_nationkey[row];
    default:
        throw runtime_error("The requested column is not the requested type");
    }
}

const char* Table_Customer::get_char(int64_t row, int column)
{
    switch (column)
    {
    case 4:
        return m_phone[row].str;
    case 6:
        return m_mktsegment[row].str;
    default:
        throw runtime_error("The requested column is not the requested type");
    }
}

float Table_Customer::get_float(int64_t row, int column)
{
    switch (column) {
    case 5:
        return m_acctbal[row];
    default:
        throw runtime_error("The requested column is not the requested type");
    }
}

void Table_Customer::build_nation_index()
{
    if (mp_nationindex != nullptr)
        return;
    mp_nationindex.reset(new key_index());

    jfkey_t index = 0;
    for (jfkey_t value : m_nationkey) {
        mp_nationindex->emplace(value, index);
        ++index;
    }
}

void Table_Customer::build_cust_index()
{
    if (mp_custindex != nullptr)
        return;
    mp_custindex.reset(new key_index());

    jfkey_t index = 0;
    for (jfkey_t value : m_custkey) {
        mp_custindex->emplace(value, index);
        ++index;
    }
}

void Table_Customer::build_acctbal_index()
{
    if (mp_acctbalindex != nullptr)
        return;
    mp_acctbalindex.reset(new float_index());

    int64_t index = 0;
    for (float value : m_acctbal) {
        mp_acctbalindex->emplace(value, index);
        ++index;
    }
}

std::shared_ptr < key_index > Table_Customer::get_key_index(int column) {
    switch (column) {
    case 0:
        build_cust_index();
        return mp_custindex;
    case 3:
        build_nation_index();
        return mp_nationindex;
    default:
        throw runtime_error("The requested column does not have this type of index");
    }
}

std::shared_ptr < float_index > Table_Customer::get_float_index(int column) {
    switch (column) {
    case C_ACCTBAL:
        build_acctbal_index();
        return mp_acctbalindex;
    default:
        throw runtime_error("The requested column does not have this type of index");
    }
}

const std::vector<jfkey_t>::iterator Table_Customer::get_key_iterator(int column)
{
    switch (column)
    {
    case 0:
        return m_custkey.begin();
    case 3:
        return m_nationkey.begin();
    default:
        throw runtime_error("The requested column is not the requested type");
    }
}
