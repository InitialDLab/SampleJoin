// Programmer: Robert Christensen
// email: robertc@cs.utah.edu

#include <iostream>
#include <fstream>
#include <string>
#include <exception>
#include <cstring>
#include <cstdlib>

#include "TableLineitem.h"
#include "DatabaseSharedTypes.h"
#include "../util/Timer.h"

using namespace std;

Table_Lineitem::Table_Lineitem(string filename, size_t row_count)
{
    m_orderkey.resize(row_count);
    m_partkey.resize(row_count);
    m_suppkey.resize(row_count);
    m_linenumber.resize(row_count);
    m_quantity.resize(row_count);
    m_extendedprice.resize(row_count);
    m_discount.resize(row_count);
    m_tax.resize(row_count);
    m_returnflag.resize(row_count);
    m_linestatus.resize(row_count);
    m_shipdate.resize(row_count);
    m_commitdate.resize(row_count);
    m_receiptdate.resize(row_count);
    m_shipinstruct.resize(row_count);
    m_shipmode.resize(row_count);

    time_t epoch = getEpoch();
    struct tm time;
    time.tm_hour = 0;
    time.tm_isdst = 0;
    time.tm_min = 0;
    time.tm_sec = 0;
    int year, month, day;

    size_t index = 0;
    string input_buffer;
    ifstream input_file(filename);
    while (getline(input_file, input_buffer))
    {
        size_t start = 0;
        size_t next = input_buffer.find(FILE_DELIMINTAR, 0);

        m_orderkey[index] = strtoll(&input_buffer[start], nullptr, 10);
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        m_partkey[index] = strtoll(&input_buffer[start], nullptr, 10);
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        m_suppkey[index] = strtoll(&input_buffer[start], nullptr, 10);
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        m_linenumber[index] = strtol(&input_buffer[start], nullptr, 10);
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        m_quantity[index] = strtof(&input_buffer[start], nullptr);
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        m_extendedprice[index] = strtof(&input_buffer[start], nullptr);
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        m_discount[index] = strtof(&input_buffer[start], nullptr);
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        m_tax[index] = strtof(&input_buffer[start], nullptr);
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        m_returnflag[index] = input_buffer[start];
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        m_linestatus[index] = input_buffer[start];
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        sscanf(&input_buffer[start], "%4i-%2i-%2i", &year, &month, &day);
        time.tm_year = year - 1900;
        time.tm_mon = month - 1;
        time.tm_mday = day;
        m_shipdate[index] = difftime(mktime(&time), epoch);
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        sscanf(&input_buffer[start], "%4i-%2i-%2i", &year, &month, &day);
        time.tm_year = year - 1900;
        time.tm_mon = month - 1;
        time.tm_mday = day;
        m_commitdate[index] = difftime(mktime(&time), epoch);
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        sscanf(&input_buffer[start], "%4i-%2i-%2i", &year, &month, &day);
        time.tm_year = year - 1900;
        time.tm_mon = month - 1;
        time.tm_mday = day;
        m_receiptdate[index] = difftime(mktime(&time), epoch);
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        strncpy(m_shipinstruct[index].str, &input_buffer[start], next - start);
        m_shipinstruct[index].str[next - start + 1] = 0;
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        strncpy(m_shipmode[index].str, &input_buffer[start], next - start);
        m_shipmode[index].str[next - start + 1] = 0;
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        ++index;
    }
    input_file.close();

    if (index != row_count)
        std::cerr << "The table is a different size than it was reported to expect";
}

Table_Lineitem::~Table_Lineitem()
{
}

DATABASE_DATA_TYPES Table_Lineitem::getColumnTypes(int column)
{
    switch (column) {
    case 0: case 1: case 2:
    case 10: case 11: case 12:
        return DATABASE_DATA_TYPES::INT64;
    case 3:
        return DATABASE_DATA_TYPES::INT32;
    case 4: case 5: case 6: case 7:
        return DATABASE_DATA_TYPES::FLOAT;
    case 8:  case 9:
    case 13: case 14:
        return DATABASE_DATA_TYPES::CHAR;
    case 15:
        return DATABASE_DATA_TYPES::STRING;
    default:
        return DATABASE_DATA_TYPES::NONEXISTANT;
    }
}

int64_t Table_Lineitem::get_int64(int64_t row, int column)
{
    switch (column)
    {
    case 0:
        return m_orderkey.at(row);
    case 1:
        return m_partkey.at(row);
    case 2:
        return m_suppkey.at(row);
    case 10:
        return m_shipdate.at(row);
    case 11:
        return m_commitdate.at(row);
    case 12:
        return m_receiptdate.at(row);
    default:
        throw runtime_error("The requested column is not the requested type");
    }
}

int32_t Table_Lineitem::get_int32(int64_t row, int column)
{
    switch (column)
    {
    case 3:
        return m_linenumber.at(row);
    default:
        throw runtime_error("The requested column is not the requested type");
    }
}

float Table_Lineitem::get_float(int64_t row, int column)
{
    switch (column)
    {
    case 4:
        return m_quantity.at(row);
    case 5:
        return m_extendedprice.at(row);
    case 6:
        return m_discount.at(row);
    case 7:
        return m_tax.at(row);
    default:
        throw runtime_error("The requested column is not the requested type");
    }
}

const char* Table_Lineitem::get_char(int64_t row, int column)
{
    switch (column)
    {
    case 8:
        return &m_returnflag.at(row);
    case 9:
        return &m_linestatus.at(row);
    case 13:
        return m_shipinstruct.at(row).str;
    case 14:
        return m_shipmode.at(row).str;
    default:
        throw runtime_error("The requested column is not the requested type");
    }
}

void Table_Lineitem::build_order_index()
{
    if (mp_orderindex != nullptr)
        return;
    mp_orderindex.reset(new key_index());

    jfkey_t index = 0;
    for (jfkey_t value : m_orderkey) {
        mp_orderindex->emplace(value, index);
        ++index;
    }
}

void Table_Lineitem::build_part_index()
{
    if (mp_partindex != nullptr)
        return;
    mp_partindex.reset(new key_index());

    jfkey_t index = 0;
    for (jfkey_t value : m_partkey) {
        mp_partindex->emplace(value, index);
        ++index;
    }
}

void Table_Lineitem::build_supp_index()
{
    if (mp_suppindex != nullptr)
        return;
    mp_suppindex.reset(new key_index());

    jfkey_t index = 0;
    for (jfkey_t value : m_suppkey) {
        mp_suppindex->emplace(value, index);
        ++index;
    }
}

void Table_Lineitem::build_shipdate_index()
{
    if (mp_shipdateIndex != nullptr)
        return;
    mp_shipdateIndex.reset(new int_index());
    int64_t index = 0;
    for (int64_t value : m_shipdate) {
        mp_shipdateIndex->emplace(value, index);
        ++index;
    }
}

void Table_Lineitem::build_commitdate_index()
{
    if (mp_commitdateIndex != nullptr)
        return;
    mp_commitdateIndex.reset(new int_index());
    int64_t index = 0;
    for (int64_t value : m_commitdate) {
        mp_commitdateIndex->emplace(value, index);
        ++index;
    }
}
void Table_Lineitem::build_receiptdate_index()
{
    if (mp_receiptdateIndex != nullptr)
        return;
    mp_receiptdateIndex.reset(new int_index());
    int64_t index = 0;
    for (int64_t value : m_receiptdate) {
        mp_receiptdateIndex->emplace(value, index);
        ++index;
    }
}

std::shared_ptr < key_index > Table_Lineitem::get_key_index(int column)
{
    switch (column) {
    case 0:
        build_order_index();
        return mp_orderindex;
    case 1:
        build_part_index();
        return mp_partindex;
    case 2:
        build_supp_index();
        return mp_suppindex;
    default:
        throw runtime_error("The requested column does not have this type of index");
    }
}

std::shared_ptr <int_index> Table_Lineitem::get_int_index(int column)
{
    switch (column) {
    case L_SHIPDATE:
        build_shipdate_index();
        return mp_shipdateIndex;
    case L_COMMITDATE:
        build_commitdate_index();
        return mp_commitdateIndex;
    case L_RECEIPTDATE:
        build_receiptdate_index();
        return mp_receiptdateIndex;
    default:
        // since they are currently the same value, we can propagate the request to the other function.
        return get_key_index(column);
    }
}

const std::vector<jfkey_t>::iterator Table_Lineitem::get_key_iterator(int column)
{
    switch (column)
    {
    case 0:
        return m_orderkey.begin();
    case 1:
        return m_partkey.begin();
    case 2:
        return m_suppkey.begin();
    case 10:
        return m_shipdate.begin();
    case 11:
        return m_commitdate.begin();
    case 12:
        return m_receiptdate.begin();
    default:
        throw runtime_error("The requested column is not the requested type");
    }
}
