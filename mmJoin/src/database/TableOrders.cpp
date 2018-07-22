// Programmer: Robert Christensen
// email: robertc@cs.utah.edu

#include <iostream>
#include <fstream>
#include <string>
#include <exception>
#include <cstring>
#include <cstdlib>

#include "TableOrders.h"
#include "DatabaseSharedTypes.h"
#include "../util/Timer.h"

using namespace std;

Table_Orders::Table_Orders(string filename, size_t row_count)
{
    m_orderkey.resize(row_count);
    m_custkey.resize(row_count);
    m_orderstatus.resize(row_count);
    m_totalprice.resize(row_count);
    m_orderdate.resize(row_count);
    m_orderpriority.resize(row_count);
    m_clerk.resize(row_count);
    m_shippriority.resize(row_count);

    time_t epoch = getEpoch();

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

        m_custkey[index] = strtoll(&input_buffer[start], nullptr, 10);
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        m_orderstatus[index] = input_buffer[start];
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        m_totalprice[index] = strtof(&input_buffer[start], nullptr);
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        struct tm time;
        int year, month, day;
        memset(&time, 0, sizeof(time));
        sscanf(&input_buffer[start], "%4i-%2i-%2i", &year, &month, &day);
        time.tm_year = year - 1900;
        time.tm_mon  = month - 1;
        time.tm_mday = day;
        m_orderdate[index] = difftime(mktime(&time), epoch);
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        strncpy(m_orderpriority[index].str, &input_buffer[start], next - start);
        m_orderpriority[index].str[next - start + 1] = 0;
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        strncpy(m_clerk[index].str, &input_buffer[start], next - start);
        m_clerk[index].str[next - start + 1] = 0;
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        m_shippriority[index] = strtoll(&input_buffer[start], nullptr, 10);
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        ++index;
    }
    input_file.close();

    if (index != row_count)
        std::cerr << "The table is a different size than it was reported to expect";
}

Table_Orders::~Table_Orders()
{
};

DATABASE_DATA_TYPES Table_Orders::getColumnTypes(int column)
{
    switch (column) {
    case 0: case 1: case 6:
        return DATABASE_DATA_TYPES::INT64;
    case 2: case 4: case 5:
        return DATABASE_DATA_TYPES::CHAR;
    case 3:
        return DATABASE_DATA_TYPES::FLOAT;
    case 7:
        return DATABASE_DATA_TYPES::INT32;
    case 8:
        return DATABASE_DATA_TYPES::STRING;
    default:
        return DATABASE_DATA_TYPES::NONEXISTANT;
    }
}

int64_t Table_Orders::get_int64(int64_t row, int column)
{
    switch (column)
    {
    case 0:
        return m_orderkey.at(row);
    case 1:
        return m_custkey.at(row);
    case 4:
        return m_orderdate.at(row);
    default:
        throw runtime_error("The requested column is not the requested type");
    }
}

const char* Table_Orders::get_char(int64_t row, int column)
{
    switch (column)
    {
    case 2:
        return &m_orderstatus.at(row);
    case 5:
        return m_orderpriority.at(row).str;
    case 6:
        return m_clerk.at(row).str;
    default:
        throw runtime_error("The requested column is not the requested type");
    }
}

float Table_Orders::get_float(int64_t row, int column)
{
    switch (column)
    {
    case 3:
        return m_totalprice.at(row);
    default:
        throw runtime_error("The requested column is not the requested type");
    }
}

void Table_Orders::build_orders_index()
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

void Table_Orders::build_cust_index()
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

void Table_Orders::build_orderdate_index()
{
    if (mp_orderdateindex != nullptr)
        return;
    mp_orderdateindex.reset(new int_index());

    int64_t index = 0;
    for (int64_t value : m_orderdate) {
        mp_orderdateindex->emplace(value, index);
        ++index;
    }
}

std::shared_ptr < key_index > Table_Orders::get_key_index(int column) {
    switch (column) {
    case 0:
        build_orders_index();
        return mp_orderindex;
    case 1:
        build_cust_index();
        return mp_custindex;
    default:
        throw runtime_error("The requested column does not have this type of index");
    }
}

std::shared_ptr<int_index> Table_Orders::get_int_index(int column)
{
    switch (column) {
    case O_ORDERDATE:
        build_orderdate_index();
        return mp_orderdateindex;
    default:
        return this->get_key_index(column);
    }
}

const std::vector<jfkey_t>::iterator Table_Orders::get_key_iterator(int column)
{
    switch (column)
    {
    case 0:
        return m_orderkey.begin();
    case 1:
        return m_custkey.begin();
    case 4:
        return m_orderdate.begin();
    default:
        throw runtime_error("The requested column is not the requested type");
    }
}
