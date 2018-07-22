// Programmer: Robert Christensen
// email: robertc@cs.utah.edu

#include <iostream>
#include <fstream>
#include <string>
#include <regex>
#include <exception>

#include "TablePartsupp.h"
#include "DatabaseSharedTypes.h"

using namespace std;

Table_Partsupp::Table_Partsupp(string filename, size_t row_count)
{
    m_partskey.resize(row_count);
    m_suppkey.resize(row_count);
    m_availqty.resize(row_count);
    m_supplycost.resize(row_count);

    size_t index = 0;
    string input_buffer;
    ifstream input_file(filename);
    while (getline(input_file, input_buffer))
    {
        size_t start = 0;
        size_t next = input_buffer.find(FILE_DELIMINTAR, 0);

        m_partskey[index] = strtoll(&input_buffer[start], nullptr, 10);
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        m_suppkey[index] = strtoll(&input_buffer[start], nullptr, 10);
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        m_availqty[index] = strtol(&input_buffer[start], nullptr, 10);
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        m_supplycost[index] = strtof(&input_buffer[start], nullptr);

        ++index;
    }
    input_file.close();

    if (index != row_count)
        std::cerr << "The table is a different size than it was reported to expect";
}

Table_Partsupp::~Table_Partsupp()
{
}

DATABASE_DATA_TYPES Table_Partsupp::getColumnTypes(int column)
{
    switch (column) {
    case 0: case 1:
        return DATABASE_DATA_TYPES::INT64;
    case 2:
        return DATABASE_DATA_TYPES::INT32;
    case 3:
        return DATABASE_DATA_TYPES::FLOAT;
    case 4:
        return DATABASE_DATA_TYPES::STRING;
    default:
        return DATABASE_DATA_TYPES::NONEXISTANT;
    }
}

int32_t Table_Partsupp::get_int32(int64_t row, int column)
{
    switch (column) {
    case 2:
        return m_availqty[row];
    default:
        throw runtime_error("The requested column is not the requested type");
    }
}

int64_t Table_Partsupp::get_int64(int64_t row, int column)
{
    switch (column) {
    case 0:
        return m_partskey[row];
    case 1:
        return m_suppkey[row];
    default:
        throw runtime_error("The requested column is not the requested type");
    }
}

float Table_Partsupp::get_float(int64_t row, int column)
{
    switch (column) {
    case 3:
        return m_supplycost[row];
    default:
        throw runtime_error("The requested column is not the requested type");
    }
}

void Table_Partsupp::build_parts_index() {
    if (mp_partsindex != nullptr)
        return;
    mp_partsindex.reset(new key_index());

    jfkey_t index = 0;
    for (jfkey_t value : m_partskey) {
        mp_partsindex->emplace(value, index);
        index++;
    }
}

void Table_Partsupp::build_supp_index() {
    if (mp_suppindex != nullptr)
        return;
    mp_suppindex.reset(new key_index());

    jfkey_t index = 0;
    for (jfkey_t value : m_suppkey) {
        mp_suppindex->emplace(value, index);
        index++;
    }
}

std::shared_ptr < key_index > Table_Partsupp::get_key_index(int column) {
    switch (column) {
    case 0:
        build_parts_index();
        return mp_partsindex;
    case 1:
        build_supp_index();
        return mp_suppindex;
    default:
        throw runtime_error("The requested column is not the requested type");
    }
}

const std::vector<jfkey_t>::iterator Table_Partsupp::get_key_iterator(int column)
{
    switch (column) {
    case 0:
        return m_partskey.begin();
    case 1:
        return m_suppkey.begin();
    default:
        throw runtime_error("The requested column is not the requested type");
    }
}
