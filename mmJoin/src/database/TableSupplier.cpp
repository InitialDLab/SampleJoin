// Programer: Robert Christensen
// email: robertc@cs.utah.edu

#include <iostream>
#include <fstream>
#include <string>
#include <regex>
#include <exception>
#include <cstring>

#include "TableSupplier.h"
#include "DatabaseSharedTypes.h"

using namespace std;

Table_Supplier::Table_Supplier(string filename, size_t row_count)
{
    m_suppkey.resize(row_count);
    m_name.resize(row_count);
    m_nationkey.resize(row_count);
    m_phone.resize(row_count);
    m_acctbal.resize(row_count);

    // build the scanf string to use to read the file
    string formater = "(\\d+)\\";
    formater += FILE_DELIMINTAR;

    formater += "([[:print:]]{0,25})\\";
    formater += FILE_DELIMINTAR;

    formater += "([[:print:]]{0,40})\\";
    formater += FILE_DELIMINTAR;

    formater += "(\\d+)\\";
    formater += FILE_DELIMINTAR;

    formater += "([[:print:]]{0,15})\\";
    formater += FILE_DELIMINTAR;

    formater += "(\\-?\\d+|\\-?\\d+.\\d+)\\";
    formater += FILE_DELIMINTAR;

    formater += "([[:print:]]{0,101})\\";
    formater += FILE_DELIMINTAR;

    smatch matcher;

    regex pattern(formater);

    size_t index = 0;
    string input_buffer;
    ifstream input_file(filename);
    while (getline(input_file, input_buffer))
    {
        //size_t start = 0;
        //size_t next = input_buffer.find(FILE_DELIMINTAR, 0);

        //m_suppkey[index] = strtoll(&input_buffer[start], nullptr, 10);
        //start = next + 1;
        //next = input_buffer.find(FILE_DELIMINTAR, start);

        //strncpy_s(m_name[index].str, &input_buffer[start], next - start);
        //m_name[index].str[next - start + 1] = 0;
        //start = next + 1;
        //next = input_buffer.find(FILE_DELIMINTAR, start);

        //// ignore index 3, which is the address
        //start = next + 1;
        //next = input_buffer.find(FILE_DELIMINTAR, start);

        //m_nationkey[index] = strtoll(&input_buffer[start], nullptr, 10);
        //start = next + 1;
        //next = input_buffer.find(FILE_DELIMINTAR, start);

        //strncpy_s(m_phone.at(index).str, &input_buffer[start], next - start);
        //m_phone[index].str[next - start + 1] = 0;
        //start = next + 1;
        //next = input_buffer.find(FILE_DELIMINTAR, start);

        //m_acctbal[index] = strtof(&input_buffer[start], nullptr);

        //++index;

        if (regex_search(input_buffer, matcher, pattern)) {
            m_suppkey[index] = strtoll(matcher[1].str().c_str(), nullptr, 10);
            strcpy(m_name[index].str, matcher[2].str().c_str());
            // ignore index 3, which is the address
            m_nationkey[index] = strtoll(matcher[4].str().c_str(), nullptr, 10);
            strcpy(m_phone[index].str, matcher[5].str().c_str());
            m_acctbal[index] = strtof(matcher[6].str().c_str(), nullptr);

            ++index;
            // discard the last field (no support for varchar!)
        }
    }

    if (index != row_count)
        std::cerr << "The table is a different size than it was reported to expect";
}

Table_Supplier::~Table_Supplier()
{
}

DATABASE_DATA_TYPES Table_Supplier::getColumnTypes(int column) {
    switch (column) {
    case 0:
        return DATABASE_DATA_TYPES::INT64;
    case 1:
        return DATABASE_DATA_TYPES::CHAR;
    case 2:
        return DATABASE_DATA_TYPES::STRING;
    case 3:
        return DATABASE_DATA_TYPES::INT64;
    case 4:
        return DATABASE_DATA_TYPES::CHAR;
    case 5:
        return DATABASE_DATA_TYPES::FLOAT;
    case 6:
        return DATABASE_DATA_TYPES::STRING;
    default:
        return DATABASE_DATA_TYPES::NONEXISTANT;
    }
}

int64_t Table_Supplier::get_int64(int64_t row, int column)
{
    switch (column) {
    case 0:
        return m_suppkey[row];
    case 3:
        return m_nationkey[row];
    default:
        throw runtime_error("The specified row is not of int64 type");
    }
}

const char* Table_Supplier::get_char(int64_t row, int column)
{
    switch (column) {
    case 1:
        return m_name[row].str;
    case 4:
        return m_phone[row].str;
    default:
        throw runtime_error("The specified row is not of type char");
    }
}

float Table_Supplier::get_float(int64_t row, int column)
{
    switch (column) {
    case 5:
        return m_acctbal[row];
    default:
        throw runtime_error("The specified row is not of type float");
    }
}

void Table_Supplier::build_Supp_index() {
    if (m_suppindex != nullptr)
        return;
    m_suppindex.reset(new key_index());

    jfkey_t index = 0;
    for (jfkey_t value : m_suppkey) {
        m_suppindex->emplace(value, index);
        index++;
    }
}

void Table_Supplier::build_Nation_index() {
    if (m_nationindex != nullptr)
        return;
    m_nationindex.reset(new key_index());

    jfkey_t index = 0;
    for (jfkey_t value : m_nationkey) {
        m_nationindex->emplace(value, index);
        index++;
    }
}

void Table_Supplier::build_acctbal_index()
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

std::shared_ptr < key_index > Table_Supplier::get_key_index(int column) {
    switch (column) {
    case 0:
        build_Supp_index();
        return m_suppindex;
    case 3:
        build_Nation_index();
        return m_nationindex;
    default:
        throw runtime_error("The specified row is not of int64 type");
    }
}

std::shared_ptr<float_index> Table_Supplier::get_float_index(int column)
{
    switch (column) {
    case S_ACCTBAL:
        build_acctbal_index();
        return mp_acctbalindex;
    default:
        throw runtime_error("The requested column does not have this type of index");
    }
}

const std::vector<jfkey_t>::iterator Table_Supplier::get_key_iterator(int column)
{
    switch (column) {
    case 0:
        return m_suppkey.begin();
    case 3:
        return m_nationkey.begin();
    default:
        throw runtime_error("The specified row is not of int64 type");
    }
}
