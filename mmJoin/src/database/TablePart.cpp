#include "TablePart.h"

#include <fstream>
#include <cstdlib>
#include <iostream>

using namespace std;

Table_Part::Table_Part(std::string filename, size_t row_count)
{
    m_partkey.resize(row_count);
    m_retailprice.resize(row_count);
    m_size.resize(row_count);

    size_t index = 0;
    string input_buffer;
    ifstream input_file(filename);
    while (getline(input_file, input_buffer))
    {
        size_t start = 0;
        size_t next = input_buffer.find(FILE_DELIMINTAR, 0);

        m_partkey[index] = strtoll(&input_buffer[start], nullptr, 10);
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        // skip name
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);
        
        // skip mfgr
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);
        
        // skip brand
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        // skip type
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        m_size[index] = strtol(&input_buffer[start], nullptr, 10);
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        // skip container
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        m_retailprice[index] = strtof(&input_buffer[start], nullptr);
        start = next + 1;
        next = input_buffer.find(FILE_DELIMINTAR, start);

        ++index;
    }
    input_file.close();

    if (index != row_count)
        std::cerr << "The table is a different size than it was reported to expect";
}

Table_Part::~Table_Part()
{
}

DATABASE_DATA_TYPES Table_Part::getColumnTypes(int column)
{
    switch (column) {
    case 0:
        return DATABASE_DATA_TYPES::INT64;
    case 1: case 4: case 8:
        return DATABASE_DATA_TYPES::STRING;
    case 2: case 3: case 6:
        return DATABASE_DATA_TYPES::CHAR;
    case 5:
        return DATABASE_DATA_TYPES::INT32;
    case 7:
        return DATABASE_DATA_TYPES::FLOAT;
    default:
        return DATABASE_DATA_TYPES::NONEXISTANT;
    }
}

int64_t Table_Part::get_int64(int64_t row, int column)
{
    switch (column) {
    case 0:
        return m_partkey.at(row);
    case 5:
        return m_size.at(row);
    default:
        throw "The requested column was not the requested type";
    }
}

float Table_Part::get_float(int64_t row, int column)
{
    switch (column) {
    case P_RETAILPRICE:
        return m_retailprice.at(row);
    default:
        throw "the requested column was not the requested type";
    }
}

std::shared_ptr<key_index> Table_Part::get_key_index(int column)
{
    switch (column) {
    case P_PARTKEY:
        build_partkey_index();
        return mp_partindex;
    default:
        throw runtime_error("The requested column does not have this type of index");
    }
}

const std::vector<jfkey_t>::iterator Table_Part::get_key_iterator(int column)
{
    switch (column) {
    case 0:
        return m_partkey.begin();
    //case 5:
    //    return m_size.begin();
    default:
        throw "The requested column was not the requested type";
    }
}

void Table_Part::build_partkey_index()
{
    if (mp_partindex != nullptr)
        return;
    mp_partindex.reset(new key_index());

    jfkey_t index = 0;
    for (jfkey_t value : m_partkey) {
        mp_partindex->emplace(value, index);
        index++;
    }
}
