// Programer: Robert Christensen
// email: robertc@cs.utah.edu

#include <iostream>
#include <fstream>
#include <string>
#include <regex>
#include <exception>
#include <cstring>

#include "TableRegion.h"
#include "DatabaseSharedTypes.h"

using namespace std;

Table_Region::Table_Region(string filename, size_t row_count)
{
    mp_regionkey.resize(row_count);
    mp_name.resize(row_count);

    // build the scanf string to use to read the file
    string formater = "(\\d+)\\";
    formater += FILE_DELIMINTAR;
    formater += "([[:print:]]{0,25})\\";
    formater += FILE_DELIMINTAR;
    formater += "([[:print:]]{0,152})\\";
    formater += FILE_DELIMINTAR;
    smatch matcher;

    regex pattern(formater);

    size_t index = 0;
    string input_buffer;
    ifstream input_file(filename);
    while (getline(input_file, input_buffer))
    {
        if (regex_search(input_buffer, matcher, pattern)) {
            mp_regionkey[index] = strtoll(matcher[1].str().c_str(), nullptr, 10);
            strcpy(mp_name[index].str, matcher[2].str().c_str());

            ++index;
            // discard the last field (no support for varchar!
        }
    }

    if (index != row_count)
        std::cerr << "The table is a different size than it was reported to expect";
}

Table_Region::~Table_Region()
{
}

DATABASE_DATA_TYPES Table_Region::getColumnTypes(int column) {
    switch (column) {
    case 0:
        return DATABASE_DATA_TYPES::INT64;
    case 1:
        return DATABASE_DATA_TYPES::CHAR;
    case 2:
        return DATABASE_DATA_TYPES::STRING;
    default:
        return DATABASE_DATA_TYPES::NONEXISTANT;
    }
}

int64_t Table_Region::get_int64(int64_t row, int column) {
    switch (column) {
    case 0:
        return mp_regionkey[row];
    default:
        throw runtime_error("The requested column is not the requested type");
    }
}

const char* Table_Region::get_char(int64_t row, int column) {
    switch (column) {
    case 1:
        return mp_name[row].str;
    default:
        throw runtime_error("The requested column is not the requested type");
    }
}

void Table_Region::build_Region_index() {
    if (mp_regionindex != nullptr)
        return;
    mp_regionindex.reset(new key_index());

    jfkey_t index = 0;
    for (jfkey_t value : mp_regionkey) {
        mp_regionindex->emplace(value, index);
        index++;
    }
}

std::shared_ptr < key_index > Table_Region::get_key_index(int column) {
    switch (column) {
    case 0:
        build_Region_index();
        return mp_regionindex;
    default:
        throw runtime_error("The requested column is not the requested type");
    }
}

const std::vector<jfkey_t>::iterator Table_Region::get_key_iterator(int column)
{
    switch (column) {
    case 0:
        return mp_regionkey.begin();
    default:
        throw runtime_error("The requested column is not the requested type");
    }
}
