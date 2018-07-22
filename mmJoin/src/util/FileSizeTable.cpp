// Programmer: Robert Christensen
// email: robertc@cs.utah.edu
#include "FileSizeTable.h"

#include <iostream>
#include <fstream>
#include <string>
#include <map>
#include <regex>

using namespace std;

FileSizeTable::FileSizeTable(std::string data_file)
{
    std::string formater = R"([[:space:]]*([[:digit:]]+)[[:space:]]*([[:digit:]]+)[[:space:]]*([[:digit:]]+)[[:space:]]*([[:print:]]+))";
    
    smatch matcher;

    regex pattern(formater);

    string input_buffer;
    ifstream input_file(data_file);
    while (getline(input_file, input_buffer))
    {
        if (regex_search(input_buffer, matcher, pattern)) {
            m_lines.emplace(matcher[4].str(), strtoll(matcher[1].str().c_str(), nullptr, 10));
            m_words.emplace(matcher[4].str(), strtoll(matcher[2].str().c_str(), nullptr, 10));
            m_bytes.emplace(matcher[4].str(), strtoll(matcher[3].str().c_str(), nullptr, 10));
        }
    }
}

long FileSizeTable::get_lines(std::string filename)
{
    if (m_lines.count(filename) != 0)
        return m_lines.at(filename);
    else
        return 0;
}

long FileSizeTable::get_words(std::string filename)
{
    if (m_words.count(filename) != 0)
        return m_words.at(filename);
    else
        return 0;
}

long FileSizeTable::get_bytes(std::string filename)
{
    if (m_words.count(filename) != 0)
        return m_words.at(filename);
    else
        return 0;
}