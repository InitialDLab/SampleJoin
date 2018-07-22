// Implements a reader to get saved information about the table files
// (such as how many lines the file contains).
// To create a file which can be read by this class, use wc > filename
//
// Programmer: Robert Christensen
// email: robertc@cs.utah.edu
#pragma once

#include <string>
#include <map>

class FileSizeTable {
public:
    FileSizeTable(std::string data_file);
    ~FileSizeTable()
    {};

    long get_lines(std::string filename);
    long get_words(std::string filename);
    long get_bytes(std::string filename);
private:
    std::map<std::string, long> m_lines;
    std::map<std::string, long> m_words;
    std::map<std::string, long> m_bytes;
};