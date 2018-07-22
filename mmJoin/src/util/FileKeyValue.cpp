// Programmer: Robert Christensen
// email: robertc@cs.utah.edu

#include "FileKeyValue.h"

#include <fstream>
#include <string>

std::string to_string(long val)
{
    char buf[32];
    sprintf(buf, "%ld", val);
    return std::string(buf);
}

FileKeyValue::FileKeyValue(std::string backedFile)
    : m_seperator{ ':' }
{
    m_BackedFileName = backedFile;
    // TODO, if a file exists, open it and insert into the data store
    std::string input_buffer;
    std::ifstream input_file(m_BackedFileName);
    if (input_file) {
        while (getline(input_file, input_buffer)) {
            size_t splitter_index = input_buffer.find(m_seperator);
            if (splitter_index != std::string::npos) {
                m_data.emplace(input_buffer.substr(0, splitter_index), input_buffer.substr(splitter_index + 1));
            }
        }
    }
}

bool FileKeyValue::exists(std::string key)
{
    return m_data.count(key) != 0;
}

bool FileKeyValue::remove(std::string key)
{
    m_data.erase(key);

    return !exists(key);
}

std::string FileKeyValue::get(std::string key)
{
    if (exists(key))
        return m_data.at(key);
    else
        return "";
}

std::string FileKeyValue::insert(std::string key, std::string value)
{
    m_data[key] = value;
    return m_data.at(key);
}

std::string FileKeyValue::insert(std::string key, long value)
{
    return insert(key, to_string(value));
}

std::string FileKeyValue::append(std::string key, std::string value)
{
    m_data[key] += value;
    return m_data.at(key);
}

std::string FileKeyValue::append(std::string key, long value)
{
    return append(key, to_string(value));
}

std::string FileKeyValue::appendArray(std::string key, std::string value)
{
    if (exists(key))
        m_data[key] += ",";
    append(key, value);

    return m_data.at(key);
}

std::string FileKeyValue::appendArray(std::string key, long value)
{
    return appendArray(key, to_string(value));
}

void FileKeyValue::flush()
{
    std::fstream out_stream(m_BackedFileName, std::fstream::out | std::fstream::trunc);

    for (auto pair : m_data) {
        out_stream << pair.first << m_seperator << pair.second << '\n';
    }

    out_stream.close();
}