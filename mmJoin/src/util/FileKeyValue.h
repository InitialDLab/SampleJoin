// Implementation of a simple text file backed key value store.
// Programmer: Robert Christensen
// email: robertc@cs.utah.edu
#pragma once

#include <map>
#include <string>

// the format of the text file storage demands that the keys and values being stored do not
// store the value separator or the new line character.  Because of this, if the string contains
// the separator character an exception will be thrown.

class FileKeyValue {
public:
    FileKeyValue(std::string backedFile);

    ~FileKeyValue()
    {};

    // returns true if such a value exists in the map
    bool exists(std::string key);

    // deletes the entry
    bool remove(std::string key);

    // return the value associated with the key
    // empty string if no such value exists
    std::string get(std::string key);

    // inserts a value into the map
    // removes the value if one exits in the map
    std::string insert(std::string key, std::string value);
    std::string insert(std::string key, long value);

    // appends the value to the current value associated with that key
    // creates the value if one does not exist.
    std::string append(std::string key, std::string value);
    std::string append(std::string key, long value);

    // place a ',' between insertions of appended values
    std::string appendArray(std::string key, std::string value);
    std::string appendArray(std::string key, long value);

    // write the data to the backing file.
    void flush();

private:
    char m_seperator;

    std::string m_BackedFileName;

    std::map<std::string, std::string> m_data;
};