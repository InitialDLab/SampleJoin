//
// This is a table of all the tables in this pretend database
#pragma once

#include <string>
#include <map>
#include <memory>

#include "Table.h"

class TableRepository
{
public:
    TableRepository();

    // return a constant shared pointer to the table being requested
    // If the table does not exist the pointer will be null
    const std::shared_ptr<Table> get(std::string table) const;

    // register a table in this repository
    // if it already exists an exception will be thrown
    void register_table(std::string table_name, std::shared_ptr<Table> table_data);

    // TODO: add an iterator which will traverse the elemends in the table?

    // TODO: add the ability to unregister a table?

private:
    std::map<std::string, std::shared_ptr<Table> > m_data;
};