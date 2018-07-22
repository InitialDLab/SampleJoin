// Programmer: Robert Christensen
// email: robertc@cs.utah.edu

#include <string>
#include <map>
#include <memory>

#include "TableRepository.h"
#include "Table.h"

TableRepository::TableRepository()
{
}

// return a constant shared pointer to the table being requested
// If the table does not exist the pointer will be null
const std::shared_ptr<Table> TableRepository::get(std::string table) const
{
    auto it = m_data.find(table);
    if (it == m_data.end())
        return std::shared_ptr<Table>(nullptr);
    else
        return it->second;
}

// register a table in this repository
// if it already exists an exception will be thrown
void TableRepository::register_table(std::string table_name, std::shared_ptr<Table> table_data)
{
    m_data[table_name] = table_data;
}