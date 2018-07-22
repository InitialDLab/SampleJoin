#pragma once
// This file is a generic table, used for reading in basic information
// from a file for joins.

#include <vector>
#include <iostream>
#include <fstream>
#include <string>
#include <assert.h>
#include <algorithm>
#include <exception>
#include <unordered_map>
#include <type_traits>

#include "Table.h"
#include "ColumnExtractor.h"

using EmptyAugmenter = ColumnExtractor<>;

class TableGenericBase {
private:
    struct index_elm
    {
        jefastKey_t value;
        jefastKey_t index;
    };
    
public:
    TableGenericBase(bool c12_same):
        m_ptr_column1(std::make_shared<std::vector<jefastKey_t>>()),
        m_column1(*m_ptr_column1),
        m_ptr_column2(
            c12_same ? m_ptr_column1 : std::make_shared<std::vector<jefastKey_t>>()),
        m_column2(*m_ptr_column2),
        m_ptr_column1_index(std::make_shared<std::vector<index_elm>>()),
        m_column1_index(*m_ptr_column1_index),
        m_ptr_column2_index(
            c12_same ? m_ptr_column1_index : std::make_shared<std::vector<index_elm>>()),
        m_column2_index(*m_ptr_column2_index),
        m_ptr_column1_fast_index(std::make_shared<
            std::unordered_map<jefastKey_t, std::vector<jefastKey_t>>>()),
        m_column1_fast_index(*m_ptr_column1_fast_index),
        m_ptr_column2_fast_index(
            c12_same ? m_ptr_column1_fast_index : std::make_shared<
            std::unordered_map<jefastKey_t, std::vector<jefastKey_t>>>()),
        m_column2_fast_index(*m_ptr_column2_fast_index),
        m_ptr_uniquecolumn1(std::make_shared<std::vector<jefastKey_t>>()),
        m_uniquecolumn1(*m_ptr_uniquecolumn1),
        m_ptr_uniquecolumn2(
            c12_same ? m_ptr_uniquecolumn1 : std::make_shared<
            std::vector<jefastKey_t>>()),
        m_uniquecolumn2(*m_ptr_uniquecolumn2) {}

    TableGenericBase(
        std::shared_ptr<std::vector<jefastKey_t>> ptr_column1,
        std::shared_ptr<std::vector<jefastKey_t>> ptr_column2,
        std::shared_ptr<std::vector<index_elm>> ptr_column1_index,
        std::shared_ptr<std::vector<index_elm>> ptr_column2_index,
        std::shared_ptr<std::unordered_map<jefastKey_t, std::vector<jefastKey_t>>>
            ptr_column1_fast_index,
        std::shared_ptr<std::unordered_map<jefastKey_t, std::vector<jefastKey_t>>>
            ptr_column2_fast_index,
        std::shared_ptr<std::vector<jefastKey_t>> ptr_uniquecolumn1,
        std::shared_ptr<std::vector<jefastKey_t>> ptr_uniquecolumn2):
        m_ptr_column1(ptr_column1),
        m_column1(*m_ptr_column1),
        m_ptr_column2(ptr_column2),
        m_column2(*m_ptr_column2),
        m_ptr_column1_index(ptr_column1_index),
        m_column1_index(*m_ptr_column1_index),
        m_ptr_column2_index(ptr_column2_index),
        m_column2_index(*m_ptr_column2_index),
        m_ptr_column1_fast_index(ptr_column1_fast_index),
        m_column1_fast_index(*m_ptr_column1_fast_index),
        m_ptr_column2_fast_index(ptr_column2_fast_index),
        m_column2_fast_index(*m_ptr_column2_fast_index),
        m_ptr_uniquecolumn1(ptr_uniquecolumn1),
        m_uniquecolumn1(*m_ptr_uniquecolumn1),
        m_ptr_uniquecolumn2(ptr_uniquecolumn2),
        m_uniquecolumn2(*m_ptr_uniquecolumn2) {}

    // build indexes for all columns
    void Build_indexes(bool full_only = false) {
        // build index 1
        if (!full_only) {
            index_elm tmp;
            tmp.index = 0;
            for (auto &i : m_column1)
            {
                tmp.value = i;
                m_column1_index.push_back(tmp);
                ++tmp.index;
            }
            // sort by key (keeping pointers to source available)
            std::sort(m_column1_index.begin(), m_column1_index.end(), [](const index_elm &a, const index_elm &b) {return a.value < b.value;});
        }

        // build index 2
        if (!full_only && &m_column2 != &m_column1) {
            index_elm tmp;
            tmp.index = 0;
            for (auto &i : m_column2)
            {
                tmp.value = i;
                m_column2_index.push_back(tmp);
                ++tmp.index;
            }
            // sort by key (keeping points to source available)
            std::sort(m_column2_index.begin(), m_column2_index.end(), [](const index_elm &a, const index_elm &b) {return a.value < b.value;});
        }

        // build fast index 1
        if (!full_only) {
            size_t idx = 0;
            for (auto &i : m_column1)
            {
                if (m_column1_fast_index.count(i) == 0)
                    m_uniquecolumn1.push_back(i);

                m_column1_fast_index[i].push_back(idx);
                ++idx;
            }
        }

        // build fast index 2
        if (!full_only && &m_column2 != &m_column1) {
            size_t idx = 0;
            for (auto &i : m_column2)
            {
                if (m_column2_fast_index.count(i) == 0)
                    m_uniquecolumn2.push_back(i);

                m_column2_fast_index[i].push_back(idx);
                ++idx;
            }
        }

        // build full lookup index
        {
            for (jefastKey_t i = 0; (i < m_column1.size()) && (i < m_column2.size()); ++i)
            {
                m_full_lookup_index[m_column1[i]][m_column2[i]] = i;
            }
        }
    }

    // return the number of rows in the table
    int get_size()
    {
        // for some tables, one of the columns might be empty, so return the max
        return std::max(this->m_column1.size(), this->m_column2.size());
    }

    // given a value, it will return the number of times that value appears in
    // the column
    int count_cardinality(int value, int column = 1) {
        switch (column) {
        case 1:
        {
            auto start_idx = std::lower_bound(m_column1_index.begin(), m_column1_index.end(), value, [](const index_elm &a, int b) {return a.value < b;});
            auto end_idx = std::upper_bound(start_idx, m_column1_index.end(), value, [](int a, const index_elm &b) {return a < b.value;});
            return (int) std::distance(start_idx, end_idx);
        }
            break;
        case 2:
        {
            auto start_idx = std::lower_bound(m_column2_index.begin(), m_column2_index.end(), value, [](const index_elm &a, int b) {return a.value < b;});
            auto end_idx = std::upper_bound(start_idx, m_column2_index.end(), value, [](int a, const index_elm &b) {return a < b.value;});
            return (int) std::distance(start_idx, end_idx);
        }
            break;
        default:
            throw "column does not exist";
        }
    }

    int count_cardinaltiy_f(int value, int column = 1) {
        switch (column) {
        case 1:
        {
            return m_column1_fast_index[value].size();
        }
        break;
        case 2:
        {
            return m_column2_fast_index[value].size();
        }
        break;
        default:
            throw "column does not exist";
        }
    }

    // get the item at a particular index for column 1
    jefastKey_t get_column1_value(int index)
    {
        return m_column1[index];
    };
    // get the item at a particular index for column 2
    jefastKey_t get_column2_value(int index)
    {
        return m_column2[index];
    };

    int get_column1_index_offset(int value, int offset = 0)
    {
        return m_column1_fast_index[value].at(offset);

        //auto start_idx = std::lower_bound(m_column1_index.begin(), m_column1_index.end(), value, [](auto &a, auto b) {return a.value < b;});
        // check to make sure the value we are finding is actually in the data
        //assert(start_idx->value == value);
        //start_idx += offset;
        //return start_idx->index;
    }

    int get_column2_index_offset(int value, int offset = 0)
    {
        return m_column2_fast_index[value].at(offset);

        //auto start_idx = std::lower_bound(m_column2_index.begin(), m_column2_index.end(), value, [](auto &a, auto b) {return a.value < b;});
        // check to make sure the value we are finding is actually in the data
        //assert(start_idx->value == value);
        //start_idx += offset;
        //return start_idx->index;
    }

    int get_column1_index_offset2(int value, int offset = 0)
    {
        if (offset > count_cardinality(value, 1))
            return -1;

        auto start_idx = std::lower_bound(m_column1_index.begin(), m_column1_index.end(), value, [](index_elm a, int b) {return a.value < b;});
        if (start_idx->value != value)
            return -1;
        start_idx += offset;
        if (start_idx->value != value)
            return -1;
        return start_idx->index;
    }

    int get_column2_index_offset2(int value, int offset = 0)
    {
        if (offset > count_cardinality(value, 2))
            return -1;

        auto start_idx = std::lower_bound(m_column2_index.begin(), m_column2_index.end(), value, [](index_elm a, int b) {return a.value < b;});
        if (start_idx->value == value)
            return -1;
        start_idx += offset;
        if (start_idx->value != value)
            return -1;
        return start_idx->index;
    }

    int get_column1_index_index(int value)
    {
        auto start_idx = std::lower_bound(m_column1_index.begin(), m_column1_index.end(), value, [](index_elm &a, int b) {return a.value < b;});
        return std::distance(m_column1_index.begin(), start_idx);
    }

    int get_column2_index_index(int value)
    {
        auto start_idx = std::lower_bound(m_column2_index.begin(), m_column2_index.end(), value, [](index_elm &a, int b) {return a.value < b;});
        return std::distance(m_column2_index.begin(), start_idx);
    }

    int get_index_of_values(jefastKey_t column1_value, jefastKey_t column2_value)
    {
        auto idx = m_full_lookup_index.find(column1_value);
        if (idx == m_full_lookup_index.end())
            return -1;

        auto idx2 = idx->second.find(column2_value);

        if (idx2 == idx->second.end())
            return -1;

        return idx2->second;
    }

protected:
    std::shared_ptr<std::vector<jefastKey_t>> m_ptr_column1;
    std::vector<jefastKey_t> &m_column1;

    std::shared_ptr<std::vector<jefastKey_t>> m_ptr_column2;
    std::vector<jefastKey_t> &m_column2;

    std::shared_ptr<std::vector<index_elm>> m_ptr_column1_index;
    std::vector<index_elm> &m_column1_index;

    std::shared_ptr<std::vector<index_elm>> m_ptr_column2_index;
    std::vector<index_elm> &m_column2_index;
public:
    // don't every use these outside this class unless you know what you are doing!
    // this is what I need to say here...why change to public if they will never
    // be used?  Because I feel like using them to make something else fast
    std::shared_ptr<std::unordered_map<jefastKey_t, std::vector<jefastKey_t>>> 
        m_ptr_column1_fast_index;
    std::unordered_map<jefastKey_t, std::vector<jefastKey_t> > &m_column1_fast_index;

    std::shared_ptr<std::unordered_map<jefastKey_t, std::vector<jefastKey_t>>>
        m_ptr_column2_fast_index;
    std::unordered_map<jefastKey_t, std::vector<jefastKey_t> > &m_column2_fast_index;
    
    std::shared_ptr<std::vector<jefastKey_t>> m_ptr_uniquecolumn1;
    std::vector<jefastKey_t> &m_uniquecolumn1;

    std::shared_ptr<std::vector<jefastKey_t>> m_ptr_uniquecolumn2;
    std::vector<jefastKey_t> &m_uniquecolumn2;

    // first indexed by column 1 value, next by column 2 value.  Returns index of item
    std::unordered_map<jefastKey_t, std::unordered_map<jefastKey_t, jefastKey_t> > m_full_lookup_index;

    friend class TableGeneric_encap;
};

template<class AugmenterType>
class TableGenericImpl: public TableGenericBase {
private:
    typedef decltype(AugmenterType()('|', "")) AuxcolType;

    static constexpr bool hasAugmenter = !std::is_same<std::decay_t<AuxcolType>, std::tuple<>>::value;
public:
    TableGenericImpl(std::string input_file, char delim, int column1, int column2,
            AugmenterType aug = AugmenterType()):
    TableGenericBase(column1 == column2), 
    m_ptr_auxcols(std::make_shared<std::vector<AuxcolType>>()),
    m_auxcols(*m_ptr_auxcols) {

        std::ifstream i_f;
        i_f.open(input_file);
        std::string buffer;
        while (std::getline(i_f, buffer))
        {
            // find the item for column 1
            if (column1 >= 0)
            {
                size_t idx = 0;
                for (int i = 1; i < column1; i++)
                {
                    idx = buffer.find(delim, idx);
                    idx++;
                }
                size_t idx_l = buffer.find(delim, idx);
                //if (idx_l == std::string::npos)
                //    idx_l = buffer.size() - 1;
                
                m_column1.push_back(std::stoll(buffer.substr(idx, idx_l - idx)));
            }


            // find the item for column 2
            if(column2 >= 0 && column1 != column2)
            {
                size_t idx = 0;
                for (int i = 1; i < column2; i++)
                {
                    idx = buffer.find(delim, idx);
                    ++idx;
                }
                size_t idx_l = buffer.find(delim, idx);
                //if (idx_l == std::string::npos)
                //    idx_l = buffer.size() - 1;
                
                m_column2.push_back(std::stoll(buffer.substr(idx, idx_l - idx)));
            }

            if (hasAugmenter) {
                m_auxcols.push_back(aug(delim, buffer));
            }
        }
    }

    void filter_column(int column, int greater_than_value) {
        std::vector<jefastKey_t> &cc = (column == 1) ? m_column1 : m_column2;
        std::vector<jefastKey_t> &cc2 = (column == 1) ? m_column2 : m_column1;
        auto i = cc.begin();
        auto i2 = cc2.begin();
        auto i3 = m_auxcols.begin(), j3 = m_auxcols.begin();
        for (auto j = cc.begin(), j2 = cc2.begin();
                j != cc.end() && j2 != cc2.end(); ++j, ++j2) {
            if (*j >= greater_than_value) {
                *(i++) = *j;
                *(i2++) = *j2;
                if (hasAugmenter) {
                    *(i3++) = *j3;
                }
            }
            if (hasAugmenter) {
                ++j3;
            }
        }
        cc.erase(i, cc.end());
        cc2.erase(i2, cc2.end());
        if (hasAugmenter) {
            m_auxcols.erase(i3, m_auxcols.end());
        }
    }


    const AuxcolType &get_auxcols(int index) {
        return m_auxcols[index];
    }

    TableGenericImpl<AugmenterType>* reverse_columns12() {
        return new TableGenericImpl<AugmenterType>(
                m_ptr_column2,
                m_ptr_column1,
                m_ptr_column2_index,
                m_ptr_column1_index,
                m_ptr_column2_fast_index,
                m_ptr_column1_fast_index,
                m_ptr_uniquecolumn2,
                m_ptr_uniquecolumn1,
                m_ptr_auxcols);
    }

private:
    std::shared_ptr<std::vector<AuxcolType>> m_ptr_auxcols;
    std::vector<AuxcolType> &m_auxcols;

    TableGenericImpl(
        decltype(m_ptr_column1) ptr_column1,
        decltype(m_ptr_column2) ptr_column2,
        decltype(m_ptr_column1_index) ptr_column1_index,
        decltype(m_ptr_column2_index) ptr_column2_index,
        decltype(m_ptr_column1_fast_index) ptr_column1_fast_index,
        decltype(m_ptr_column2_fast_index) ptr_column2_fast_index,
        decltype(m_ptr_uniquecolumn1) ptr_uniquecolumn1,
        decltype(m_ptr_uniquecolumn2) ptr_uniquecolumn2,
        decltype(m_ptr_auxcols) ptr_auxcols):
    TableGenericBase(
        ptr_column1,
        ptr_column2,
        ptr_column1_index,
        ptr_column2_index,
        ptr_column1_fast_index,
        ptr_column2_fast_index,
        ptr_uniquecolumn1,
        ptr_uniquecolumn2),
    m_ptr_auxcols(ptr_auxcols),
    m_auxcols(*m_ptr_auxcols) {}

public:
    auto size_of_auxcols() -> decltype(this->m_auxcols.size()) {
        return m_auxcols.size();
    }

};

using TableGeneric = TableGenericImpl<EmptyAugmenter>;

// this encapsulates a generic table so it can be used as a 'Table' using the
// previous techniques
class TableGeneric_encap : public Table {
public:
    TableGeneric_encap(std::string input_file, char delim, int column1, int column2)
        : m_table(new TableGeneric(input_file, delim, column1, column2))
    {
    };

    TableGeneric_encap(std::shared_ptr<TableGenericBase> table)
        : m_table(table)
    {};

    int column_count()
    {
        return 2;
    }

    int64_t row_count()
    {
        return m_table->get_size();
    }

    int64_t get_int64(int64_t row, int column)
    {
        switch (column)
        {
        case 0:
            return m_table->get_column1_value(row);
            break;
        case 1:
            return m_table->get_column2_value(row);
            break;
        default:
            throw std::runtime_error("no column with that label");
        }
    }

    DATABASE_DATA_TYPES getColumnTypes(int column)
    {
        if (column >= 0 && column < column_count())
            return DATABASE_DATA_TYPES::INT64;
        else
            throw "column does not exist";
    }

    const std::vector<jfkey_t>::iterator get_key_iterator(int column)
    {
        switch (column)
        {
        case 0:
            return m_table->m_column1.begin();
            break;
        case 1:
            return m_table->m_column2.begin();
            break;
        default:
            throw "the requested column does not exist";
            break;
        }
    }
private:
    std::shared_ptr<TableGenericBase> m_table;
};


