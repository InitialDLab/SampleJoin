// Programmer: Robert Christensen
// email: robertc@cs.utah.edu
// Implements a filter in the jefast graph
#pragma once

#include <memory>
#include <map>
#include <cassert>

#include "Table.h"
#include "DatabaseSharedTypes.h"

class jefastEnumerator {
public:
    virtual ~jefastEnumerator()
    {};

    virtual bool Step() = 0;

    virtual jfkey_t getValue() = 0;
};

class jefastEnumerator_counter : public jefastEnumerator {
public:
    jefastEnumerator_counter(int64_t min, int64_t max, int64_t stepSize = 1)
        : m_current{ min - stepSize }
        , m_end{ max }
        , m_stepSize{ stepSize }
        , m_finished{ false }
    {};

    bool Step() {
        if (m_finished)
            return false;

        // this is to make it so we don't skip the first element when 
        // doing a step.
        m_current += m_stepSize;

        if (m_current >= m_end)
            m_finished = true;
        return !m_finished;
    }

    // return the value of what is being pointed to
    int64_t getValue()
    {
        return m_current;
    }
private:
    int64_t m_current;
    int64_t m_end;
    int64_t m_stepSize;
    bool m_finished;
};

class jefastEnumerator_int : public jefastEnumerator {
public:
    jefastEnumerator_int(std::map<int64_t, int64_t>::const_iterator start
        , std::map<int64_t, int64_t>::const_iterator end)
        : finished{ false }
        , m_observed{ 0 }
        , m_current{ start }
        , m_end{ end }
        //, finished{ false }
    {}

    bool Step() {
        if (finished)
            return false;

        // this is to make it so we don't skip the first element when 
        // doing a step.
        if (m_observed > 0)
            m_current++;

        ++m_observed;

        if (m_current == m_end)
            finished = true;
        return !finished;
    }

    // return the value of what is being pointed to
    int64_t getValue()
    {
        return m_current->second;
    }


private:
    bool finished;
    int64_t m_observed;
    std::map<int64_t, int64_t>::const_iterator m_current;
    std::map<int64_t, int64_t>::const_iterator m_end;
};

class jefastEnumerator_float : public jefastEnumerator {
public:
    jefastEnumerator_float(std::map<float, int64_t>::const_iterator start
        , std::map<float, int64_t>::const_iterator end)
        : m_observed{ 0 }
        , m_current{ start }
        , m_end{ end }
        , finished{ false }
    {}

    bool Step() {
        if (finished)
            return false;

        // this is to make it so we don't skip the first element when 
        // doing a step.
        if (m_observed > 0)
            m_current++;

        ++m_observed;
        if (m_current == m_end)
            finished = true;
        return !finished;
    }

    int64_t getValue()
    {
        return m_current->second;
    }
private:
    int64_t m_observed;
    std::map<float, int64_t>::const_iterator m_current;
    std::map<float, int64_t>::const_iterator m_end;
    bool finished;
};

class jefastFilter {
public:
    
    virtual bool Validate(jfkey_t record_id) = 0;

    // the enumerator will get the record ID of the tuple which is valid
    virtual std::shared_ptr<jefastEnumerator> getEnumerator() = 0;
};

class all_jefastFilter : public jefastFilter {
public:
    all_jefastFilter(std::shared_ptr<Table> filteredTable, int column)
        : m_filteredTable{ filteredTable }
        //, m_column{ column }
    { }

    bool Validate(jfkey_t record_id) {
        return true;
    }

    std::shared_ptr<jefastEnumerator> getEnumerator()
    {
        //auto start = m_filteredTable->get_int_index(m_column)->begin();
        //auto end = m_filteredTable->get_int_index(m_column)->end();
        //std::shared_ptr<jefastEnumerator> retVal(new jefastEnumerator_int(start, end));
        std::shared_ptr<jefastEnumerator> retVal(new jefastEnumerator_counter(0, m_filteredTable->row_count(), 1));
        return retVal;
    }
private:
    std::shared_ptr<Table> m_filteredTable;
    //int m_column;
};

class lt_int_jefastFilter : public jefastFilter {
private:

public:
    lt_int_jefastFilter(std::shared_ptr<Table> filteredTable, int column, int64_t max)
        : m_filteredTable{ filteredTable }
        , m_column{ column }
        , m_max{ max }
    { }

    bool Validate(jfkey_t record_id) {
        if (m_filteredTable->get_int64(record_id, m_column) < m_max)
            return true;
        else
            return false;            
    }

    std::shared_ptr<jefastEnumerator> getEnumerator()
    {
        auto start = m_filteredTable->get_int_index(m_column)->begin();
        auto end = m_filteredTable->get_int_index(m_column)->upper_bound(m_max);
        std::shared_ptr<jefastEnumerator> retVal(new jefastEnumerator_int(start, end));
        return retVal;
    }
private:
    std::shared_ptr<Table> m_filteredTable;
    int m_column;
    int64_t m_max;
};

class gt_int_jefastFilter : public jefastFilter {
private:

public:
    gt_int_jefastFilter(std::shared_ptr<Table> filteredTable, int column, int64_t min)
        : m_filteredTable{ filteredTable }
        , m_column{ column }
        , m_min{ min }
    { }

    bool Validate(jfkey_t record_id) {
        if (m_filteredTable->get_int64(record_id, m_column) > m_min)
            return true;
        else
            return false;
    }

    std::shared_ptr<jefastEnumerator> getEnumerator()
    {
        auto start = m_filteredTable->get_int_index(m_column)->lower_bound(m_min);
        auto end = m_filteredTable->get_int_index(m_column)->end();
        std::shared_ptr<jefastEnumerator> retVal(new jefastEnumerator_int(start, end));
        return retVal;
    }
private:
    std::shared_ptr<Table> m_filteredTable;
    int m_column;
    int64_t m_min;
};

class eq_int_jefastFilter : public jefastFilter {
public:
    eq_int_jefastFilter(std::shared_ptr<Table> filteredTable, int column, int64_t value)
        : m_filteredTable{ filteredTable }
        , m_column{ column }
        , m_value{ value }
    { }

    bool Validate(jfkey_t record_id) {
        auto item = m_filteredTable->get_int64(record_id, m_column);
        if (item == m_value)
            return true;
        else
            return false;
    }

    std::shared_ptr<jefastEnumerator> getEnumerator()
    {
        auto range = m_filteredTable->get_int_index(m_column)->equal_range(m_value);
        std::shared_ptr<jefastEnumerator> retVal(new jefastEnumerator_int(range.first, range.second));
        return retVal;
    }
private:
    std::shared_ptr<Table> m_filteredTable;
    int m_column;
    int64_t m_value;
};

class gt_lt_int_jefastFilter : public jefastFilter {
private:

public:
    gt_lt_int_jefastFilter(std::shared_ptr<Table> filteredTable, int column, int64_t min, int64_t max)
        : m_filteredTable{ filteredTable }
        , m_column{ column }
        , m_min{ min }
        , m_max{ max }
    { }

    bool Validate(jfkey_t record_id) {
        auto item = m_filteredTable->get_int64(record_id, m_column);
        if (item > m_min && item < m_max)
            return true;
        else
            return false;
    }

    std::shared_ptr<jefastEnumerator> getEnumerator()
    {
        auto start = m_filteredTable->get_int_index(m_column)->lower_bound(m_min);
        auto end = m_filteredTable->get_int_index(m_column)->upper_bound(m_max);
        std::shared_ptr<jefastEnumerator> retVal(new jefastEnumerator_int(start, end));
        return retVal;
    }
private:
    std::shared_ptr<Table> m_filteredTable;
    int m_column;
    int64_t m_min;
    int64_t m_max;
};

class gt_float_jefastFilter : public jefastFilter {
private:

public:
    gt_float_jefastFilter(std::shared_ptr<Table> filteredTable, int column, float min)
        : m_filteredTable{ filteredTable }
        , m_column{ column }
        , m_min{ min }
    { }

    bool Validate(jfkey_t record_id) {
        if (m_filteredTable->get_float(record_id, m_column) > m_min)
            return true;
        else
            return false;
    }

    std::shared_ptr<jefastEnumerator> getEnumerator()
    {
        auto start = m_filteredTable->get_float_index(m_column)->lower_bound(m_min);
        auto end = m_filteredTable->get_float_index(m_column)->end();
        std::shared_ptr<jefastEnumerator> retVal(new jefastEnumerator_float(start, end));
        return retVal;
    }
private:
    std::shared_ptr<Table> m_filteredTable;
    int m_column;
    float m_min;
};