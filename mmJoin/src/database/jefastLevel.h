// Programmer: Robert Christensen
// email: robertc@cs.utah.edu
// Implements a level in the jefast graph
#pragma once

#include <vector>
#include <map>
#include <memory>
#include <algorithm>

#include <iostream>

//#include "cpp-btree/btree_map.h"

#include "Table.h"
#include "jefastFilter.h"
#include "jefastVertex.h"
#include "DatabaseSharedTypes.h"

//typedef std::map<jfkey_t, std::shared_ptr<JefastVertex> > internal_map;
//typedef btree::btree_map<jfkey_t, std::shared_ptr<JefastVertex> > internal_map;
typedef std::pair<jfkey_t, std::shared_ptr<JefastVertex> > map_pair_t;
typedef std::unordered_map<jfkey_t, std::shared_ptr<JefastVertex> > internal_map;


class JefastLevelEnumerator {
public:
    
    virtual ~JefastLevelEnumerator()
    {};

    // return true if we make a step
    virtual bool Step() = 0;

    virtual int64_t getValue() = 0;
    virtual int64_t getRecordId() = 0;
    virtual int64_t getWeight() = 0;
    virtual int64_t getVertexValue() = 0;
};

class JefastLevelEnumeratorValue : public jefastEnumerator {
public:
    JefastLevelEnumeratorValue(internal_map::iterator start
        , internal_map::iterator end)
        : m_current{ start }
        , m_end{ end }
        , m_currentState{ STATE::STARTING }
    {
        if (start == end)
            m_currentState = STATE::ENDED;
    }

    bool Step()
    {
        // I don't like to use trickery as I have done here, but I wanted to
        // have a fast way to detect where we are quickly jump to where we
        // need to go.  Since the switch is small, I justified that one can
        // easily read the code to understand what is going on.

        switch (m_currentState)
        {
        case STATE::RUNNING:

            // else, we have to find a new value for observer
            if (++m_current != m_end)
            {
        case STATE::STARTING:
                m_currentState = STATE::RUNNING;
                return true;
            }

            m_currentState = STATE::ENDED;
        case STATE::ENDED:
        default:
            return false;
        }
    }

    jfkey_t getValue() {
        return m_current->first;
    };

private:
    internal_map::iterator m_current;
    internal_map::iterator m_end;
    enum STATE { STARTING, RUNNING, ENDED };
    STATE m_currentState;
};

// JefastVertex<jfkey_t>
template <typename VertexType_t>
class JefastLevelEnumeratorLHS : public JefastLevelEnumerator {
public:
    typedef internal_map map_t;

    JefastLevelEnumeratorLHS(typename map_t::iterator start
        , typename map_t::iterator end)
        : m_current{ start }
        , m_end{ end }
        , m_currentState{ STATE::STARTING }
    {
        if (start == end)
            m_currentState = STATE::ENDED;
    }

    ~JefastLevelEnumeratorLHS()
    {};

    bool Step()
    {
        // I don't like to use trickery as I have done here, but I wanted to
        // have a fast way to detect where we are quickly jump to where we
        // need to go.  Since the switch is small, I justified that one can
        // easily read the code to understand what is going on.

        switch (m_currentState)
        {
        case STATE::RUNNING:
            // if observer can step, do it
            if (m_observer->Step())
                return true;

            // else, we have to find a new value for observer
            while (++m_current != m_end)
            {
        case STATE::STARTING:
                if (m_current->second->get_LHS_outdegree() != 0)
                {
                    m_observer = m_current->second->getLHSEnumerator();
                    m_observer->Step();
                    m_currWeight = m_current->second->getWeight();
                    m_currentState = STATE::RUNNING;
                    return true;
                }
            }

            m_currentState = STATE::ENDED;
        case STATE::ENDED:
        default:
            return false;
        }
    }

    int64_t getValue()
    {
        return m_observer->getValue();
    }
    // get the value of the currently visited vertex
    int64_t getVertexValue()
    {
        return m_current->first;
    }
    int64_t getRecordId()
    {
        return m_observer->getRecordId();
    }
    int64_t getWeight()
    {
        return m_currWeight;
    }
private:
    typename map_t::iterator m_current;
    typename map_t::iterator m_end;
    std::unique_ptr<JefastVertexEnumerator> m_observer;
    weight_t m_currWeight;
    enum STATE { STARTING, RUNNING, ENDED };
    STATE m_currentState;
};

template <typename VertexType_t>
class JefastLevelEnumeratorRHS : public JefastLevelEnumerator {
public:
    typedef internal_map map_t;

    JefastLevelEnumeratorRHS(typename map_t::iterator start
        , typename map_t::iterator end)
        : m_current{ start }
        , m_end{ end }
        , m_currentState{ STATE::STARTING }
    {
        if (start == end)
            m_currentState = STATE::ENDED;
    }

    ~JefastLevelEnumeratorRHS()
    {};

    bool Step()
    {
        // I don't like to use trickery as I have done here, but I wanted to
        // have a fast way to detect where we are quickly jump to where we
        // need to go.  Since the switch is small, I justified that one can
        // easily read the code to understand what is going on.

        switch (m_currentState)
        {
        case STATE::RUNNING:
            // if observer can step, do it
            if (m_observer->Step())
                return true;

            // else, we have to find a new value for observer
            while (++m_current != m_end)
            {
        case STATE::STARTING:
            if (m_current->second->get_RHS_outdegree() != 0)
            {
                m_observer = m_current->second->getRHSEnumerator();
                m_observer->Step();
                m_currWeight = m_current->second->getWeight();
                m_currentState = STATE::RUNNING;
                return true;
            }
            }

            m_currentState = STATE::ENDED;
        case STATE::ENDED:
        default:
            return false;
        }
    }

    int64_t getValue()
    {
        return m_observer->getValue();
    }
    // get the value of the currently visited vertex
    int64_t getVertexValue()
    {
        return m_current->first;
    }
    int64_t getRecordId()
    {
        return m_observer->getRecordId();
    }
    int64_t getWeight()
    {
        return m_currWeight;
    }
    void setWeight(weight_t w)
    {
        m_observer->setWeight(w);
    }
private:
    typename map_t::iterator m_current;
    typename map_t::iterator m_end;
    std::unique_ptr<JefastVertexEnumerator> m_observer;
    weight_t m_currWeight;
    enum STATE { STARTING, RUNNING, ENDED };
    STATE m_currentState;
};

//template <typename pointer_t>
template <class next_value_t>
class JefastLevel {
public:
    JefastLevel(std::shared_ptr<Table> LHS_table, std::shared_ptr<Table> RHS_table, bool useDefaultWeight)
        : m_NewVertexLocked{ false }
        , mp_LHS_Table{LHS_table}
        , mp_RHS_Table{RHS_table}
        , m_useDefaultVertexWeight{ useDefaultWeight }
        , m_LHS_Table_index{ -1 }
        , m_RHS_Table_index{ -1 }
        , m_optimized{ false }
    { }

    // adds a new filter to the jefast level
    // returns the number of filters associated with this level
    // after adding the filter
    size_t AddLHSFilter(std::shared_ptr<jefastFilter> filter) {
        m_LHS_filters.push_back(filter);
        return m_LHS_filters.size();
    }

    size_t AddRHSFilter(std::shared_ptr<jefastFilter> filter) {
        m_RHS_filters.push_back(filter);
        return m_RHS_filters.size();
    }

    void LockNewVertex() {
        m_NewVertexLocked = true;
    }

    bool isLockedVertex() {
        return m_NewVertexLocked;
    }

    std::shared_ptr<Table> get_LHS_Table() {
        return mp_LHS_Table;
    }

    std::shared_ptr<Table> get_RHS_Table() {
        return mp_RHS_Table;
    }

    int get_LHS_table_index() {
        return m_LHS_Table_index;
    }

    // the join index for the 'left' table in the join
    int get_index_for_LHS_table() {
        return m_RHS_Table_index;
    }

    // the join index for the 'right' table in the join
    int get_index_for_RHS_table() {
        return m_LHS_Table_index;
    }

    int get_RHS_table_index() {
        return m_RHS_Table_index;
    }

    void set_LHS_table_index(int value) {
        m_LHS_Table_index = value;
    }

    void set_RHS_table_index(int value) {
        m_RHS_Table_index = value;
    }

    // used when traversing the level to lookup a join result.
    // id - the input value for the current level
    // inout_weight - a counter to indicate which path to go down.  will be updated on return
    // out_key - the key of the LHS item in the join
    // out_next - the value of the next level to traverse.
    void GetNextStep(jfkey_t id, weight_t &inout_weight, jfkey_t &out_key) {
        m_data.find(id)->second->get_records(inout_weight, out_key);
    }

    void GetStartPairStep(weight_t &inout_weight, jfkey_t &out_key1, jfkey_t &out_key2) {
        // find the pair for the weight
        auto w_itr = std::upper_bound(m_searchWeights.begin(), m_searchWeights.end(), inout_weight);
        --w_itr; // we will find the record +1, so we need to correct.
        size_t index = w_itr - m_searchWeights.begin();

        inout_weight -= *w_itr;

        auto record = m_data.find(m_indexes[index]);
        

        // correct if there are multiple possible starting values
        int LHS_record = inout_weight / record->second->getWeight();
        inout_weight -= (LHS_record) * record->second->getWeight();

        auto temp = record->second->getLHSEnumerator();
        for (;LHS_record >= 0; --LHS_record)
            temp->Step();

        out_key1 = temp->getRecordId();
        record->second->get_records(inout_weight, out_key2);
    }

    // inert a new item on the LHS of the join level.  Return true if we created something.
    bool InsertLHSRecord(jfkey_t value, jfkey_t LHS_recordId) {
        //auto search = m_data.lower_bound(value);
        auto search = m_data.find(value);
        //if (search!= m_data.end() && search->first == value) {
        if(search != m_data.end()){
            search->second->insert_lhs_record_ids(LHS_recordId);
            return true;
        }
        else if (!m_NewVertexLocked) {
            // otherwise, we need to insert a new value using a hint.
            std::shared_ptr<JefastVertex> tmp(new JefastVertex(m_useDefaultVertexWeight));
            map_pair_t t_pair(value, std::move(tmp));
            //auto insertedValue = m_data.insert(search, t_pair);
            auto insertedValue = m_data.insert(t_pair);
            insertedValue.first->second->insert_lhs_record_ids(LHS_recordId);
            return true;
        }
        else {
            return false;
        }
    }

    bool InsertRHSRecord(jfkey_t value, jfkey_t RHS_recordId) {
        //auto search = m_data.lower_bound(value);
        auto search = m_data.find(value);
        //if (search != m_data.end() && search->first == value) {
        if(search != m_data.end()){
            // if it is equal, that means we found the object
            search->second->insert_rhs_record_ids(RHS_recordId);
            return true;
        }
        else if (!m_NewVertexLocked) {
            // otherwise, we need to insert a new value using a hint.
            std::shared_ptr<JefastVertex> tmp(new JefastVertex(m_useDefaultVertexWeight));
            map_pair_t t_pair(value, std::move(tmp));
            //auto insertedValue = m_data.insert(search, t_pair);
            auto insertedValue = m_data.insert(t_pair);
            insertedValue.first->second->insert_rhs_record_ids(RHS_recordId);
            return true;
        }
        else {
            return false;
        }
    }

    bool AdjustRHSRecordWeight(jfkey_t value, jfkey_t RHS_recordId, weight_t weight) {
        auto search = m_data.find(value);
        if (search != m_data.end()) {
            search->second->adjust_rhs_record_weight(RHS_recordId, weight);
            return true;
        }
        // otherwise it does not exist.  We don't create new elements here if we 
        // could not find it.
        return false;
    }

    weight_t GetLevelWeight() {
        auto start = m_data.begin();
        auto end = m_data.end();
        weight_t weight_counter = 0;

        while (start != end)
        {
            // for the level weight, we should adjust the total weight by the indegree.
            // this number will give you what you would use to query this level if you were starting here.
            //std::cout << start->second->getWeight() << std::endl;
            weight_counter += start->second->getWeight() * start->second->get_LHS_outdegree();
            ++start;
        }

        return weight_counter;
    }

    // return true if the vertex exists in this level
    bool DoesVertexExist(jfkey_t value) {
        return m_data.count(value) > 0;
    };

    std::shared_ptr<JefastVertex> getVertex(jfkey_t value)
    {
        return m_data.find(value)->second;
    }

    std::unique_ptr<JefastLevelEnumerator> GetLHSEnumerator()
    {
        return std::unique_ptr<JefastLevelEnumerator>(new JefastLevelEnumeratorLHS<next_value_t>(this->m_data.begin(), this->m_data.end()));
    }

    std::unique_ptr<jefastEnumerator> GetVertexValueEnumerator()
    {
        return std::unique_ptr<jefastEnumerator>(new JefastLevelEnumeratorValue(this->m_data.begin(), this->m_data.end()));
    }

    std::unique_ptr<JefastLevelEnumeratorRHS<next_value_t> > GetRHSEnumerator()
    {
        return std::unique_ptr<JefastLevelEnumeratorRHS<next_value_t> >(new JefastLevelEnumeratorRHS<next_value_t>(this->m_data.begin(), this->m_data.end()));
    }

    weight_t fill_weight(std::shared_ptr<JefastLevel<next_value_t> > nextLevel, int nextLevelIndex)
    {
        // iterate though all RHS items in this level
        auto iter = std::unique_ptr<JefastLevelEnumeratorRHS<next_value_t> >(new JefastLevelEnumeratorRHS<next_value_t>(this->m_data.begin(), this->m_data.end()));

        weight_t counter = 0;

        auto table = mp_RHS_Table->get_key_iterator(nextLevelIndex);

        while (iter->Step())
        {
            // what is the current index I am looking at
            jfkey_t recordId = iter->getRecordId();

            // what is the next value we need to look at
            //jfkey_t recordValue = mp_RHS_Table->get_int64(recordId, nextLevelIndex);
            jfkey_t recordValue = *(table + recordId);

            // find the vertex in the next level with that value and pull the weight
            // from that value.
            auto i = nextLevel->m_data.find(recordValue);
            if (i == nextLevel->m_data.end())
                continue;

            weight_t w = i->second->getWeight();
            iter->setWeight(w);
            counter += w;
        }

        return counter;
        //return 0;
    }

    // performs an optimize step to try to store the data
    // better for queries
    void optimize() {
        if (m_optimized)
            throw "already optimized!";

        for (auto itr = m_data.begin(); itr != m_data.end(); ++itr)
        {
            itr->second->sort();
            itr->second->SetupPrefixSum();
        }
        m_optimized = true;
    }

    bool is_optimized() {
        return m_optimized;
    }

    size_t getMaxOutdegree() {
        size_t max = 0;

        //max = std::max_element(m_data.begin(), m_data.end(),
        //    [](std::pair<jfkey_t, JefastVertex> &x, std::pair<jfkey_t, JefastVertex> &y)
        //{ return x.second.get_RHS_outdegree() > y.second.get_RHS_outdegree();});
        for (auto &v : m_data) {
            max = std::max(max, v.second->get_RHS_outdegree());
        }

        return max;
    }

    void build_starting() {
        //if (m_searchWeights.size() != 0 || m_indexes.size() != 0)
        //    return;

        struct key_itr_pair {
            weight_t weight;
            //internal_map::iterator itr;
            jefastKey_t value;
        };

        std::vector<key_itr_pair> temp_data;
        temp_data.resize(m_data.size());

        auto temp_data_i = temp_data.begin();
        for (auto i = m_data.begin(); i != m_data.end(); ++i, ++temp_data_i) {
            temp_data_i->weight = i->second->getWeight() * i->second->get_LHS_outdegree();
            temp_data_i->value = i->first;
        }

        // sort by weight
        std::sort(temp_data.begin(), temp_data.end(), [](key_itr_pair a, key_itr_pair b) {return a.weight > b.weight;});

        // copy data back out
        {
            m_searchWeights.resize(temp_data.size());
            m_indexes.resize(temp_data.size());
            weight_t sum = 0;
            auto weights_i = m_searchWeights.begin();
            auto indexes_i = m_indexes.begin();
            for (auto i = temp_data.begin(); i != temp_data.end(); ++i, ++weights_i, ++indexes_i) {
                *weights_i = sum;
                *indexes_i = i->value;
                sum += i->weight;
            }
        }
    }

    void dump_weights() {
        for (int i = 0; i < m_searchWeights.size(); ++i)
        {
            std::cout << "idx: " << m_indexes[i] << " w=" << m_searchWeights[i] << '\n';
        }
        std::cout.flush();
    }

    void update_search_weight(jefastKey_t key, weight_t new_weight) {
        auto index = std::find(m_indexes.begin(), m_indexes.end(), key);
        if (index + 1 == m_indexes.end())
            return;

        weight_t current_weight = *(index + 1) - *index;
        weight_t weight_adjust = current_weight - new_weight;

        auto i_val = index - m_indexes.begin();

        std::transform(m_searchWeights.begin() + i_val
            , m_searchWeights.end()
            , m_searchWeights.begin() + i_val
            , [weight_adjust](weight_t t) {return t + weight_adjust;});
    }

private:


    // true if we don't allow for new vertexes
    bool m_optimized;

    bool m_NewVertexLocked;
    bool m_useDefaultVertexWeight;

    std::shared_ptr<Table> mp_LHS_Table;
    std::shared_ptr<Table> mp_RHS_Table;

    int m_LHS_Table_index;
    int m_RHS_Table_index;

    std::vector<weight_t> m_searchWeights;
    //std::vector<std::map<jfkey_t, JefastVertex>::iterator> m_indexes;
    std::vector<jfkey_t> m_indexes;

    // filters?
    std::vector<std::shared_ptr<jefastFilter> > m_LHS_filters;
    std::vector<std::shared_ptr<jefastFilter> > m_RHS_filters;

    internal_map m_data;

    friend class jefastBuilderWJoinAttribSelection;
    friend class jefastBuilderWNonJoinAttribSelection;
};
