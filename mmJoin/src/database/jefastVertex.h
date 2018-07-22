// Programmer: Robert Christensen
// email: robertc@cs.utah.edu
// Implements a vertex in the jefast graph
#pragma once

#include <vector>
#include <assert.h>
#include <memory>
#include <iterator>
#include <algorithm>
#include <iostream>
#include "DatabaseSharedTypes.h"

class JefastVertexEnumerator {
public:
    virtual ~JefastVertexEnumerator()
    {};

    // return true if we make a step
    virtual bool Step() = 0;

    virtual int64_t getValue() = 0;
    virtual int64_t getRecordId() = 0;

    // set the weight of the record currently being observed
    virtual void setWeight(weight_t w) = 0;
    virtual weight_t getWeight() = 0;
};


class JefastVertex
{
public:
    JefastVertex()
        : m_weight{ 0 }
        , mp_matching_rhs_record_weight{ new std::vector<weight_t> }
    {  
        /* no other setup needed */  
    }

    ~JefastVertex()
    {};
    
    JefastVertex(const JefastVertex& other){
        this->m_weight = other.m_weight;
        this->m_matching_lhs_record_ids = other.m_matching_lhs_record_ids;
        this->m_matching_rhs_record_ids = other.m_matching_rhs_record_ids;
        if(other.mp_matching_rhs_record_weight != nullptr) {
            mp_matching_rhs_record_weight.reset(new std::vector<weight_t>(*other.mp_matching_rhs_record_weight));
        }
        std::cout << '.';
    };

    JefastVertex(bool useDefaultWeight)
        : m_weight{ 0 }
        , mp_matching_rhs_record_weight{ nullptr }
    {
        if (!useDefaultWeight) {
            mp_matching_rhs_record_weight.reset(new std::vector<weight_t>);
        }
    }

    weight_t getWeight() {
        if (mp_matching_rhs_record_weight == nullptr)
            return get_RHS_outdegree();
        else
            return m_weight;
    }

    size_t get_LHS_outdegree() const {
        return m_matching_lhs_record_ids.size();
    }

    size_t get_RHS_outdegree() const {
        return m_matching_rhs_record_ids.size();
    }

    void insert_lhs_record_ids(jfkey_t record_id) {
        m_matching_lhs_record_ids.push_back(record_id);
    }

    void delete_lhs_record_ids(jfkey_t record_id) {
        auto itr = std::find(m_matching_lhs_record_ids.begin(), m_matching_lhs_record_ids.end(), record_id);
        if(itr != m_matching_lhs_record_ids.end())
            m_matching_lhs_record_ids.erase(itr);
    }

    void insert_rhs_record_ids(jfkey_t record_id) {
        m_matching_rhs_record_ids.push_back(record_id);
    }

    void adjust_rhs_record_weight(jfkey_t record_id, weight_t weight) {
        // verify the weight vector is the right size
        mp_matching_rhs_record_weight->resize(get_RHS_outdegree());

            // do a slow search for the elements with that record_id
        for (size_t i = 0; i < m_matching_rhs_record_ids.size(); ++i) {
            if (m_matching_rhs_record_ids[i] == record_id) {
                m_weight += (weight - mp_matching_rhs_record_weight->at(i));
                mp_matching_rhs_record_weight->at(i) = weight;
                return;
            }
        }
        // if we get to this point we could not find the element in the vertex.
    }

    weight_t insert_rhs_record_weight_with_sum(jfkey_t record_id, weight_t new_weight) {
        // insert at the end of the list
        m_matching_rhs_record_ids.push_back(record_id);
        if (mp_matching_rhs_record_weight != nullptr) {
            mp_matching_rhs_record_weight->push_back(new_weight + mp_matching_rhs_record_weight->back());
            this->m_weight += new_weight;
        }

        return this->m_weight;
    }

    // for delete we will place a tombstone value and mark it with 0 weight.
    weight_t delete_rhs_record_weight_with_sum(jfkey_t record_id) {
        auto itr = std::find(m_matching_rhs_record_ids.begin(), m_matching_rhs_record_ids.end(), record_id);
        // if the record weight pointer is null, we must remove the value
        if (mp_matching_rhs_record_weight == nullptr) {
            m_matching_rhs_record_ids.erase(itr);
            --this->m_weight;
            return this->m_weight;
        }
        else {
            adjust_rhs_record_weight_with_sum(record_id, 0);
            *itr = -1;
            return this->m_weight;
        }
    }
    
    // returns the new total weight of this vertex
    weight_t adjust_rhs_record_weight_with_sum(jfkey_t record_id, weight_t new_weight) {
        // we assume the weight vector is the correct size

        for (size_t i = 0; i < m_matching_rhs_record_ids.size(); ++i) {
            if (m_matching_rhs_record_ids[i] == record_id) {
                // adjust the weight

                // special case where i==size
                if (i == m_matching_rhs_record_ids.size() - 1) {
                    weight_t weight_value = this->m_weight - mp_matching_rhs_record_weight->at(i);
                    weight_t weight_diff = new_weight - weight_value;
                    this->m_weight += weight_diff;
                    return this->m_weight;
                }
                else { // regular case
                    weight_t weight_value = mp_matching_rhs_record_weight->at(i + 1) - mp_matching_rhs_record_weight->at(i);
                    weight_t weight_diff = new_weight - weight_value;
                    //std::cout << "total weight=" << this->m_weight << " weight=" << weight_value << " weight_diff=" << weight_diff << std::endl;
                    // adjust the prefix sum for all future values
                    std::transform(mp_matching_rhs_record_weight->begin() + i + 1
                        , mp_matching_rhs_record_weight->end()
                        , mp_matching_rhs_record_weight->begin() + i + 1
                        , [weight_diff](weight_t t) {return t + weight_diff;}
                    );
                    this->m_weight += weight_diff;
                    return this->m_weight;
                }
            }
        }
        return this->m_weight;
    }

    void get_records(weight_t &inout_weight_condition, jfkey_t &out_record_id) {
        //weight_t counter = inout_weight_condition;

        //assert(counter < m_weight);

        if (mp_matching_rhs_record_weight != nullptr) {

            // special case
            if (mp_matching_rhs_record_weight->size() == 1) {
                 out_record_id = m_matching_rhs_record_ids[0];
                return;
            }


            auto w_itr = std::upper_bound(mp_matching_rhs_record_weight->begin(), mp_matching_rhs_record_weight->end(), inout_weight_condition);
            --w_itr;  // we will find the weight we need, but the index will be one less
            size_t index = w_itr - mp_matching_rhs_record_weight->begin();

            inout_weight_condition -= *w_itr;
            out_record_id = m_matching_rhs_record_ids[index];
            return;
        }
        else {
            // if we are using default weights we do not need to search though the lists
            out_record_id = m_matching_rhs_record_ids[inout_weight_condition];
            inout_weight_condition -= inout_weight_condition;
            return;
        }
    }

    std::unique_ptr<JefastVertexEnumerator> getLHSEnumerator()
    {
        std::unique_ptr<JefastVertexEnumerator> toRet;
        toRet.reset(new JefastVertexEnumeratorLHS(this));
        return toRet;
    }

    std::unique_ptr<JefastVertexEnumerator> getRHSEnumerator()
    {
        std::unique_ptr<JefastVertexEnumerator> toRet;
        toRet.reset(new JefastVertexEnumeratorRHS(this));
        return toRet;
    }

    // the way we do a sort is to pack all the data into a sort buffer, sort, than
    // copy the data out of the sort buffer back into the columns.  This makes
    // it much more easy to perform the sort, rather than trying to construct some
    // other abstraction to do the sort.
    void sort()
    {
        std::vector<key_weight_pair> data;
        data.resize(m_matching_rhs_record_ids.size());

        {
            // copy the data in
            auto record_itr = m_matching_rhs_record_ids.begin();
            auto weight_itr = mp_matching_rhs_record_weight->begin();
            auto input_itr = data.begin();
            auto record_itr_end = m_matching_rhs_record_ids.end();
            while (record_itr != record_itr_end)
            {
                input_itr->key = *record_itr;
                input_itr->weight = *weight_itr;
                ++record_itr;
                ++weight_itr;
                ++input_itr;
            }
        }

        // perform the sort operation
        std::sort(data.begin(), data.end(), [](key_weight_pair a, key_weight_pair b) {return a.weight > b.weight;});
        
        {
            // copy the data back
            auto record_itr = m_matching_rhs_record_ids.begin();
            auto weight_itr = mp_matching_rhs_record_weight->begin();
            auto input_itr = data.begin();
            auto record_itr_end = m_matching_rhs_record_ids.end();
            while (record_itr != record_itr_end)
            {
                *record_itr = input_itr->key;
                *weight_itr = input_itr->weight;
                ++record_itr;
                ++weight_itr;
                ++input_itr;
            }
        }
    }

    void SetupPrefixSum()
    {
        auto weight_itr = mp_matching_rhs_record_weight->begin();
        weight_t sum = 0;
        weight_t tmp;
        while (weight_itr != mp_matching_rhs_record_weight->end()) {
            tmp = *weight_itr;
            *weight_itr = sum;
            sum += tmp;
            ++weight_itr;
        }
    }

private:

    weight_t m_weight;

    std::vector<jfkey_t> m_matching_lhs_record_ids;

    std::vector<jfkey_t> m_matching_rhs_record_ids;

    std::unique_ptr<std::vector<weight_t> > mp_matching_rhs_record_weight;

    class JefastVertexEnumeratorRHS : public JefastVertexEnumerator {
    public:
        JefastVertexEnumeratorRHS(JefastVertex *vtx)
            : mp_vtx{ vtx }
            , m_idx(-1)
        { }

        // return true if we make a step
        bool Step()
        {
            ++m_idx;
            return (m_idx < mp_vtx->m_matching_rhs_record_ids.size());
        }

        int64_t getValue()
        {
            throw "unsupported feature";
            //return mp_vtx->m_matching_rhs_record_next_value.at(m_idx);
        }
        int64_t getRecordId()
        {
            return mp_vtx->m_matching_rhs_record_ids.at(m_idx);
        }

        void setWeight(weight_t w)
        {
            // verify the weight list is the correct size
            mp_vtx->mp_matching_rhs_record_weight->resize(mp_vtx->get_RHS_outdegree());

            mp_vtx->m_weight += (w - mp_vtx->mp_matching_rhs_record_weight->at(m_idx));
            mp_vtx->mp_matching_rhs_record_weight->at(m_idx) = w;
        }

        weight_t getWeight()
        {
            return mp_vtx->mp_matching_rhs_record_weight->at(m_idx);
        }
    private:
        JefastVertex *mp_vtx;
        int m_idx;
    };

    class JefastVertexEnumeratorLHS : public JefastVertexEnumerator {
    public:
        JefastVertexEnumeratorLHS(JefastVertex *vtx)
            : mp_vtx{ vtx }
            , m_idx( -1 )
        { }

        // return true if we make a step
        bool Step()
        {
            ++m_idx;
            return (m_idx < mp_vtx->m_matching_lhs_record_ids.size());
        }

        int64_t getValue()
        {
            throw "Unsupported feature";
            //return mp_vtx->m_matching_lhs_record_prev_value.at(m_idx);
        }
        int64_t getRecordId()
        {
            return mp_vtx->m_matching_lhs_record_ids.at(m_idx);
        }

        void setWeight(weight_t w)
        {
            throw "not implemented";
        }

        weight_t getWeight()
        {
            throw "not implemented";
        }
    private:
        JefastVertex *mp_vtx;
        int m_idx;
    };

    struct key_weight_pair {
        jfkey_t key;
        weight_t weight;
    };
};