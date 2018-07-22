#pragma once
#include <vector>
#include <memory>
#include <map>
#include <cinttypes>


#include "DynamicVertex.h"
#include "TableGeneric.h"

class DynamicLevel {
public:
    DynamicLevel(std::shared_ptr<TableGenericBase> rhs_table, int64_t max_AGM)
        : m_max_AGM{ max_AGM }
        , m_rhs_table{ rhs_table }
    {};

    virtual ~DynamicLevel()
    {};

    // return an iterator to the node we are pointing to (so we can perform
    // updates after we finish the search)
    std::unordered_map<jfkey_t, std::shared_ptr<DynamicVertex> >::iterator
        GetNextStep(jfkey_t id, weight_t &inout_weight, jfkey_t &out_key, int64_t &update_key, WALK_STATUS &status, double &p)
    {
        // look for the item to do the join
        auto ptr = m_data.find(id);
        if (ptr != m_data.end())
        { // in this case the mode exists
            status |= ptr->second->get_records_p(inout_weight, out_key, update_key, p);
        }
        else {
            // we need to make it, then pull the record
            auto count = m_rhs_table->count_cardinaltiy_f(id, 1);
            std::shared_ptr<DynamicVertex> temp_p(new DynamicVertex(m_max_AGM, count, id, m_rhs_table));
            std::pair<jfkey_t, std::shared_ptr<DynamicVertex> > temp(id, temp_p);
            m_data.insert(temp);
            //m_uniqueKeys.push_back(id);

            ptr = m_data.find(id);

            // if we are in a fail condition
            status |= ptr->second->get_records_p(inout_weight, out_key, update_key, p);
            if (status & FAIL)
                ptr = m_data.end();
        }

        return ptr;
    }

    // return a vertex with a particular key (or creates one if it does not exist)
    std::unordered_map<jfkey_t, std::shared_ptr<DynamicVertex> >::iterator get_vertex(jfkey_t id)
    {
        auto ptr = m_data.find(id);
        if (ptr == m_data.end())
        {
            auto count = m_rhs_table->count_cardinaltiy_f(id, 1);
            std::shared_ptr<DynamicVertex> temp_p(new DynamicVertex(m_max_AGM, count, id, m_rhs_table));
            std::pair<jfkey_t, std::shared_ptr<DynamicVertex> > temp(id, temp_p);
            m_data.insert(temp);
            //m_uniqueKeys.push_back(id);

            ptr = m_data.find(id);
        }
        return ptr;
    }


    //virtual int64_t upper_bound()
    //{
    //    // this version should never be called
    //    return 0;
    //}
//protected:
    int64_t m_max_AGM;

    std::unordered_map<jfkey_t, std::shared_ptr<DynamicVertex> > m_data;

    //std::vector<jfkey_t> m_uniqueKeys;

    std::shared_ptr<TableGenericBase> m_rhs_table;
};

class DynamicInitialLevel /* : public DynamicLevel */ {
public:
    DynamicInitialLevel(std::shared_ptr<TableGenericBase> rhs_table, int64_t max_AGM)
        //: DynamicLevel(rhs_table, max_AGM)
    {
        // setup the initial step into the graph
        std::shared_ptr<DynamicOriginVertex> temp{ new DynamicOriginVertex(max_AGM, rhs_table) };

        m_data.emplace(0, temp);
    };

    virtual ~DynamicInitialLevel()
    {};

    int64_t upper_bound()
    {
        return m_data.begin()->second->get_max_weight();
    }

    int64_t est_size()
    {
        return int64_t(m_data.begin()->second->est_size());
    }

    // return an iterator to the node we are pointing to (so we can perform
    // updates after we finish the search)
    std::unordered_map<jfkey_t, std::shared_ptr<DynamicOriginVertex> >::iterator
        GetNextStep(jfkey_t id, weight_t &inout_weight, jfkey_t &out_key, int64_t &update_key, WALK_STATUS &status, double &p)
    {
        // look for the item to do the join
        auto ptr = m_data.find(0);
        if (ptr != m_data.end())
        { // in this case the mode exists
            status |= ptr->second->get_records_p(inout_weight, out_key, update_key, p);
        }

        return ptr;
    }

    std::unordered_map<jfkey_t, std::shared_ptr<DynamicOriginVertex> >::iterator get_vertex()
    {
        auto ptr = m_data.find(0);
        return ptr;
    }

    std::unordered_map<jfkey_t, std::shared_ptr<DynamicOriginVertex> > m_data;
};
