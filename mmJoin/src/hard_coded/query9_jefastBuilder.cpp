#include "query9_jefastBuilder.h"

#include <random>
#include <chrono>

std::shared_ptr<query9_jefast> Query9_JefastBuilder::Build()
{
    std::shared_ptr<query9_jefast> builder{ new query9_jefast() };

    // do the link for join 1
    builder->mp_first.reset(new JefastLevel<jfkey_t>(m_nationTable, m_supplierTable, false));
    builder->mp_first->set_LHS_table_index(Table_Nation::N_NATIONKEY);
    builder->mp_second.reset(new JefastLevel<jfkey_t>(m_supplierTable, m_lineitemTable, false));
    builder->mp_second->set_LHS_table_index(Table_Supplier::S_SUPPKEY);
    builder->mp_final.reset(new JefastQuery9Fork(m_lineitemTable));

    // insert nation items into first join
    int nation_item_count = m_nationTable->row_count();
    for (int i = 0; i < nation_item_count; ++i) {
        int64_t LHS_value = m_nationTable->get_int64(i, Table_Nation::N_NATIONKEY);

        // insert into the graph
        builder->mp_first->InsertLHSRecord(LHS_value, i);
    }

    // insert supplier items into first and second join
    // TODO: question, is it better to fill both at the same time or do
    // fill one join at a time?
    //  -- for now we will do both sides at the same time
    int supplier_item_count = m_supplierTable->row_count();
    for (int i = 0; i < supplier_item_count; ++i) {
        int64_t RHS_value = m_supplierTable->get_int64(i, Table_Supplier::S_NATIONKEY);
        int64_t LHS_value = m_supplierTable->get_int64(i, Table_Supplier::S_SUPPKEY);

        // only do insert.  We are not currently locking vertexes
        builder->mp_first->InsertRHSRecord(RHS_value, i);
        builder->mp_second->InsertLHSRecord(LHS_value, i);
    }

    // insert data for second join and tree
    int line_item_count = m_lineitemTable->row_count();
    for (int i = 0; i < line_item_count; ++i) {
        query9ForkKey_t key2{ m_lineitemTable->get_int64(i, Table_Lineitem::L_PARTKEY)
                           , m_lineitemTable->get_int64(i, Table_Lineitem::L_SUPPKEY)
                           , m_lineitemTable->get_int64(i, Table_Lineitem::L_ORDERKEY) };
        int64_t key1 = std::get<SUPKEY_IDX>(key2);

        builder->mp_second->InsertRHSRecord(key1, i);
        //builder->mp_final->InsertLHSRecord(key2, i, key1);
    }

    // fill in the partsupp data
    int partsupp_item_count = m_partssuppTable->row_count();
    for (jfkey_t i = 0; i < partsupp_item_count; ++i) {
        query9PartsuppKey_t key{ m_partssuppTable->get_int64(i, Table_Partsupp::PS_PARTKEY)
                               , m_partssuppTable->get_int64(i, Table_Partsupp::PS_SUPPKEY) };
        
        builder->mp_final->InsertRHSRecord(JefastQuery9Fork::PARTSUPP, key, i);
    }

    // fill in the orders data
    int orders_item_count = m_ordersTable->row_count();
    for (jfkey_t i = 0; i < orders_item_count; ++i) {
        int64_t key = m_ordersTable->get_int64(i, Table_Orders::O_ORDERKEY);

        builder->mp_final->InsertRHSRecord(JefastQuery9Fork::ORDERS, key, i);
    }

    // fill in the part data
    int part_item_count = m_partsTable->row_count();
    for (jfkey_t i = 0; i < part_item_count; ++i) {
        int64_t key = m_partsTable->get_int64(i, Table_Part::P_PARTKEY);

        builder->mp_final->InsertRHSRecord(JefastQuery9Fork::PART, key, i);
    }


    // fill in the weights for the tree
    //builder->mp_final->propegate_weights_internal();
    builder->mp_final->propegate_weights_prev(builder->mp_second);

    weight_t total_weight = builder->mp_first->fill_weight(builder->mp_second, builder->mp_second->get_LHS_table_index());

    builder->mp_second->optimize();
    builder->mp_first->optimize();

    // build the first level weights
    //auto enumerator = builder->mp_second->GetLHSEnumerator();
    //weight_t total_weight = 0;
    //while (enumerator->Step())
    //{
    //    builder->mp_first->AdjustRHSRecordWeight(enumerator->getValue()
    //        , enumerator->getRecordId()
    //        , enumerator->getWeight());
    //    total_weight += enumerator->getWeight();
    //}

    builder->start_weight = total_weight;

    builder->m_distribution = std::uniform_int_distribution<weight_t>(0, builder->start_weight);
    std::chrono::high_resolution_clock::duration d = std::chrono::high_resolution_clock::now().time_since_epoch();
    builder->m_generator.seed(d.count());

    return builder;
}
