// Programmer: Robert Christensen
// email: robertc@cs.utah.edu
// Implements a special builder for jefast which is specific for 
// query9 in the TCPH benchmark
#pragma once

#include <vector>
#include <map>
#include <random>
#include <memory>

#include "../database/jefastVertex.h"
#include "../database/jefastLevel.h"
#include "../database/Table.h"
#include "../database/TableNation.h"
#include "../database/TableSupplier.h"
#include "../database/TableLineitem.h"
#include "../database/TablePartsupp.h"
#include "../database/TableOrders.h"
#include "../database/TablePart.h"

#include "query9_jefast.h"

class Query9_JefastBuilder {
public:
    Query9_JefastBuilder(std::shared_ptr<Table> nationTable
                       , std::shared_ptr<Table> supplierTable
                       , std::shared_ptr<Table> lineitemTable
                       , std::shared_ptr<Table> partsuppTable
                       , std::shared_ptr<Table> ordersTable
                       , std::shared_ptr<Table> partsTable)
        : m_nationTable{ nationTable }
        , m_supplierTable{ supplierTable }
        , m_lineitemTable{ lineitemTable }
        , m_partssuppTable{ partsuppTable }
        , m_ordersTable{ ordersTable }
        , m_partsTable{ partsTable }
    {};

    std::shared_ptr<query9_jefast> Build();

private:
    std::shared_ptr<Table> m_nationTable;
    std::shared_ptr<Table> m_supplierTable;
    std::shared_ptr<Table> m_lineitemTable;
    std::shared_ptr<Table> m_partssuppTable;
    std::shared_ptr<Table> m_ordersTable;
    std::shared_ptr<Table> m_partsTable;

    // constants for which columns to perform a join.
    // JOIN 1
    //  nation::N_NATIONKEY == supplier::S_NATIONKEY
    // JOIN 2
    //  supplier::S_SUPPKEY == lineitem::L_SUPPKEY
    // JOIN 3 (fork)
    //  1) lineitem::L_SUPPKEY == partsupp::PS_SUPPKEY and lineitem::L_PARTKEY == partsupp::PS_PARTKEY
    //  2) lineitem::L_ORDERKEY == orders::O_ORDERKEY
    //  3) lineitem::L_PARTKEY == part::P_PARTKEY
};