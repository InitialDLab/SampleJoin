// This implemented the different algorithms for query5
// Programmer: Robert Christensen
// email: robertc@cs.utah.edu

#include <iostream>
#include <fstream>
#include <memory>
#include <random>
#include <map>
#include <vector>
#include <thread>
#include <future>
#include <cstring>

#include "util/Timer.h"
#include "util/FileSizeTable.h"
#include "util/FileKeyValue.h"
#include "util/accumulator.h"

#include "database/TableRegion.h"
#include "database/TableNation.h"
#include "database/TableSupplier.h"
#include "database/TablePartsupp.h"
#include "database/TableCustomer.h"
#include "database/TableOrders.h"
#include "database/TableLineitem.h"

#include "database/jefastBuilder.h"
#include "database/jefastFilter.h"
#include "database/jefastBuilderWJoinAttribSelection.h"
#include "database/jefastBuilderWNonJoinAttribSelection.h"

//SELECT n_name, s_nationkey, c_nationkey, o_custkey, l_orderkey
//FROM nation, supplier, customer, orders, lineitem
//WHERE	--n_name = 'CHINA'--
//  AND n_nationkey = s_nationkey
//  AND s_nationkey = c_nationkey
//  AND c_custkey = o_custkey
//  AND o_orderkey = l_orderkey;

static std::shared_ptr<Table> nation_table;
static std::shared_ptr<Table> supplier_table;
static std::shared_ptr<Table> customer_table;
static std::shared_ptr<Table> orders_table;
static std::shared_ptr<Table> lineitem_table;

static std::shared_ptr<JefastBuilder> jefast_index_builder;
static std::shared_ptr<jefastIndexLinear> jefast_index;
static std::shared_ptr<jefastIndexLinear> jefast_index_filtered;

static std::shared_ptr<FileKeyValue> data_map;

int64_t convertDate(std::string str) {
    time_t epoch = getEpoch();
    struct tm time;
    time.tm_hour = 0;
    time.tm_isdst = 0;
    time.tm_min = 0;
    time.tm_sec = 0;
    int year, month, day;
    
    sscanf(str.c_str(), "%4i-%2i-%2i", &year, &month, &day);
    time.tm_year = year - 1900;
    time.tm_mon = month - 1;
    time.tm_mday = day;
    return difftime(mktime(&time), epoch);
}

// Note, we will require a filter for each column.  It can just be an empty filter (an everything filter)
// we will be doing a linear scan of the data to implement this algorithm for now.
int64_t exactJoinNoIndex(std::string outfile, std::vector<std::shared_ptr<jefastFilter> > filters) {
    // implements a straightforward implementation of a join which
    // does not require an index.

    std::ofstream output_file(outfile, std::ios::out);
    int64_t count = 0;

    auto Table_1 = nation_table;
    auto Table_2 = supplier_table;
    auto Table_3 = customer_table;
    auto Table_4 = orders_table;
    auto Table_5 = lineitem_table;

    int table1Index2 = Table_Nation::N_NATIONKEY;
    int table2Index1 = Table_Supplier::S_NATIONKEY;

    int table2Index2 = Table_Supplier::S_NATIONKEY;
    int table3Index1 = Table_Customer::C_NATIONKEY;

    int table3Index2 = Table_Customer::C_CUSTKEY;
    int table4Index1 = Table_Orders::O_CUSTKEY;

    int table4Index2 = Table_Orders::O_ORDERKEY;
    int table5Index1 = Table_Lineitem::L_ORDERKEY;

    // build the hash for table 1
    std::map<jfkey_t, std::vector<int64_t> > Table1_hash;
    //int64_t Table_1_count = Table_1->row_count();
    //for (int64_t i = 0; i < Table_1_count; ++i) {
    for (auto f1_enu = filters.at(0)->getEnumerator(); f1_enu->Step();){
        Table1_hash[Table_1->get_int64(f1_enu->getValue(), table1Index2)].push_back(f1_enu->getValue());
    }

    // build the hash for table 2.  All matched elements from table 1 hash will be emitted
    // the tuple has the form <index from table 1, index for table 2> for all matching tuple
    std::map<jfkey_t, std::vector<std::tuple<int64_t, int64_t> > > Table2_hash;
    //int64_t Table_2_count = Table_2->row_count();
    //for (int64_t i = 0; i < Table_2_count; ++i) {
    for (auto f2_enu = filters.at(1)->getEnumerator(); f2_enu->Step();){
        jfkey_t value = Table_2->get_int64(f2_enu->getValue(), table2Index1);
        for (auto matching_indexes : Table1_hash[value]) {
            Table2_hash[Table_2->get_int64(f2_enu->getValue(), table2Index2)].emplace_back(matching_indexes, f2_enu->getValue());
        }
    }

    // build the hash for table 3.  All matched elements from table 2 hash will be emitted.
    std::map<jfkey_t, std::vector<std::tuple<int64_t, int64_t, int64_t> > > Table3_hash;
    //int64_t Table_3_count = Table_3->row_count();
    //for (int64_t i = 0; i < Table_3_count; ++i) {
    for (auto f3_enu = filters.at(2)->getEnumerator(); f3_enu->Step();) {
        jfkey_t value = Table_3->get_int64(f3_enu->getValue(), table3Index1);
        for (auto matching_indexes : Table2_hash[value]) {
            Table3_hash[Table_3->get_int64(f3_enu->getValue(), table3Index2)].emplace_back(std::get<0>(matching_indexes), std::get<1>(matching_indexes), f3_enu->getValue());
        }
    }

    // build the hash for table 3.  All matched elements from table 2 hash will be emitted.
    std::map<jfkey_t, std::vector<std::tuple<int64_t, int64_t, int64_t, int64_t> > > Table4_hash;
    //int64_t Table_4_count = Table_4->row_count();
    //for (int64_t i = 0; i < Table_4_count; ++i) {
    for (auto f4_enu = filters.at(3)->getEnumerator(); f4_enu->Step();){
        jfkey_t value = Table_4->get_int64(f4_enu->getValue(), table4Index1);
        for (auto matching_indexes : Table3_hash[value]) {
            Table4_hash[Table_4->get_int64(f4_enu->getValue(), table4Index2)].emplace_back(std::get<0>(matching_indexes), std::get<1>(matching_indexes), std::get<2>(matching_indexes), f4_enu->getValue());
        }
    }

    // build the hash for table 4?  No, we can just emit when we find a match in this case.
    int64_t Table_5_count = Table_5->row_count();
    //for (int64_t i = 0; i < Table_5_count; ++i) {
    for (auto f5_enu = filters.at(4)->getEnumerator(); f5_enu->Step();){
        int64_t value = Table_5->get_int64(f5_enu->getValue(), table5Index1);
        for (auto matching_indexes : Table4_hash[value]) {
            // emit the join results here.
            output_file << count << ' '
                << Table_1->get_int64(std::get<0>(matching_indexes), table1Index2) << ' '
                << Table_2->get_int64(std::get<1>(matching_indexes), table2Index2) << ' '
                << Table_3->get_int64(std::get<2>(matching_indexes), table3Index2) << ' '
                << Table_4->get_int64(std::get<3>(matching_indexes), 1) << ' '
                << Table_5->get_int64(f5_enu->getValue(), 1) << '\n';
            count++;
        }
    }

    output_file.close();
    return count;
}

int64_t randomjefastJoin(std::ofstream &output_file, int64_t output_count, std::shared_ptr<jefastIndexLinear> jefast) {
    std::vector<int64_t> results;

    auto Table_1 = nation_table;
    auto Table_2 = supplier_table;
    auto Table_3 = customer_table;
    auto Table_4 = orders_table;
    auto Table_5 = lineitem_table;

    int table1Index2 = Table_Nation::N_NATIONKEY;
    int table2Index2 = Table_Supplier::S_NATIONKEY;
    int table3Index2 = Table_Customer::C_CUSTKEY;
    int table4Index2 = Table_Orders::O_ORDERKEY;

    int64_t max = jefast->GetTotal();
    std::vector<weight_t> random_values;
    std::uniform_int_distribution<weight_t> distribution(0, max);
    random_values.resize(output_count);

    // lets try to pre-generate all the random numbers we will need.
    static std::default_random_engine generator;

    std::generate(random_values.begin(), random_values.end(), [&]() {return distribution(generator);});

    std::sort(random_values.begin(), random_values.end());

    for (int i = 0; i < output_count; ++i) {
        jefast->GetJoinNumber(random_values[i], results);

        //output_file << i << " "
        //    << Table_1->get_int64(results.at(0), table1Index2) << ' '
        //    << Table_2->get_int64(results.at(1), table2Index2) << ' '
        //    << Table_3->get_int64(results.at(2), table3Index2) << ' '
        //    << Table_4->get_int64(results.at(3), table4Index2) << ' '
        //    << Table_5->get_int64(results.at(4), 1) << '\n';
    }
    //output_file.close();
    return output_count;
}

int64_t randomjefastJoin(std::string outfile, int64_t output_count, std::shared_ptr<jefastIndexLinear> jefast){
    // this will do a full table join using the jefast datastructure
    // NOTE, jefast is not optimized for scanning right now.  It is
    //  optimized for point queries
    std::ofstream output_file(outfile, std::ios::out);

    int64_t count = randomjefastJoin(output_file, output_count, jefast);
    output_file.close();
    return count;
}

int64_t randomjefastJoinT(std::string outfile, int64_t output_count, std::shared_ptr<jefastIndexLinear> jefast) {
    return randomjefastJoin(outfile, output_count, jefast);
}

int64_t fulljefastJoin(std::string outfile, int64_t output_count){
    // this will do a full table join using the jefast data structure
    // NOTE, jefast is not optimized for scanning right now.  It is
    //  optimized for point queries
    std::ofstream output_file(outfile, std::ios::out);

    std::vector<int64_t> results;

    auto Table_1 = nation_table;
    auto Table_2 = supplier_table;
    auto Table_3 = customer_table;
    auto Table_4 = orders_table;
    auto Table_5 = lineitem_table;

    int table1Index2 = Table_Nation::N_NATIONKEY;
    int table2Index2 = Table_Supplier::S_NATIONKEY;
    int table3Index2 = Table_Customer::C_CUSTKEY;
    int table4Index2 = Table_Orders::O_ORDERKEY;

    for (int64_t i = 0; i < output_count; ++i) {
        jefast_index_filtered->GetJoinNumber(i, results);

        output_file << i << " "
            << Table_1->get_int64(results.at(0), table1Index2) << ' '
            << Table_2->get_int64(results.at(1), table2Index2) << ' '
            << Table_3->get_int64(results.at(2), table3Index2) << ' '
            << Table_4->get_int64(results.at(3), table4Index2) << ' '
            << Table_5->get_int64(results.at(4), 1) << '\n';
    }
    output_file.close();
    return output_count;
}

int64_t baselineJoin(std::ofstream &output_file, int output_count, const std::vector<weight_t> &max_cardinality) {
    // the exact join will combine between the four tables

    //std::ofstream output_file(outfile, std::ios::out);

    auto Table_1 = nation_table;
    auto Table_2 = supplier_table;
    auto Table_3 = customer_table;
    auto Table_4 = orders_table;
    auto Table_5 = lineitem_table;

    int table1Index2 = Table_Nation::N_NATIONKEY;
    int table2Index1 = Table_Supplier::S_NATIONKEY;

    int table2Index2 = Table_Supplier::S_NATIONKEY;
    int table3Index1 = Table_Customer::C_NATIONKEY;

    int table3Index2 = Table_Customer::C_CUSTKEY;
    int table4Index1 = Table_Orders::O_CUSTKEY;

    int table4Index2 = Table_Orders::O_ORDERKEY;
    int table5Index1 = Table_Lineitem::L_ORDERKEY;

    int64_t count = 0;
    int64_t rejected = 0;
    int64_t max_possible_path = 0;

    std::default_random_engine generator;



    int64_t Table1_size = Table_1->row_count();
    std::uniform_int_distribution<int64_t> region_dist(0, Table1_size - 1);
    int64_t M_value = max_cardinality[0] * max_cardinality[1] * max_cardinality[2] * max_cardinality[3];
    std::uniform_int_distribution<int64_t> rejection_filter(1, M_value);

    while (count < output_count)
    {
        // do random selection of Table 1
        int64_t Table1_idx = region_dist(generator);
        int64_t Table1_v = Table_1->get_int64(Table1_idx, table1Index2);

        // do random selection of Table 2
        auto Table2_range = Table_2->get_key_index(table2Index1)->equal_range(Table1_v);
        int64_t Table2_count = Table_2->get_key_index(table2Index1)->count(Table1_v);
        if (Table2_count == 0)
            continue;
        std::uniform_int_distribution<int64_t> Table2_dist(0, Table2_count - 1);
        int64_t selection_count_2 = Table2_dist(generator);
        auto Table2_i = Table2_range.first;
        while (selection_count_2 > 0) {
            Table2_i++;
            --selection_count_2;
        }
        int64_t Table2_v = Table_2->get_int64(Table2_i->second, table2Index2);

        // do random selection of Table3
        auto Table3_range = Table_3->get_key_index(table3Index1)->equal_range(Table2_v);
        int64_t Table3_count = Table_3->get_key_index(table3Index1)->count(Table2_v);
        if (Table3_count == 0)
            continue;
        std::uniform_int_distribution<int64_t> Table3_dist(0, Table3_count - 1);
        int64_t selection_count_3 = Table3_dist(generator);
        auto Table3_i = Table3_range.first;
        while (selection_count_3 > 0) {
            Table3_i++;
            --selection_count_3;
        }
        int64_t Table3_v = Table_3->get_int64(Table3_i->second, table3Index2);

        // do random selection of Table4
        auto Table4_range = Table_4->get_key_index(table4Index1)->equal_range(Table3_v);
        int64_t Table4_count = Table_4->get_key_index(table4Index1)->count(Table3_v);
        if (Table4_count == 0)
            continue;
        std::uniform_int_distribution<int64_t> Table4_dist(0, Table4_count - 1);
        int64_t selection_count_4 = Table4_dist(generator);
        auto Table4_i = Table4_range.first;
        while (selection_count_4 > 0) {
            Table4_i++;
            --selection_count_4;
        }
        int64_t Table4_v = Table_4->get_int64(Table4_i->second, table4Index2);

        // do random selection of Table5
        auto Table5_range = Table_5->get_key_index(table5Index1)->equal_range(Table4_v);
        int64_t Table5_count = Table_5->get_key_index(table5Index1)->count(Table4_v);
        if (Table5_count == 0)
            continue;
        std::uniform_int_distribution<int64_t> Table5_dist(0, Table5_count - 1);
        int64_t selection_count_5 = Table5_dist(generator);
        auto Table5_i = Table5_range.first;
        while (selection_count_5 > 0) {
            Table5_i++;
            --selection_count_5;
        }
        int64_t Table5_v = Table_5->get_int64(Table5_i->second, 1);

        // decide if we should reject
        int64_t possible_paths = (Table2_count * Table3_count) * (Table4_count * Table5_count);
        max_possible_path = std::max(possible_paths, max_possible_path);
        // if true, accept
        if (rejection_filter(generator) < possible_paths) {
            output_file << count << ' '
                << Table1_v << ' '
                //<< regionName << ' '
                << Table2_v << ' '
                //<< nationName << ' '
                << Table3_v << ' '
                << Table4_v << ' '
                << Table5_v << '\n';
            count++;
        }
        else {
            rejected++;
        }
    }

    output_file.close();
    return rejected;
}

int64_t baselineJoin(std::string outfile, int output_count, const std::vector<weight_t> &max_cardinality) {
    // the exact join will combine between the four tables

    std::ofstream output_file(outfile, std::ios::out);

    int count = baselineJoin(output_file, output_count, max_cardinality);

    output_file.close();
    return count;
}

void joinAttribExp(std::shared_ptr<jefastIndexLinear> jefast_base)
{
    Timer timer;

    for (auto min_orderkey : { 1000000, 2000000, 3000000, 4000000, 5000000 })
    {
        std::vector< std::shared_ptr<jefastFilter> > filters(5);
        filters.at(0) = std::shared_ptr<jefastFilter>(new all_jefastFilter(nation_table, Table_Nation::N_NATIONKEY));
        filters.at(1) = std::shared_ptr<jefastFilter>(new all_jefastFilter(supplier_table, Table_Supplier::S_SUPPKEY));
        filters.at(2) = std::shared_ptr<jefastFilter>(new all_jefastFilter(customer_table, Table_Customer::C_CUSTKEY));
        //filters.at(3) = std::shared_ptr<jefastFilter>(new all_jefastFilter(orders_table, Table_Orders::O_ORDERKEY));
        filters.at(3) = std::shared_ptr<jefastFilter>(new gt_int_jefastFilter(orders_table, Table_Orders::O_ORDERKEY, min_orderkey));
        filters.at(4) = std::shared_ptr<jefastFilter>(new all_jefastFilter(lineitem_table, Table_Lineitem::L_ORDERKEY));

        {
            timer.reset();
            timer.start();
            auto count = exactJoinNoIndex("query5_full.txt", filters);
            timer.stop();
            std::cout << "join took " << timer.getMilliseconds() << " milliseconds with cardinality " << count << std::endl;
            std::string name = "join_xm_attbJoin";
            name[5] = "012345"[min_orderkey / 1000000];
            data_map->appendArray(name, long(timer.getMilliseconds()));
            data_map->appendArray("full_join_cardinality", count);
        }

        {
            timer.reset();
            timer.start();
            auto jefast_index_wfilter_builder = jefastBuilderWJoinAttribSelection(jefast_base);
            //jefast_index_wfilter_builder.AddFilter(min_custid, std::numeric_limits<jfkey_t>::max(), 0);
            // we do a filter which does nothing in order to trick the program to rebuild the first partition
            jefast_index_wfilter_builder.AddFilter(min_orderkey, std::numeric_limits<jfkey_t>::max(), 2);
            auto jefast_index_wfilter = jefast_index_wfilter_builder.Build();
            timer.stop();
            std::cout << "jefast took " << timer.getMilliseconds() << " milliseconds to build for min=" << min_orderkey << std::endl;
            std::string namebuild = "jefast_xm_attbJoinBuild";
            namebuild[7] = "012345"[min_orderkey / 1000000];
            data_map->appendArray(namebuild, long(timer.getMilliseconds()));

            timer.reset();
            timer.start();
            randomjefastJoin("randomjefast5.txt", jefast_index_wfilter->GetTotal() / 10, jefast_index_wfilter);
            timer.stop();
            std::cout << "jefast took " << timer.getMilliseconds() << " milliseconds to gather 10%" << std::endl;
            std::string name = "jefast_xm_attbJoin";
            name[7] = "012345"[min_orderkey / 1000000];
            data_map->appendArray(name, long(timer.getMilliseconds()));
        }
    }
}

void nonJoinAttribTrial(int id, float min_accbal, int64_t min_date, int64_t max_date, int64_t max_region, std::shared_ptr<jefastIndexLinear> jefast_base)
{
    Timer timer;

    std::vector< std::shared_ptr<jefastFilter> > filters(5);
    //filters.at(0) = std::shared_ptr<jefastFilter>(new all_jefastFilter(nation_table, Table_Nation::N_NATIONKEY));
    filters.at(0) = std::shared_ptr<jefastFilter>(new lt_int_jefastFilter(nation_table, Table_Nation::N_REGIONKEY, max_region));
    filters.at(1) = std::shared_ptr<jefastFilter>(new all_jefastFilter(supplier_table, Table_Supplier::S_SUPPKEY));
    //filters.at(2) = std::shared_ptr<jefastFilter>(new all_jefastFilter(customer_table, Table_Customer::C_CUSTKEY));
    filters.at(2) = std::shared_ptr<jefastFilter>(new gt_float_jefastFilter(customer_table, Table_Customer::C_ACCTBAL, min_accbal));
    //filters.at(3) = std::shared_ptr<jefastFilter>(new all_jefastFilter(orders_table, Table_Orders::O_ORDERKEY));
    filters.at(3) = std::shared_ptr<jefastFilter>(new gt_lt_int_jefastFilter(orders_table, Table_Orders::O_ORDERDATE, min_date, max_date));
    filters.at(4) = std::shared_ptr<jefastFilter>(new all_jefastFilter(lineitem_table, Table_Lineitem::L_ORDERKEY));

    {
        timer.reset();
        timer.start();
        auto count = exactJoinNoIndex("query5_full.txt", filters);
        timer.stop();
        std::cout << "join took " << timer.getMilliseconds() << " milliseconds with cardinality " << count << std::endl;
        std::string name = "join_x_nattbJoin";
        name[5] = "0123456789"[id];
        data_map->appendArray(name, long(timer.getMilliseconds()));
        data_map->appendArray("full_join_cardinality", count);
    }

    {
        timer.reset();
        timer.start();
        //auto jefast_index_wfilter_builder = jefastBuilderWJoinAttribSelection(jefast_base);
        auto jefast_index_wfilter_builder = jefastBuilderWNonJoinAttribSelection(jefast_base);
        jefast_index_wfilter_builder.AddFilter(filters.at(0), 0);
        jefast_index_wfilter_builder.AddFilter(filters.at(2), 2);
        jefast_index_wfilter_builder.AddFilter(filters.at(3), 3);
        //jefast_index_wfilter_builder.AddFilter(min_custid, std::numeric_limits<jfkey_t>::max(), 0);
        // we do a filter which does nothing in order to trick the program to rebuild the first partition
        auto jefast_index_wfilter = jefast_index_wfilter_builder.Build();
        timer.stop();
        std::cout << "jefast took " << timer.getMilliseconds() << " milliseconds to build for id=" << id << std::endl;
        std::string namebuild = "jefast_x_nattbJoinBuild";
        namebuild[7] = "0123456789"[id];
        data_map->appendArray(namebuild, long(timer.getMilliseconds()));

        timer.reset();
        timer.start();
        randomjefastJoin("randomjefast5.txt", jefast_index_wfilter->GetTotal() / 10, jefast_index_wfilter);
        timer.stop();
        std::cout << "jefast took " << timer.getMilliseconds() << " milliseconds to gather 10%" << std::endl;
        std::string name = "jefast_x_nattbJoin";
        name[7] = "0123456789"[id];
        data_map->appendArray(name, long(timer.getMilliseconds()));
    }
}

void doNonJoinAttribExp(std::shared_ptr<jefastIndexLinear> jefast_base)
{
    // do trial 1
    nonJoinAttribTrial(1, 5000.0f, convertDate("1995-01-01"), convertDate("2000-01-01"), 1, jefast_base);

    // do trial 2
    nonJoinAttribTrial(2, 0.0f, convertDate("1995-01-01"), convertDate("2000-01-01"), 1, jefast_base);

    // do trial 3
    nonJoinAttribTrial(3, 0.0f, convertDate("1995-01-01"), convertDate("2000-01-01"), 2, jefast_base);

    // do trial 4
    nonJoinAttribTrial(4, 0.0f, convertDate("1995-01-01"), convertDate("2000-01-01"), 3, jefast_base);

    // do trial 5
    nonJoinAttribTrial(5, 0.0f, convertDate("1995-01-01"), convertDate("2000-01-01"), 6, jefast_base);
}

void setup() {
    FileSizeTable table_sizes("fileInfo.txt");
    data_map.reset(new FileKeyValue("query5_timings.txt"));

    Timer timer;
    std::cout << "opening tables" << std::endl;
    timer.reset();
    timer.start();
    nation_table.reset(new Table_Nation("nation.tbl", table_sizes.get_lines("nation.tbl")));
    supplier_table.reset(new Table_Supplier("supplier.tbl", table_sizes.get_lines("supplier.tbl")));
    customer_table.reset(new Table_Customer("customer.tbl", table_sizes.get_lines("customer.tbl")));
    orders_table.reset(new Table_Orders("orders.tbl", table_sizes.get_lines("orders.tbl")));
    lineitem_table.reset(new Table_Lineitem("lineitem.tbl", table_sizes.get_lines("lineitem.tbl")));
    timer.stop();
    std::cout << "opening tables took " << timer.getMilliseconds() << " milliseconds" << std::endl;

    std::cout << "building indexes" << std::endl;
    timer.reset();
    timer.start();
    nation_table->get_key_index(Table_Nation::N_NATIONKEY);
    supplier_table->get_key_index(Table_Supplier::S_NATIONKEY);
    customer_table->get_key_index(Table_Customer::C_NATIONKEY);
    customer_table->get_key_index(Table_Customer::C_CUSTKEY);
    customer_table->get_float_index(Table_Customer::C_ACCTBAL);
    orders_table->get_key_index(Table_Orders::O_CUSTKEY);
    orders_table->get_key_index(Table_Orders::O_ORDERKEY);
    orders_table->get_int_index(Table_Orders::O_ORDERDATE);
    lineitem_table->get_key_index(Table_Lineitem::L_ORDERKEY);
    timer.stop();
    std::cout << "building basic indexes took " << timer.getMilliseconds() << " milliseconds" << std::endl;
    data_map->appendArray("basic_index_building", long(timer.getMilliseconds()));

}

int main() {
    Timer timer;
   
    setup();

    // {
        // std::cout << "building jefast" << std::endl;
        // timer.reset();
        // timer.start();
        // jefast_index_builder.reset(new JefastBuilder());
        // jefast_index_builder->AppendTable(nation_table, -1, Table_Nation::N_NATIONKEY, 0);
        // jefast_index_builder->AppendTable(supplier_table, Table_Supplier::S_NATIONKEY, Table_Supplier::S_NATIONKEY, 1);
        // jefast_index_builder->AppendTable(customer_table, Table_Customer::C_NATIONKEY, Table_Customer::C_CUSTKEY, 2);
        // jefast_index_builder->AppendTable(orders_table, Table_Orders::O_CUSTKEY, Table_Orders::O_ORDERKEY, 3);
        // jefast_index_builder->AppendTable(lineitem_table, Table_Lineitem::L_ORDERKEY, -1, 4);
        // // insert filters into jefast

        // // build a basic jefast
        // jefast_index = jefast_index_builder->Build();
        // timer.stop();
        // std::cout << "building jefast took " << timer.getMilliseconds() << " milliseconds reporting " << jefast_index->GetTotal() << "join possibilities" << std::endl;
        // data_map->appendArray("jefast_building", timer.getMilliseconds());
        // // condition c_acctbal > '[min balance]'
    // }

    //{
    //    // do full hash join
    //    std::vector< std::shared_ptr<jefastFilter> > filters(5);
    //    filters.at(0) = std::shared_ptr<jefastFilter>(new all_jefastFilter(nation_table, Table_Nation::N_NATIONKEY));
    //    filters.at(1) = std::shared_ptr<jefastFilter>(new all_jefastFilter(supplier_table, Table_Supplier::S_SUPPKEY));
    //    filters.at(2) = std::shared_ptr<jefastFilter>(new all_jefastFilter(customer_table, Table_Customer::C_CUSTKEY));
    //    filters.at(3) = std::shared_ptr<jefastFilter>(new all_jefastFilter(orders_table, Table_Orders::O_ORDERKEY));
    //    filters.at(4) = std::shared_ptr<jefastFilter>(new all_jefastFilter(lineitem_table, Table_Lineitem::L_ORDERKEY));

    //    timer.reset();
    //    timer.start();
    //    auto count = exactJoinNoIndex("query5_full.txt", filters);
    //    timer.stop();
    //    std::cout << "full join took " << timer.getMilliseconds() << " milliseconds with cardinality " << count << std::endl;
    //    data_map->appendArray("full_join", long(timer.getMilliseconds()));
    //    data_map->appendArray("full_join_cardinality", count);
    //}

    //{
    //    // do 10% sample using jefast
    //    timer.reset();
    //    timer.start();
    //    auto toRequest = jefast_index->GetTotal() / 10;
    //    auto count = randomjefastJoin("query5_sample.txt", toRequest, jefast_index);
    //    timer.stop();

    //    std::cout << "sampling 10% using jefast took " << timer.getMilliseconds() << " milliseconds with cardinality " << count << std::endl;
    //    data_map->appendArray("jefast_10p", long(timer.getMilliseconds()));
    //}

    //{
    //    // do 0.01% using olkin join
    //    auto max_outdegree = jefast_index->MaxOutdegree();
    //    timer.reset();
    //    timer.start();
    //    auto toRequest = jefast_index->GetTotal() / 10000;
    //    auto rejected = baselineJoin("query5_sample.txt", toRequest, max_outdegree);
    //    timer.stop();

    //    std::cout << "baseline sample for 0.01% took " << timer.getMilliseconds() << " milliseconds. acepted=" << toRequest << " rejected=" << rejected << std::endl;
    //    data_map->appendArray("baseline_001p", long(timer.getMilliseconds()));
    //    data_map->appendArray("baseline_001pRejected", rejected);
    //}

    //{
    //    // do the sample rate test for jefast
    //    Timer total_time;
    //    double last_ms = 0;
    //    int per_sample_request = 2000 * 2;
    //    int total_samples = 0;
    //    std::ofstream output_file("query5_sample.txt", std::ios::out);
    //    std::ofstream results_file("query5_jefast_sample_rate.txt", std::ios::out);
    //    total_time.reset();
    //    do {
    //        total_samples += randomjefastJoin(output_file, per_sample_request, jefast_index);
    //        total_time.update_accumulator();

    //        total_time.update_accumulator();
    //        results_file << total_time.getMilliseconds() << '\t' << total_samples << '\t' << per_sample_request << '\t' << (total_time.getMicroseconds() / 1000 - last_ms) << '\n';
    //        last_ms = total_time.getMicroseconds() / 1000;
    //    } while (total_time.getSeconds() < 4);
    //    results_file.close();

    //}

    //{
    //    // do the sample rate test for olkin join
    //    Timer total_time;
    //    double last_ms = 0;
    //    int per_sample_request = 15 * 2;
    //    int total_samples = 0;
    //    std::ofstream output_file("query5_sample.txt", std::ios::out);
    //    std::ofstream results_file("query5_olkin_sample_rate.txt", std::ios::out);
    //    auto max_outdegree = jefast_index->MaxOutdegree();
    //    total_time.reset();
    //    do {
    //        total_samples += baselineJoin(output_file, per_sample_request, max_outdegree);
    //        total_time.update_accumulator();

    //        total_time.update_accumulator();
    //        results_file << total_time.getMilliseconds() << '\t' << total_samples << '\t' << per_sample_request << '\t' << (total_time.getMicroseconds() / 1000 - last_ms) << '\n';
    //        last_ms = total_time.getMicroseconds() / 1000;
    //    } while (total_time.getSeconds() < 4);
    //    results_file.close();

    //}

    // do threading experiment for jefast
    // {
        // int64_t max_threads = 8;
        // int64_t requests = jefast_index->GetTotal() / 10;

        // std::vector<std::string> output_file_names;
        // std::vector<std::future<int64_t> > results;
        // // open all the files
        // for (int i = 0; i < max_threads; ++i)
        // {
            // std::string filename = "query_";
            // filename += "0123456789"[i];
            // output_file_names.push_back(filename);
        // }
        // // do the trials.
        // for (int64_t i = 1; i <= max_threads; i *= 2) {
            // timer.reset();
            // timer.start();
            // // start the threads
            // for (int t = 0; t < i; ++t) {
                // results.emplace_back(std::async(randomjefastJoinT, output_file_names[t], requests / i, jefast_index));
            // }
            // // wait for all the joins to finish
            // for (int t = 0; t < i; ++t) {
                // results[t].wait();
            // }
            // // all results are in!
            // timer.stop();
            // std::string results_string = "jefast_thread_";
            // results_string += "0123456789"[i];

            // std::cout << "finished test with " << i << " threads, taking " << timer.getMilliseconds() << " milliseconds" << std::endl;
            // data_map->appendArray(results_string, long(timer.getMilliseconds()));
        // }
    // }

    //std::cout << "adding filtering to jefast" << std::endl;
    //timer.reset();
    //timer.start();

    //float min_balance = 0.0f;
    //std::shared_ptr<jefastFilter> balance_filter(new gt_float_jefastFilter(customer_table, Table_Customer::C_ACCTBAL, min_balance));

    //// setup date filter
    //time_t epoch = getEpoch();
    //struct tm time;
    //time.tm_hour = 0;
    //time.tm_isdst = 0;
    //time.tm_min = 0;
    //time.tm_sec = 0;
    //int year, month, day;
    //
    //sscanf_s("1995-01-01", "%4i-%2i-%2i", &year, &month, &day);
    //time.tm_year = year - 1900;
    //time.tm_mon = month - 1;
    //time.tm_mday = day;
    //int64_t min_date = difftime(mktime(&time), epoch);

    //sscanf_s("2000-01-01", "%4i-%2i-%2i", &year, &month, &day);
    //time.tm_year = year - 1900;
    //time.tm_mon = month - 1;
    //time.tm_mday = day;
    //int64_t max_date = difftime(mktime(&time), epoch);

    //std::shared_ptr<jefastFilter> date_filter(new gt_lt_int_jefastFilter(orders_table, Table_Orders::O_ORDERDATE, min_date, max_date));

    //// setup region filter
    //std::shared_ptr<jefastFilter> region_filter(new lt_int_jefastFilter(nation_table, Table_Nation::N_REGIONKEY, 6));

    ////jefast_index_builder->AddFilter(balance_filter, 2);
    ////jefast_index_builder->AddFilter(date_filter, 3);
    ////jefast_index_builder->AddFilter(region_filter, 0);

    ////jefast_index_builder->AppendBuildSuggestion(1, JefastBuilder::BuilderSuggestion::RIGHT);
    ////jefast_index_builder->AppendBuildSuggestion(0, JefastBuilder::BuilderSuggestion::LEFT);
    ////jefast_index_builder->AppendBuildSuggestion(4, JefastBuilder::BuilderSuggestion::RIGHT);

    //auto jefast_index_wfilter_builder = jefastBuilderWNonJoinAttribSelection(jefast_index);
    //jefast_index_wfilter_builder.AddFilter(balance_filter, 2);
    //jefast_index_wfilter_builder.AddFilter(date_filter, 3);
    //jefast_index_wfilter_builder.AddFilter(region_filter, 0);
    //jefast_index_filtered = jefast_index_wfilter_builder.Build();
    //timer.stop();
    //std::cout << "building jefast with selection condition took " << timer.getMilliseconds() << " milliseconds" << std::endl;
    //data_map.appendArray("jefast_building", timer.getMilliseconds());

    //std::cout << "jefast has a reported " << jefast_index->GetTotal() << " join possibilities" << std::endl;
    //std::cout << "jefast with selection reports " << jefast_index_filtered->GetTotal() << " join possibilities" << std::endl;

    //timer.reset();
    //timer.start();
    //randomjefastJoin("query5_filtering_jefast_random.txt", 100000);
    //timer.stop();
    //std::cout << "selecting 100,000 random elements using jefast took " << timer.getMilliseconds() << " milliseconds" << std::endl;
    //data_map.appendArray("jefast_100000", timer.getMilliseconds());

    //std::cout << "starting full jefast join on " << jefast_index_filtered->GetTotal() << " results" << std::endl;
    //timer.reset();
    //timer.start();
    //fulljefastJoin("query5_filtering_jefast_full.txt", jefast_index_filtered->GetTotal());
    //timer.stop();
    //std::cout << "full jefast took " << timer.getMilliseconds() << " milliseconds" << std::endl;
    //data_map.appendArray("jefast_full", timer.getMilliseconds());

    //std::cout << "doing full hash join with indexes";
    //timer.reset();
    //timer.start();
    //std::vector<std::shared_ptr<jefastFilter> > filters(5);
    //filters.at(0) = region_filter;
    //filters.at(1) = std::shared_ptr<jefastFilter>(new all_jefastFilter(supplier_table, Table_Supplier::S_SUPPKEY));
    //filters.at(2) = balance_filter;
    //filters.at(3) = date_filter;
    //filters.at(4) = std::shared_ptr<jefastFilter>(new all_jefastFilter(lineitem_table, Table_Lineitem::L_ORDERKEY));
    //exactJoinNoIndex("query_5_filtering_full.txt", filters);
    //timer.stop();
    //std::cout << "full join took " << timer.getMilliseconds() << " milliseconds" << std::endl;



    // {
        // // do experiment with join attribute with selection condition.
        // joinAttribExp(jefast_index);
    // }

    data_map->flush();
    std::cout << "done" << std::endl;

    return 0;
}