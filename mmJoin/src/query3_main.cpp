// This implemented the different algorithms for query3
// Programmer: Robert Christensen
// email: robertc@cs.utah.edu

#include <iostream>
#include <fstream>
#include <memory>
#include <random>
#include <algorithm>
#include <map>
#include <vector>
#include <thread>
#include <future>
#include <limits>
#include <cstring>

#include "util/Timer.h"
#include "util/FileSizeTable.h"
#include "util/FileKeyValue.h"
#include "util/accumulator.h"
#include "util/joinSettings.h"

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

// SELECT SUM(l_extendedprice * (1 - l_discount))
// FROM customer, orders, lineitem
// WHERE c_custkey = o_custkey
// AND l_orderkey = o_orderkey

static std::shared_ptr<Table> customer_table;
static std::shared_ptr<Table> orders_table;
static std::shared_ptr<Table> lineitem_table;

static std::shared_ptr<FileKeyValue> data_map;
static std::shared_ptr<jefastIndexLinear> jefastIndex;

static global_settings query3Settings;

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

// do full join
int64_t exactJoinNoIndex(std::string outfile, std::vector<std::shared_ptr<jefastFilter> > &filters) {
    // implements a straightforward implementation of a join which
    // does not require an index.

    std::ofstream output_file(outfile, std::ios::out);
    int64_t count = 0;

    auto Table_1 = customer_table;
    auto Table_2 = orders_table;
    auto Table_3 = lineitem_table;

    int table1Index2 = Table_Customer::C_CUSTKEY;
    int table2Index1 = Table_Orders::O_CUSTKEY;

    int table2Index2 = Table_Orders::O_ORDERKEY;
    int table3Index1 = Table_Lineitem::L_ORDERKEY;


    // build the hash for table 1
    std::map<jfkey_t, std::vector<int64_t> > Table1_hash;
    //int64_t Table_1_count = Table_1->row_count();
    //for (int64_t i = 0; i < Table_1_count; ++i) {
    for (auto f1_enu = filters.at(0)->getEnumerator(); f1_enu->Step();) {
        Table1_hash[Table_1->get_int64(f1_enu->getValue(), table1Index2)].push_back(f1_enu->getValue());
    }

    // build the hash for table 2.  All matched elements from table 1 hash will be emitted
    // the tuple has the form <index from table 1, index for table 2> for all matching tuple
    std::map<jfkey_t, std::vector<std::tuple<int64_t, int64_t> > > Table2_hash;
    //int64_t Table_2_count = Table_2->row_count();
    //for (int64_t i = 0; i < Table_2_count; ++i) {
    for (auto f2_enu = filters.at(1)->getEnumerator(); f2_enu->Step();) {
        jfkey_t value = Table_2->get_int64(f2_enu->getValue(), table2Index1);
        for (auto matching_indexes : Table1_hash[value]) {
            Table2_hash[Table_2->get_int64(f2_enu->getValue(), table2Index2)].emplace_back(matching_indexes, f2_enu->getValue());
        }
    }

    Table1_hash.clear();

    // build the hash for table 3?  No, we can just emit when we find a match in this case.
    //int64_t Table_3_count = Table_3->row_count();
    //for (int64_t i = 0; i < Table_5_count; ++i) {
    for (auto f3_enu = filters.at(2)->getEnumerator(); f3_enu->Step();) {
        int64_t value = Table_3->get_int64(f3_enu->getValue(), table3Index1);
        for (auto matching_indexes : Table2_hash[value]) {
            //accum.insert(Table_3->get_float(f3_enu->getValue(), Table_Lineitem::L_EXTENDEDPRICE) * (1 - Table_3->get_float(f3_enu->getValue(), Table_Lineitem::L_DISCOUNT)));
            // emit the join results here.
            //output_file << count << ' ' << std::to_string(Table_1->get_int64(std::get<0>(matching_indexes), table1Index2))
            //    << ' ' << std::to_string(Table_2->get_int64(std::get<1>(matching_indexes), table2Index2))
            //    << ' ' << std::to_string(Table_3->get_int64(f3_enu->getValue(), 1)) << '\n';

            ++count;
        }
    }

    output_file.close();
    return count;
}

int64_t randomjefastJoin(std::string outfile, int64_t output_count, std::shared_ptr<jefastIndexLinear> jefast)
{
    std::ofstream output_file(outfile, std::ios::out);

    std::vector<int64_t> results;

    //Timer timer;
    //timer.start();

    auto Table_1 = customer_table;
    auto Table_2 = orders_table;
    auto Table_3 = lineitem_table;

    int table1Index2 = Table_Customer::C_CUSTKEY;
    //int table2Index1 = Table_Orders::O_CUSTKEY;

    int table2Index2 = Table_Orders::O_ORDERKEY;
    //int table3Index1 = Table_Lineitem::L_ORDERKEY;

    int64_t max = jefast->GetTotal();
    std::vector<weight_t> random_values;
    std::uniform_int_distribution<weight_t> distribution(0, max-1);


    static std::default_random_engine generator;

    random_values.resize(output_count);
    std::generate(random_values.begin(), random_values.end(), [&]() {return distribution(generator);});
    //for (int i = 0; i < max; ++i)
    //    random_values.push_back(i);


    std::sort(random_values.begin(), random_values.end());

    int counter = 0;
    //accumulator_double accum;
    //accum.set_total(max);
    for (int i : random_values) {
        jefast->GetJoinNumber(i, results);

        //std::cout << i << std::endl;

        //output_file << counter << ' '
        //    << std::to_string(Table_1->get_int64(results[0], table1Index2)) << ' '
        //    << std::to_string(Table_2->get_int64(results[1], table2Index2)) << ' '
        //    << std::to_string(Table_3->get_int64(results[2], 1)) << '\n';
        ++counter;
    }

    return counter;
}

int64_t baselineJoin(std::string outfile, int output_count, const std::vector<weight_t> &max_cardinality) {

    auto Table_1 = customer_table;
    auto Table_2 = orders_table;
    auto Table_3 = lineitem_table;

    int table1Index2 = Table_Customer::C_CUSTKEY;
    int table2Index1 = Table_Orders::O_CUSTKEY;

    int table2Index2 = Table_Orders::O_ORDERKEY;
    int table3Index1 = Table_Lineitem::L_ORDERKEY;

    int64_t count = 0;
    int64_t rejected = 0;
    int64_t max_possible_path = 0;

    std::default_random_engine generator;

    std::ofstream output_file(outfile, std::ios::out);

    int64_t Table1_size = Table_1->row_count();
    std::uniform_int_distribution<int64_t> region_dist(0, Table1_size - 1);
    int64_t M_value = 1;
    for (weight_t x : max_cardinality)
        M_value *= x;
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
        int64_t Table3_v = Table_3->get_int64(Table3_i->second, 1);

        //// do random selection of Table4
        //auto Table4_range = Table_4->get_key_index(table4Index1)->equal_range(Table3_v);
        //int64_t Table4_count = Table_4->get_key_index(table4Index1)->count(Table3_v);
        //if (Table4_count == 0)
        //    continue;
        //std::uniform_int_distribution<int64_t> Table4_dist(0, Table4_count - 1);
        //int64_t selection_count_4 = Table4_dist(generator);
        //auto Table4_i = Table4_range.first;
        //while (selection_count_4 > 0) {
        //    Table4_i++;
        //    --selection_count_4;
        //}
        //int64_t Table4_v = Table_4->get_int64(Table4_i->second, 1);

        //// do random selection of Table5
        //auto Table5_range = Table_5->get_key_index(table5Index1)->equal_range(Table4_v);
        //int64_t Table5_count = Table_5->get_key_index(table5Index1)->count(Table4_v);
        //if (Table5_count == 0)
        //    continue;
        //std::uniform_int_distribution<int64_t> Table5_dist(0, Table5_count - 1);
        //int64_t selection_count_5 = Table5_dist(generator);
        //auto Table5_i = Table5_range.first;
        //while (selection_count_5 > 0) {
        //    Table5_i++;
        //    --selection_count_5;
        //}
        //int64_t Table5_v = Table_5->get_int64(Table5_i->second, 1);

        // decide if we should reject
        int64_t possible_paths = Table2_count * Table3_count;
        max_possible_path = std::max(possible_paths, max_possible_path);
        // if true, accept
        if (rejection_filter(generator) < possible_paths) {
            //output_file << count << ' '
            //    << std::to_string(Table1_v) << ' '
            //    //<< regionName << ' '
            //    << std::to_string(Table2_v) << ' '
            //    //<< nationName << ' '
            //    << std::to_string(Table3_v) << '\n';
            //    //<< Table4_v << '\n';
            count++;
        }
        else {
            rejected++;
        }
    }

    output_file.close();
    return rejected;
}

void joinAttribExp(std::shared_ptr<jefastIndexLinear> jefast_base)
{
    Timer timer;

    for (auto min_orderkey : { 1000000, 2000000, 3000000, 4000000, 5000000 })
    {
            // do full hash join
            std::vector< std::shared_ptr<jefastFilter> > filters(3);
            filters.at(0) = std::shared_ptr<jefastFilter>(new all_jefastFilter(customer_table, Table_Customer::C_CUSTKEY));
            filters.at(1) = std::shared_ptr<jefastFilter>(new gt_int_jefastFilter(orders_table, Table_Orders::O_ORDERKEY, min_orderkey));
            filters.at(2) = std::shared_ptr<jefastFilter>(new all_jefastFilter(lineitem_table, Table_Lineitem::L_ORDERKEY));

            if(query3Settings.joinAttribHash)
            {
                timer.reset();
                timer.start();
                auto count = exactJoinNoIndex("query3_full.txt", filters);
                timer.stop();
                std::cout << "join took " << timer.getMilliseconds() << " milliseconds with cardinality " << count << std::endl;
                std::string name = "join_xm_attbJoin";
                name[5] = "012345"[min_orderkey / 1000000];
                data_map->appendArray(name, long(timer.getMilliseconds()));
                data_map->appendArray(name + "_cardinality", count);
            }

            if(query3Settings.joinAttribJefast)
            {
                timer.reset();
                timer.start();
                auto jefast_index_wfilter_builder = jefastBuilderWJoinAttribSelection(jefast_base);
                //jefast_index_wfilter_builder.AddFilter(min_custid, std::numeric_limits<jfkey_t>::max(), 0);
                // we do a filter which does nothing in order to trick the program to rebuild the first partition
                jefast_index_wfilter_builder.AddFilter(min_orderkey, std::numeric_limits<jfkey_t>::max(), 1);
                auto jefast_index_wfilter = jefast_index_wfilter_builder.Build();
                timer.stop();
                std::cout << "jefast took " << timer.getMilliseconds() << " milliseconds to build for min=" << min_orderkey << std::endl;
                std::string namebuild = "jefast_xm_attbJoinBuild";
                namebuild[7] = "012345"[min_orderkey / 1000000];
                data_map->appendArray(namebuild, long(timer.getMilliseconds()));

                timer.reset();
                timer.start();
                randomjefastJoin("randomjefast3.txt", jefast_index_wfilter->GetTotal() / 10, jefast_index_wfilter);
                timer.stop();
                std::cout << "jefast took " << timer.getMilliseconds() << " milliseconds to gather 10%" << std::endl;
                std::string name = "jefast_xm_attbJoin";
                name[7] = "012345"[min_orderkey / 1000000];
                data_map->appendArray(name, long(timer.getMilliseconds()));
            }
    }
}

void nonJoinAttribTrial(int id, float min_accbal, int64_t min_date, int64_t max_date, std::shared_ptr<jefastIndexLinear> jefast_base)
{
    Timer timer;

    std::vector< std::shared_ptr<jefastFilter> > filters(3);
    filters.at(0) = std::shared_ptr<jefastFilter>(new gt_float_jefastFilter(customer_table, Table_Customer::C_ACCTBAL, min_accbal));
    filters.at(1) = std::shared_ptr<jefastFilter>(new gt_lt_int_jefastFilter(orders_table, Table_Orders::O_ORDERDATE, min_date, max_date));
    filters.at(2) = std::shared_ptr<jefastFilter>(new all_jefastFilter(lineitem_table, Table_Lineitem::L_ORDERKEY));

    if(query3Settings.nonJoinAttribHash)
    {
        timer.reset();
        timer.start();
        auto count = exactJoinNoIndex("query3_full.txt", filters);
        timer.stop();
        std::cout << "join took " << timer.getMilliseconds() << " milliseconds with cardinality " << count << std::endl;
        std::string name = "join_x_nattbJoin";
        name[5] = "0123456789"[id];
        data_map->appendArray(name, long(timer.getMilliseconds()));
        data_map->appendArray(name + "_cardinality", count);
    }

    if(query3Settings.nonJoinAttribJefast)
    {
        timer.reset();
        timer.start();
        //auto jefast_index_wfilter_builder = jefastBuilderWJoinAttribSelection(jefast_base);
        auto jefast_index_wfilter_builder = jefastBuilderWNonJoinAttribSelection(jefast_base);
        jefast_index_wfilter_builder.AddFilter(filters.at(0), 0);
        jefast_index_wfilter_builder.AddFilter(filters.at(1), 1);
        //jefast_index_wfilter_builder.AddFilter(min_custid, std::numeric_limits<jfkey_t>::max(), 0);
        // we do a filter which does nothing in order to trick the program to rebuild the first partition
        auto jefast_index_wfilter = jefast_index_wfilter_builder.Build();
        timer.stop();
        std::cout << "jefast took " << timer.getMilliseconds() << " milliseconds to build for id=" << id << " with cardinality=" << jefast_index_wfilter->GetTotal() << std::endl;
        std::string namebuild = "jefast_x_nattbJoinBuild";
        namebuild[7] = "0123456789"[id];
        data_map->appendArray(namebuild, long(timer.getMilliseconds()));

        timer.reset();
        timer.start();
        randomjefastJoin("randomjefast3.txt", jefast_index_wfilter->GetTotal() / 10, jefast_index_wfilter);
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
    nonJoinAttribTrial(1, -5000.0f, convertDate("1994-01-01"), convertDate("2000-01-01"), jefast_base);

    // do trial 2
    nonJoinAttribTrial(2, 0.0f,     convertDate("1995-01-01"), convertDate("2000-01-01"), jefast_base);

    // do trial 3
    nonJoinAttribTrial(3, 5000.0f, convertDate("1995-01-01"), convertDate("2000-01-01"), jefast_base);

    // do trial 4
    nonJoinAttribTrial(4, 3000.0f, convertDate("1995-01-01"), convertDate("1997-01-01"), jefast_base);

    // do trial 5
    nonJoinAttribTrial(5, 6000.0f, convertDate("1995-01-01"), convertDate("2007-01-01"), jefast_base);
}

void setup_data() {
    // load the tables into memory
    FileSizeTable table_sizes("fileInfo.txt");
    data_map.reset(new FileKeyValue("query3_timings.txt"));

    Timer timer;
    std::cout << "opening tables" << std::endl;
    timer.reset();
    timer.start();
    customer_table.reset(new Table_Customer("customer.tbl", table_sizes.get_lines("customer.tbl")));
    orders_table.reset(new Table_Orders("orders.tbl", table_sizes.get_lines("orders.tbl")));
    lineitem_table.reset(new Table_Lineitem("lineitem.tbl", table_sizes.get_lines("lineitem.tbl")));
    timer.stop();
    std::cout << "opening tables took " << timer.getSeconds() << " seconds" << std::endl;
    data_map->appendArray("opening_tables", long(timer.getMilliseconds()));

    // build the indexes which might be used in the experiment
    std::cout << "building indexes" << std::endl;
    timer.reset();
    timer.start();
    // join indexes
    if (query3Settings.buildIndex)
    {
        customer_table->get_key_index(Table_Customer::C_CUSTKEY);
        orders_table->get_key_index(Table_Orders::O_CUSTKEY);
        orders_table->get_key_index(Table_Orders::O_ORDERKEY);
        lineitem_table->get_key_index(Table_Lineitem::L_ORDERKEY);
        lineitem_table->get_key_index(Table_Lineitem::L_ORDERKEY);
    }

    // filtering conditions

    // we do not currently have plans to do selection conditions on query 0
    timer.stop();
    std::cout << "done building indexes took " << timer.getMilliseconds() << " milliseconds" << std::endl;
    data_map->appendArray("building_indexes", long(timer.getMilliseconds()));


    // find max outdegree of each join?
}

int main(int argc, char** argv) {
    query3Settings = parse_args(argc, argv);

    setup_data();
    Timer timer;

    // do hash join
    if(query3Settings.hashJoin)
    {
       std::vector<std::shared_ptr<jefastFilter> > filters(3);
       filters.at(0) = std::shared_ptr<jefastFilter>(new all_jefastFilter(customer_table, Table_Customer::C_CUSTKEY));
       filters.at(1) = std::shared_ptr<jefastFilter>(new all_jefastFilter(orders_table, Table_Orders::O_ORDERKEY));
       filters.at(2) = std::shared_ptr<jefastFilter>(new all_jefastFilter(lineitem_table, Table_Lineitem::L_ORDERKEY));

       timer.reset();
       timer.start();
       auto count = exactJoinNoIndex("query3_full.txt", filters);
       timer.stop();
       std::cout << "full join took " << timer.getMilliseconds() << " milliseconds with cardinality " << count << std::endl;
       data_map->appendArray("full_join", long(timer.getMilliseconds()));
       data_map->appendArray("full_join_cadinality", count);
    }

    // building jefast
    if(query3Settings.buildJefast)
    {
        timer.reset();
        timer.start();
        JefastBuilder jefast_index_builder;
        jefast_index_builder.AppendTable(customer_table, -1, Table_Customer::C_CUSTKEY, 0);
        jefast_index_builder.AppendTable(orders_table, Table_Orders::O_CUSTKEY, Table_Orders::O_ORDERKEY, 1);
        jefast_index_builder.AppendTable(lineitem_table, Table_Lineitem::L_ORDERKEY, -1, 2);

        jefastIndex = jefast_index_builder.Build();
        timer.stop();

        std::cout << "building jefast took " << timer.getMilliseconds() << " milliseconds with cardinality " << jefastIndex->GetTotal() << std::endl;
        data_map->appendArray("jefast_build", long(timer.getMilliseconds()));
    }

    //do a 10% sample using jefast
    if(query3Settings.jefastSample)
    {
       timer.reset();
       timer.start();
       auto toRequest = jefastIndex->GetTotal() / 10;
       auto count = randomjefastJoin("query3_samples.txt", toRequest, jefastIndex);
       timer.stop();

       std::cout << "sampling 10% took " << timer.getMilliseconds() << " milliseconds with cardinality " << count << std::endl;
       data_map->appendArray("jefast_10p", long(timer.getMilliseconds()));
    }

    if (query3Settings.jefastModify)
    {
        // do random modifications for supplier, table 2
        int table_id = 1;

        data_map->appendArray("modifyCount", query3Settings.modifyCount);

        // generate random ids to remove/insert
        std::vector<jfkey_t> modify_records = generate_unique_random_numbers(query3Settings.modifyCount, 0, orders_table->row_count());
        // do delete

        jefastIndex->set_postponeRebuild(true);
        timer.reset();
        timer.start();
        for (auto record_id : modify_records) {
            //std::cout << "removing " << record_id << std::endl;
            jefastIndex->Delete(table_id, record_id);
        }
        jefastIndex->rebuild_initial();
        timer.stop();
        data_map->appendArray("DeleteTime", long(timer.getMilliseconds()));

        std::cout << "delete of " << query3Settings.modifyCount << " took " << timer.getMilliseconds() << "ms. The cardinality is now " << jefastIndex->GetTotal() << std::endl;

        // do insert
        timer.reset();
        timer.start();
        for (auto record_id : modify_records) {
            jefastIndex->Insert(table_id, record_id);
        }
        jefastIndex->rebuild_initial();
        timer.stop();
        data_map->appendArray("InsertTime", long(timer.getMilliseconds()));

        std::cout << "insert of " << query3Settings.modifyCount << " took " << timer.getMilliseconds() << "ms. The cardinality is now " << jefastIndex->GetTotal() << std::endl;
    }

    //do a 10% baseline olkin join
    if(query3Settings.olkenSample)
    {
       auto max_outdegree = jefastIndex->MaxOutdegree();
       timer.reset();
       timer.start();
       auto toRequest = 100000;//jefastIndex->GetTotal() / 10;
       auto rejected = baselineJoin("query3_samples.txt", toRequest, max_outdegree);
       timer.stop();

       std::cout << "baseline sample for 10% took " << timer.getMilliseconds() << " milliseconds accepted=" << toRequest << " rejected=" << rejected << std::endl;
       data_map->appendArray("baseline_100000p", long(timer.getMilliseconds()));
       data_map->appendArray("baseline_100000pRejected", rejected);
    }

    // do threading experiment for jefast
    if(query3Settings.threading)
    {
        int64_t max_threads = 8;
        int64_t requests = jefastIndex->GetTotal() / 10;

        std::vector<std::future<int64_t> > results;
        // do the trials.
        for (int64_t i = 1; i <= max_threads; i *= 2) {
            timer.reset();
            timer.start();
            // start the threads
            for (int t = 0; t < i; ++t) {
                results.emplace_back(std::async(randomjefastJoin, query3Settings.null_file_name, requests / i, jefastIndex));
            }
            // wait for all the joins to finish
            for (int t = 0; t < i; ++t) {
                results[t].wait();
            }
            // all results are in!
            timer.stop();
            std::string results_string = "jefast_thread_";
            results_string += "0123456789"[i];

            std::cout << "finished test with " << i << " threads, taking " << timer.getMilliseconds() << " milliseconds" << std::endl;
            data_map->appendArray(results_string, long(timer.getMilliseconds()));
        }
    }

    if (query3Settings.joinAttribHash || query3Settings.joinAttribJefast)
    {
        // do experiment with join attribute with selection condition.
        joinAttribExp(jefastIndex);
    }

    if (query3Settings.nonJoinAttribHash || query3Settings.nonJoinAttribJefast)
    {
        doNonJoinAttribExp(jefastIndex);
    }

    data_map->flush();
    std::cout << "done" << std::endl;
}