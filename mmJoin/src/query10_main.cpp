
#include <iostream>
#include <fstream>
#include <memory>
#include <random>
#include <map>
#include <vector>
#include <thread>
#include <future>

#include "util/Timer.h"
#include "util/FileSizeTable.h"
#include "util/FileKeyValue.h"
#include "util/accumulator.h"
#include "util/joinSettings.h"

#include "database/TableNation.h"
#include "database/TableCustomer.h"
#include "database/TableOrders.h"
#include "database/TableLineitem.h"

#include "database/jefastIndex.h"
#include "database/jefastBuilder.h"

static std::shared_ptr<Table> nation_table;
static std::shared_ptr<Table> customer_table;
static std::shared_ptr<Table> orders_table;
static std::shared_ptr<Table> lineitem_table;

static std::shared_ptr<jefastIndexLinear> jefastIndex;
static std::shared_ptr<FileKeyValue> data_map;

static global_settings query10Settings;

// Note, we will require a filter for each column.  It can just be an empty filter (an everything filter)
// we will be doing a linear scan of the data to implement this algorithm for now.
int64_t exactJoinNoIndex(std::string outfile, std::vector<std::shared_ptr<jefastFilter> > filters) {
    // implements a straightforward implementation of a join which
    // does not require an index.

    std::ofstream output_file(outfile, std::ios::out);
    int64_t count = 0;

    auto Table_1 = nation_table;
    auto Table_2 = customer_table;
    auto Table_3 = orders_table;
    auto Table_4 = lineitem_table;

    int table1Index2 = Table_Nation::N_NATIONKEY;
    int table2Index1 = Table_Customer::C_NATIONKEY;

    int table2Index2 = Table_Customer::C_CUSTKEY;
    int table3Index1 = Table_Orders::O_CUSTKEY;

    int table3Index2 = Table_Orders::O_ORDERKEY;
    int table4Index1 = Table_Lineitem::L_ORDERKEY;

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

    // build the hash for table 4?  No, we can just emit when we find a match in this case.
    //int64_t Table_4_count = Table_4->row_count();
    //for (int64_t i = 0; i < Table_5_count; ++i) {
    for (auto f4_enu = filters.at(3)->getEnumerator(); f4_enu->Step();) {
        int64_t value = Table_4->get_int64(f4_enu->getValue(), table4Index1);
        for (auto matching_indexes : Table3_hash[value]) {
            // emit the join results here.
            //output_file << count << ' '
            //    << std::to_string(Table_1->get_int64(std::get<0>(matching_indexes), table1Index2)) << ' '
            //    << std::to_string(Table_2->get_int64(std::get<1>(matching_indexes), table2Index2)) << ' '
            //    << std::to_string(Table_3->get_int64(std::get<2>(matching_indexes), table3Index2)) << ' '
            //    << std::to_string(Table_4->get_int64(f4_enu->getValue(), 1)) << '\n';
            ++count;
        }
    }

    output_file.close();
    return count;
}

int64_t randomjefastJoin(std::ofstream & output_file, int64_t output_count, std::shared_ptr<jefastIndexLinear> jefast) {
    std::vector<int64_t> results;

    auto Table_1 = nation_table;
    auto Table_2 = customer_table;
    auto Table_3 = orders_table;
    auto Table_4 = lineitem_table;

    int table1Index2 = Table_Nation::N_NATIONKEY;
    int table2Index1 = Table_Customer::C_NATIONKEY;

    int table2Index2 = Table_Customer::C_CUSTKEY;
    int table3Index1 = Table_Orders::O_CUSTKEY;

    int table3Index2 = Table_Orders::O_ORDERKEY;
    int table4Index1 = Table_Lineitem::L_ORDERKEY;

    int64_t max = jefast->GetTotal();
    std::vector<weight_t> random_values;
    std::uniform_int_distribution<weight_t> distribution(0, max - 1);
    random_values.resize(output_count);

    static std::default_random_engine generator;

    std::generate(random_values.begin(), random_values.end(), [&]() {return distribution(generator);});

    std::sort(random_values.begin(), random_values.end());

    int counter = 0;
    float sum = 0.0;
    for (int i : random_values) {
        jefast->GetJoinNumber(i, results);

        //sum += lineitem_table->get_float(results[3], Table_Lineitem::L_EXTENDEDPRICE);

        //output_file << counter << ' '
        //    << std::to_string(Table_1->get_int64(results[0], table1Index2)) << ' '
        //    << std::to_string(Table_2->get_int64(results[1], table2Index2)) << ' '
        //    << std::to_string(Table_3->get_int64(results[2], table3Index2)) << ' '
        //    << std::to_string(Table_4->get_int64(results[3], 1)) << '\n';
    }

    return output_count;

}

int64_t randomjefastJoin(std::string outfile, int64_t output_count, std::shared_ptr<jefastIndexLinear> jefast) {
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


int64_t baselineJoin(std::ofstream & output_file, int output_count, const std::vector<weight_t> &max_cardinality) {
    //std::ofstream output_file(outfile, std::ios::out);

    auto Table_1 = nation_table;
    auto Table_2 = customer_table;
    auto Table_3 = orders_table;
    auto Table_4 = lineitem_table;

    int table1Index2 = Table_Nation::N_NATIONKEY;
    int table2Index1 = Table_Customer::C_NATIONKEY;

    int table2Index2 = Table_Customer::C_CUSTKEY;
    int table3Index1 = Table_Orders::O_CUSTKEY;

    int table3Index2 = Table_Orders::O_ORDERKEY;
    int table4Index1 = Table_Lineitem::L_ORDERKEY;

    int64_t count = 0;
    int64_t rejected = 0;
    int64_t max_possible_path = 0;

    std::default_random_engine generator;

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
        int64_t Table4_v = Table_4->get_int64(Table4_i->second, 1);

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
        int64_t possible_paths = Table2_count * Table3_count * Table4_count;
        max_possible_path = std::max(possible_paths, max_possible_path);
        // if true, accept
        if (rejection_filter(generator) < possible_paths) {
            output_file << count << ' '
                << std::to_string(Table1_v) << ' '
                //<< regionName << ' '
                << std::to_string(Table2_v) << ' '
                //<< nationName << ' '
                << std::to_string(Table3_v) << ' '
                << std::to_string(Table4_v) << '\n';
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

void setup_data() {
    // load the tables into memory
    FileSizeTable table_sizes("fileInfo.txt");
    data_map.reset(new FileKeyValue("query10_timings.txt"));

    Timer timer;
    std::cout << "opening tables" << std::endl;
    timer.reset();
    timer.start();
    nation_table.reset(new Table_Nation("nation.tbl", table_sizes.get_lines("nation.tbl")));
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
    if (query10Settings.buildIndex) {
        nation_table->get_key_index(Table_Nation::N_NATIONKEY);
        customer_table->get_key_index(Table_Customer::C_NATIONKEY);
        customer_table->get_key_index(Table_Customer::C_CUSTKEY);
        orders_table->get_key_index(Table_Orders::O_ORDERKEY);
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
    query10Settings = parse_args(argc, argv);

    setup_data();
    Timer timer;

    // do hash join
    if (query10Settings.hashJoin)
    {
        std::vector<std::shared_ptr<jefastFilter> > filters(4);
        filters.at(0) = std::shared_ptr<jefastFilter>(new all_jefastFilter(nation_table, Table_Nation::N_NATIONKEY));
        filters.at(1) = std::shared_ptr<jefastFilter>(new all_jefastFilter(customer_table, Table_Customer::C_CUSTKEY));
        filters.at(2) = std::shared_ptr<jefastFilter>(new all_jefastFilter(orders_table, Table_Orders::O_ORDERKEY));
        filters.at(3) = std::shared_ptr<jefastFilter>(new all_jefastFilter(lineitem_table, Table_Lineitem::L_ORDERKEY));

        timer.reset();
        timer.start();
        auto count = exactJoinNoIndex("query10_full.txt", filters);
        timer.stop();
        std::cout << "full join took " << timer.getMilliseconds() << " milliseconds with cardinality " << count << std::endl;
        data_map->appendArray("full_join", long(timer.getMilliseconds()));
        data_map->appendArray("full_join_cadinality", count);
    }

    // building jefast
    if (query10Settings.buildJefast)
    {
        timer.reset();
        timer.start();
        JefastBuilder jefast_index_builder;
        jefast_index_builder.AppendTable(nation_table, -1, Table_Nation::N_NATIONKEY, 0);
        jefast_index_builder.AppendTable(customer_table, Table_Customer::C_NATIONKEY, Table_Customer::C_CUSTKEY, 1);
        jefast_index_builder.AppendTable(orders_table, Table_Orders::O_CUSTKEY, Table_Orders::O_ORDERKEY, 2);
        jefast_index_builder.AppendTable(lineitem_table, Table_Lineitem::L_ORDERKEY, -1, 3);

        jefastIndex = jefast_index_builder.Build();
        timer.stop();

        std::cout << "building jefast took " << timer.getMilliseconds() << " milliseconds with cardinality " << jefastIndex->GetTotal() << std::endl;
        data_map->appendArray("jefast_build", long(timer.getMilliseconds()));
    }

    // do a 10% sample using jefast
    if (query10Settings.jefastSample)
    {
        timer.reset();
        timer.start();
        auto toRequest = jefastIndex->GetTotal() / 10;
        auto count = randomjefastJoin("query10_sample.txt", toRequest, jefastIndex);
        timer.stop();

        std::cout << "sampling 10% took " << timer.getMilliseconds() << " milliseconds with cardinality " << count << std::endl;
        data_map->appendArray("jefast_10p", long(timer.getMilliseconds()));
    }

    if (query10Settings.jefastModify)
    {
        // do random modifications for supplier, table 2
        int table_id = 2;

        data_map->appendArray("modifyCount", query10Settings.modifyCount);

        // generate random ids to remove/insert
        std::vector<jfkey_t> modify_records = generate_unique_random_numbers(query10Settings.modifyCount, 0, orders_table->row_count());
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

        std::cout << "delete of " << query10Settings.modifyCount << " took " << timer.getMilliseconds() << "ms. The cardinality is now " << jefastIndex->GetTotal() << std::endl;

        // do insert
        timer.reset();
        timer.start();
        for (auto record_id : modify_records) {
            jefastIndex->Insert(table_id, record_id);
        }
        jefastIndex->rebuild_initial();
        timer.stop();
        data_map->appendArray("InsertTime", long(timer.getMilliseconds()));

        std::cout << "insert of " << query10Settings.modifyCount << " took " << timer.getMilliseconds() << "ms. The cardinality is now " << jefastIndex->GetTotal() << std::endl;
    }

    // do a 10% baseline olkin join
    if (query10Settings.olkenSample)
    {
        auto max_outdegree = jefastIndex->MaxOutdegree();
        timer.reset();
        timer.start();
        auto toRequest = 10000;//jefastIndex->GetTotal() / 10;
        auto rejected = baselineJoin("query0_samples.txt", toRequest, max_outdegree);
        timer.stop();

        std::cout << "baseline sample for 10% took " << timer.getMilliseconds() << " milliseconds accepted=" << toRequest << " rejected=" << rejected << std::endl;
        data_map->appendArray("baseline_10000p", long(timer.getMilliseconds()));
        data_map->appendArray("baseline_10000pRejected", rejected);
    }

    // do threading experiment for jefast
    if (query10Settings.threading)
    {
        int64_t max_threads = 8;
        int64_t requests = jefastIndex->GetTotal();

        std::vector<std::string> output_file_names;
        std::vector<std::future<int64_t> > results;
        // open all the files
        for (int i = 0; i < max_threads; ++i)
        {
            std::string filename = "query_";
            filename += "0123456789"[i];
            output_file_names.push_back(filename);
        }
        // do the trials.
        for (int64_t i = 1; i <= max_threads; i *= 2) {
            timer.reset();
            timer.start();
            // start the threads
            for (int t = 0; t < i; ++t) {
                results.emplace_back(std::async(randomjefastJoinT, query10Settings.null_file_name, requests / i, jefastIndex));
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

    if (query10Settings.jefastRate)
    {
        // do the sample rate test for jefast
        Timer total_time;
        double last_ms = 0;
        int per_sample_request = 3000;
        int total_samples = 0;
        std::ofstream output_file("query10_sample.txt", std::ios::out);
        std::ofstream results_file("query10_jefast_sample_rate.txt", std::ios::out);
        total_time.reset();
        do {
            total_samples += randomjefastJoin(output_file, per_sample_request, jefastIndex);
            total_time.update_accumulator();

            total_time.update_accumulator();
            results_file << total_time.getMilliseconds() << '\t' << total_samples << '\t' << per_sample_request << '\t' << (total_time.getMicroseconds() / 1000 - last_ms) << '\n';
            last_ms = total_time.getMicroseconds() / 1000;
        } while (total_time.getSeconds() < 4);
        results_file.close();
    }

    if (query10Settings.OlkenRate)
    {
        // do the sample rate test for olkin join
        Timer total_time;
        double last_ms = 0;
        int per_sample_request = 20;
        int total_samples = 0;
        std::ofstream output_file("query10_sample.txt", std::ios::out);
        std::ofstream results_file("query10_olkin_sample_rate.txt", std::ios::out);
        auto max_outdegree = jefastIndex->MaxOutdegree();
        total_time.reset();
        do {
            total_samples += baselineJoin(output_file, per_sample_request, max_outdegree);
            total_time.update_accumulator();

            total_time.update_accumulator();
            results_file << total_time.getMilliseconds() << '\t' << total_samples << '\t' << per_sample_request << '\t' << (total_time.getMicroseconds() / 1000 - last_ms) << '\n';
            last_ms = total_time.getMicroseconds() / 1000;
        } while (total_time.getSeconds() < 4);
        results_file.close();
    }

    data_map->flush();
    std::cout << "done" << std::endl;
}