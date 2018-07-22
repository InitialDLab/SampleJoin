// This implemented the different algorithms for query5
// Programmer: Robert Christensen
// email: robertc@cs.utah.edu

#include <iostream>
#include <fstream>
#include <memory>
#include <random>
#include <map>
#include <vector>
#include <array>
#include <thread>
#include <future>

#include "util/Timer.h"
#include "util/FileSizeTable.h"
#include "util/FileKeyValue.h"
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

#include "hard_coded/query9_jefast.h"
#include "hard_coded/query9_jefastBuilder.h"

static std::shared_ptr<Table> nation_table;
static std::shared_ptr<Table> orders_table;
static std::shared_ptr<Table> part_table;
static std::shared_ptr<Table> supplier_table;
static std::shared_ptr<Table> partsupp_table;
static std::shared_ptr<Table> lineitem_table;

static std::shared_ptr<Query9_JefastBuilder> jefast_index_builder;
static std::shared_ptr<query9_jefast> jefast_index;
static std::shared_ptr<FileKeyValue> data_map;

static std::multimap<std::tuple<jfkey_t, jfkey_t>, jfkey_t> Table4_index;

static global_settings query9Settings;

int64_t exactJoinNoIndex(std::string outfile) {
    // implements a straightforward implementation of a join which
    // does not require an index.

    std::ofstream output_file(outfile, std::ios::out);
    int64_t count = 0;

    auto Table_1 = nation_table;
    auto Table_2 = supplier_table;
    auto Table_3 = lineitem_table;
    auto Table_4 = partsupp_table;
    auto Table_5 = orders_table;
    auto Table_6 = part_table;

    int table1Index2 = Table_Nation::N_NATIONKEY;
    int table2Index1 = Table_Supplier::S_NATIONKEY;

    int table2Index2 = Table_Supplier::S_SUPPKEY;
    int table3Index1 = Table_Lineitem::L_SUPPKEY;

    //int table3Index2 = Table_Lineitem::L_PARTKEY;
    //int table4Index1 = Table_Partsupp::PS_SUP;

    // build the hash for table 1
    std::map<jfkey_t, std::vector<int64_t> > Table1_hash;
    int64_t Table_1_count = Table_1->row_count();
    for (int64_t i = 0; i < Table_1_count; ++i) {
        Table1_hash[Table_1->get_int64(i, table1Index2)].push_back(i);
    }

    // build the hash for table 2.  All matched elements from table 1 hash will be emitted
    // the tuple has the form <index from table 1, index for table 2> for all matching tuple
    std::map<jfkey_t, std::vector<std::tuple<int64_t, int64_t> > > Table2_hash;
    int64_t Table_2_count = Table_2->row_count();
    for (int64_t i = 0; i < Table_2_count; ++i) {
        jfkey_t value = Table_2->get_int64(i, table2Index1);
        for (auto matching_indexes : Table1_hash[value]) {
            Table2_hash[Table_2->get_int64(i, table2Index2)].emplace_back(matching_indexes, i);
        }
    }

    // build the hash for table 3.  All matched elements from table 2 hash will be emitted.
    //std::map<std::tuple<jfkey_t, jfkey_t>, std::vector<std::tuple<int64_t, int64_t, int64_t> > > Table3_hash;
    int64_t Table3_count = Table_3->row_count();
    for (int i3 = 0; i3 < Table3_count; ++i3) {
        jfkey_t value = Table_3->get_int64(i3, table3Index1);

        jfkey_t supp_value = Table_3->get_int64(i3, Table_Lineitem::L_SUPPKEY);
        jfkey_t part_value = Table_3->get_int64(i3, Table_Lineitem::L_PARTKEY);
        jfkey_t order_value = Table_3->get_int64(i3, Table_Lineitem::L_ORDERKEY);

        for (auto matching_indexes : Table2_hash[value]) {
            auto Table4_range = Table4_index.equal_range(std::tuple<jfkey_t, jfkey_t>(part_value, supp_value));
            auto Table5_range = Table_5->get_key_index(Table_Orders::O_ORDERKEY)->equal_range(order_value);
            auto Table6_range = Table_6->get_key_index(Table_Part::P_PARTKEY)->equal_range(part_value);

            for (auto first_i = Table4_range.first; first_i != Table4_range.second; ++first_i) {
                for (auto second_i = Table5_range.first; second_i != Table5_range.second; ++second_i) {
                    for (auto third_i = Table6_range.first; third_i != Table6_range.second; ++third_i) {
                        //output_file << count << ' '
                        //    << std::to_string(Table_1->get_int64(std::get<0>(matching_indexes), 0)) << ' '
                        //    << std::to_string(Table_2->get_int64(std::get<1>(matching_indexes), 0)) << ' '
                        //    << std::to_string(Table_3->get_int64(i3, 0))            << ' '
                        //    << std::to_string(Table_4->get_int64(first_i->second, 0))               << ' '
                        //    << std::to_string(Table_5->get_int64(second_i->second, 0))              << ' '
                        //    << std::to_string(Table_6->get_int64(third_i->second, 0))               << '\n';
                        ++count;
                    }
                }
            }
            

        }
    }

    // build the hash for table 4?  No, we can just emit when we find a match in this case.
    //int64_t Table_4_count = Table_4->row_count();
    ////for (int64_t i = 0; i < Table_5_count; ++i) {
    //for (auto f4_enu = filters.at(3)->getEnumerator(); f4_enu->Step();) {
    //    int64_t value = Table_4->get_int64(f4_enu->getValue(), table4Index1);
    //    for (auto matching_indexes : Table3_hash[value]) {
    //        // emit the join results here.
    //        output_file << count << ' '
    //            << Table_1->get_int64(std::get<0>(matching_indexes), table1Index2) << ' '
    //            << Table_2->get_int64(std::get<1>(matching_indexes), table2Index2) << ' '
    //            << Table_3->get_int64(std::get<2>(matching_indexes), table3Index2) << ' '
    //            << Table_4->get_int64(f4_enu->getValue(), 1) << '\n';
    //        ++count;
    //    }
    //}

    output_file.close();
    return count;
}

int64_t fulljefastJoin(std::string outfile, int64_t output_count) {
    // this will do a full table join using the jefast data structure
    // NOTE, jefast is not optimized for scanning right now.  It is
    //  optimized for point queries
    std::ofstream output_file(outfile, std::ios::out);

    std::vector<int64_t> results;

    auto Table_1 = nation_table;
    auto Table_2 = supplier_table;
    auto Table_3 = lineitem_table;
    auto Table_4 = partsupp_table;
    auto Table_5 = orders_table;
    auto Table_6 = part_table;

    //int table1Index2 = Table_Nation::N_NATIONKEY;
    //int table2Index2 = Table_Supplier::S_NATIONKEY;
    //int table3Index2 = Table_Customer::C_CUSTKEY;
    //int table4Index2 = Table_Orders::O_ORDERKEY;

    for (int64_t i = 0; i < output_count; ++i) {
        jefast_index->GetJoinNumber(i, results);

        //output_file << i << " "
        //    << std::to_string(Table_1->get_int64(results.at(0), 0)) << ' '
        //    << std::to_string(Table_2->get_int64(results.at(1), 0)) << ' '
        //    << std::to_string(Table_3->get_int64(results.at(2), 0)) << ' '
        //    << std::to_string(Table_4->get_int64(results.at(3), 0)) << ' '
        //    << std::to_string(Table_5->get_int64(results.at(4), 0)) << ' '
        //    << std::to_string(Table_6->get_int64(results.at(5), 0)) << '\n';
    }
    output_file.close();
    return output_count;
}

int64_t randomjefastJoin(std::ofstream &output_file, int64_t output_count, std::shared_ptr<query9_jefast> jefast) {
    // this will do a full table join using the jefast data structure
    // NOTE, jefast is not optimized for scanning right now.  It is
    //  optimized for point queries
    //std::ofstream output_file(outfile, std::ios::out);

    std::vector<int64_t> results;

    auto Table_1 = nation_table;
    auto Table_2 = supplier_table;
    auto Table_3 = lineitem_table;
    auto Table_4 = partsupp_table;
    auto Table_5 = orders_table;
    auto Table_6 = part_table;

    //int table1Index2 = Table_Nation::N_NATIONKEY;
    //int table2Index2 = Table_Supplier::S_NATIONKEY;
    //int table3Index2 = Table_Customer::C_CUSTKEY;
    //int table4Index2 = Table_Orders::O_ORDERKEY;

    static std::default_random_engine generator;

    int64_t max = jefast->GetTotal();
    std::vector<weight_t> random_values;
    std::uniform_int_distribution<weight_t> distribution(0, max-1);
    random_values.resize(output_count);

    std::generate(random_values.begin(), random_values.end(), [&]() {return distribution(generator);});

    std::sort(random_values.begin(), random_values.end());

    int counter = 0;
    for (auto i : random_values) {
        jefast->GetJoinNumber(i, results);

        //output_file << counter << " "
        //    << std::to_string(Table_1->get_int64(results.at(0), 0)) << ' '
        //    << std::to_string(Table_2->get_int64(results.at(1), 0)) << ' '
        //    << std::to_string(Table_3->get_int64(results.at(2), 0)) << ' '
        //    << std::to_string(Table_4->get_int64(results.at(3), 0)) << ' '
        //    << std::to_string(Table_5->get_int64(results.at(4), 0)) << '\n';
        ++counter;
    }
    output_file.close();
    return output_count;
}

int64_t randomjefastJoin(std::string outfile, int64_t output_count, std::shared_ptr<query9_jefast> jefast) {
    // this will do a full table join using the jefast datastructure
    // NOTE, jefast is not optimized for scanning right now.  It is
    //  optimized for point queries
    std::ofstream output_file(outfile, std::ios::out);

    int64_t count = randomjefastJoin(output_file, output_count, jefast);
    output_file.close();
    return count;
}

int64_t randomjefastJoinT(std::string outfile, int64_t output_count, std::shared_ptr<query9_jefast> jefast) {
    return randomjefastJoin(outfile, output_count, jefast);
}

int64_t baselineJoin(std::ofstream &output_file, int output_count, weight_t max_cardinality_coef) {

    auto Table_1 = nation_table;
    auto Table_2 = supplier_table;
    auto Table_3 = lineitem_table;
    auto Table_4 = partsupp_table;
    auto Table_5 = orders_table;
    auto Table_6 = part_table;

    int table1Index2 = Table_Nation::N_NATIONKEY;
    int table2Index1 = Table_Supplier::S_NATIONKEY;

    int table2Index2 = Table_Supplier::S_SUPPKEY;
    int table3Index1 = Table_Lineitem::L_SUPPKEY;

    int64_t count = 0;
    int64_t rejected = 0;
    int64_t max_possible_path = 0;

    std::default_random_engine generator;

    //std::ofstream output_file(outfile, std::ios::out);

    int64_t Table1_size = Table_1->row_count();
    std::uniform_int_distribution<int64_t> region_dist(0, Table1_size - 1);
    std::uniform_int_distribution<int64_t> rejection_filter(1, max_cardinality_coef);

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
        //int64_t Table3_v = Table_3->get_int64(Table3_i->second, table3Index2);
        jfkey_t supp_value = Table_3->get_int64(Table3_i->second, Table_Lineitem::L_SUPPKEY);
        jfkey_t part_value = Table_3->get_int64(Table3_i->second, Table_Lineitem::L_PARTKEY);
        jfkey_t order_value = Table_3->get_int64(Table3_i->second, Table_Lineitem::L_PARTKEY);

        // do random selection of Table4 (partsupp)
        auto Table4_range = Table4_index.equal_range(std::tuple<jfkey_t, jfkey_t>(part_value, supp_value));

        int64_t Table4_count = Table4_index.count(std::tuple<jfkey_t, jfkey_t>(part_value, supp_value));
        if (Table4_count == 0)
            continue;
        std::uniform_int_distribution<int64_t> Table4_dist(0, Table4_count - 1);
        int64_t selection_count_4 = Table4_dist(generator);
        auto Table4_i = Table4_range.first;
        while (selection_count_4 > 0) {
            Table4_i++;
            --selection_count_4;
        }
        int64_t Table4_v = Table_4->get_int64(Table4_i->second, 0);


        // do random selection of Table5
        auto Table5_range = Table_5->get_key_index(Table_Orders::O_ORDERKEY)->equal_range(order_value);
        int64_t Table5_count = Table_5->get_key_index(Table_Orders::O_ORDERKEY)->count(order_value);
        if (Table5_count == 0)
            continue;
        std::uniform_int_distribution<int64_t> Table5_dist(0, Table5_count - 1);
        int64_t selection_count_5 = Table5_dist(generator);
        auto Table5_i = Table5_range.first;
        while (selection_count_5 > 0) {
            Table5_i++;
            --selection_count_5;
        }
        int64_t Table5_v = Table_5->get_int64(Table5_i->second, 0);

        // do random selection of Table5
        auto Table6_range = Table_6->get_key_index(Table_Part::P_PARTKEY)->equal_range(part_value);
        int64_t Table6_count = Table_6->get_key_index(Table_Part::P_PARTKEY)->count(part_value);
        if (Table6_count == 0)
            continue;
        std::uniform_int_distribution<int64_t> Table6_dist(0, Table6_count - 1);
        int64_t selection_count_6 = Table6_dist(generator);
        auto Table6_i = Table6_range.first;
        while (selection_count_5 > 0) {
            Table6_i++;
            --selection_count_6;
        }
        int64_t Table6_v = Table_6->get_int64(Table6_i->second, 0);

        // decide if we should reject
        int64_t possible_paths = Table2_count * Table3_count * Table4_count;
        max_possible_path = std::max(possible_paths, max_possible_path);
        // if true, accept
        if (rejection_filter(generator) < possible_paths) {
            //output_file << count << ' '
            //    << Table_1->get_int64(Table1_idx, 0) << ' '
            //    << Table_2->get_int64(Table2_i->second, 0) << ' '
            //    << Table_3->get_int64(Table3_i->second, 0) << ' '
            //    << Table4_v << ' '
            //    << Table5_v << ' '
            //    << Table6_v << '\n';
            count++;
        }
        else {
            rejected++;
        }
    }

    output_file.close();
    return rejected;
}

int64_t baselineJoin(std::string outfile, int output_count, weight_t max_cardinality) {
    // the exact join will combine between the four tables

    std::ofstream output_file(outfile, std::ios::out);

    int64_t count = baselineJoin(output_file, output_count, max_cardinality);

    output_file.close();
    return count;
}

void setup_data() {
    Timer timer;
    FileSizeTable table_sizes("fileInfo.txt");
    data_map.reset(new FileKeyValue("query9_timings.txt"));

    data_map->insert("timing_resolution", "ms");

    std::cout << "opening tables" << std::endl;
    timer.start();
    nation_table.reset(new Table_Nation("nation.tbl", table_sizes.get_lines("nation.tbl")));
    orders_table.reset(new Table_Orders("orders.tbl", table_sizes.get_lines("orders.tbl")));
    part_table.reset(new Table_Part("part.tbl", table_sizes.get_lines("part.tbl")));
    supplier_table.reset(new Table_Supplier("supplier.tbl", table_sizes.get_lines("supplier.tbl")));
    partsupp_table.reset(new Table_Partsupp("partsupp.tbl", table_sizes.get_lines("partsupp.tbl")));
    lineitem_table.reset(new Table_Lineitem("lineitem.tbl", table_sizes.get_lines("lineitem.tbl")));
    timer.stop();
    std::cout << "opening tables took " << timer.getSeconds() << " Seconds" << std::endl;
    data_map->appendArray("opening_tables", long(timer.getMilliseconds()));

    std::cout << "building indexes" << std::endl;
    timer.reset();
    timer.start();
    if (query9Settings.buildIndex)
    {
        nation_table->get_key_index(Table_Nation::N_NATIONKEY);
        orders_table->get_key_index(Table_Orders::O_ORDERKEY);
        part_table->get_key_index(Table_Part::P_PARTKEY);
        supplier_table->get_key_index(Table_Supplier::S_SUPPKEY);
        //partsupp_table->get_key_index(Table_Partsupp::PS);
        lineitem_table->get_key_index(Table_Lineitem::L_SUPPKEY);

        for (int i = 0; i < partsupp_table->row_count(); ++i) {
            Table4_index.insert(std::pair<std::tuple<jfkey_t, jfkey_t>, jfkey_t>(
                std::tuple<jfkey_t, jfkey_t>(partsupp_table->get_int64(i, Table_Partsupp::PS_PARTKEY)
                    , partsupp_table->get_int64(i, Table_Partsupp::PS_SUPPKEY)), i));
        }
    }
    std::cout << "linitem index has " << Table4_index.size() << " elements of " << partsupp_table->row_count() << std::endl;
    timer.stop();
    std::cout << "building basic indexes took " << timer.getMilliseconds() << " milliseconds" << std::endl;
}

int main(int argc, char** argv) {
    query9Settings = parse_args(argc, argv);

    Timer timer;
    setup_data();

    if(query9Settings.hashJoin)
    {
       // do a full join
       std::cout << "doing full join" << std::endl;
       timer.reset();
       timer.start();
       auto count = exactJoinNoIndex("query9_full.txt");
       timer.stop();
       std::cout << "full join took " << timer.getMilliseconds() << " milliseconds with cardinality " << count << std::endl;
       data_map->appendArray("full_join", long(timer.getMilliseconds()));
       data_map->appendArray("full_join_cardinality", long(count));
    }

    if(query9Settings.buildJefast)
    {
        // build jefast
        std::cout << "building jefast" << std::endl;
        timer.reset();
        timer.start();
        jefast_index_builder.reset(new Query9_JefastBuilder(nation_table, supplier_table, lineitem_table, partsupp_table, orders_table, part_table));
        jefast_index = jefast_index_builder->Build();
        timer.stop();
        std::cout << "building jefast took " << timer.getMilliseconds() << " milliseconds with cardinality " << jefast_index->GetTotal() << std::endl;

        data_map->appendArray("building_jefast", timer.getMilliseconds());
    }

    if(query9Settings.jefastSample)
    {
       std::cout << "Getting 10% samples using jefast" << std::endl;
       timer.reset();
       timer.start();
       randomjefastJoin("query9_samples10p.txt", jefast_index->GetTotal() / 10, jefast_index);
       timer.stop();
       std::cout << "getting 10% samples took " << timer.getMilliseconds() << " milliseconds" << std::endl;
       data_map->appendArray("jefast_10p", long(timer.getMilliseconds()));
    }

    if(query9Settings.olkenSample)
    {
       int64_t coeffecient = jefast_index->getOlkinCoeffecient();
       std::cout << "Getting 10% samples using baseline" << std::endl;
       timer.reset();
       timer.start();
       baselineJoin("query9_samples10p.txt", 100000, coeffecient);
       timer.stop();
       std::cout << "getting 10% samples took " << timer.getMilliseconds() << " milliseconds" << std::endl;
       data_map->appendArray("baseline_100000p", long(timer.getMilliseconds()));
    }

    if(query9Settings.jefastRate)
    {
        // do the sample rate test for jefast
        Timer total_time;
        double last_ms = 0;
        int per_sample_request = 4000 * 2;
        int total_samples = 0;
        std::ofstream output_file("query9_sample.txt", std::ios::out);
        std::ofstream results_file("query9_jefast_sample_rate.txt", std::ios::out);
        total_time.reset();
        do {
            total_samples += randomjefastJoin(output_file, per_sample_request, jefast_index);
            total_time.update_accumulator();

            total_time.update_accumulator();
            results_file << total_time.getMilliseconds() << '\t' << total_samples << '\t' << per_sample_request << '\t' << (total_time.getMicroseconds() / 1000 - last_ms) << '\n';
            last_ms = total_time.getMicroseconds() / 1000;
        } while (total_time.getSeconds() < 4);
        results_file.close();
    }

    if(query9Settings.OlkenRate)
    {
        // do the sample rate test for olkin join
        Timer total_time;
        double last_ms = 0;
        int per_sample_request = 100 * 2;
        int total_samples = 0;
        std::ofstream output_file("query9_sample.txt", std::ios::out);
        std::ofstream results_file("query9_olkin_sample_rate.txt", std::ios::out);
        auto max_outdegree = jefast_index->getOlkinCoeffecient();
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

    // do threading experiment for jefast
    if (query9Settings.threading)
    {
        int64_t max_threads = 8;
        int64_t requests = jefast_index->GetTotal() / 10;

        std::vector<std::future<int64_t> > results;
        // open all the files
        // do the trials.
        for (int64_t i = 1; i <= max_threads; i *= 2) {
            timer.reset();
            timer.start();
            // start the threads
            for (int t = 0; t < i; ++t) {
                results.emplace_back(std::async(randomjefastJoinT, query9Settings.null_file_name, requests / i, jefast_index));
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

    data_map->flush();
    std::cout << "done" << std::endl;
}