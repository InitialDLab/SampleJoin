#include <iostream>
#include <string>
#include <chrono>
#include <ratio>

#include "tclap-1.2.1/include/tclap/CmdLine.h"

#include <cstdint>
#include <vector>
#include <fstream>
#include <thread>
#include <atomic>

#include "database/TableGeneric.h"
#include "database/AdaptiveProbabilityIndex.h"

#include "database/jefastBuilder.h"
#include "database/jefastFilter.h"
#include "database/jefastBuilderWJoinAttribSelection.h"
#include "database/jefastBuilderWNonJoinAttribSelection.h"
#include "database/DynamicIndex.h"
#include "database/olkenIndex.h"

// if we are running a long running experiment (such as baseline) we exit after this
// many seconds
double experiment_timeout = 3600.0;

struct generic_settings {
    std::string base_directory = ".";

    int scale_factor;
    double experiment_timeout;

    int trials;

    bool do_Q3;
    bool do_QX;
    bool do_QY;
    bool do_triangle;
    bool do_bigtriangle;
    bool do_smallsquare;
    bool do_bigsquare;
    bool do_SkewTri;

    int l_extendedprice;
    int n_nationkey;
};

generic_settings settings;

void TCP3(int sf = 10)
{
    std::cout << "loading data..." << std::endl;

    std::vector<int> sample_count = { 1000, 10000, 100000, 1000000 };
    //std::vector<int> sample_count = { 1000 };
    std::vector<int> table_conditions = { 10000, 30000, 50000, 70000 };

    // load the data
    std::shared_ptr<TableGeneric> table1(new TableGeneric(std::to_string(sf) + "x/customer.tbl", '|', 1, 1));
    std::shared_ptr<TableGeneric> table2(new TableGeneric(std::to_string(sf) + "x/orders.tbl", '|', 2, 1));
    std::shared_ptr<TableGeneric> table3(new TableGeneric(std::to_string(sf) + "x/lineitem.tbl", '|', 1, 6));

    std::map<int, std::shared_ptr<TableGeneric_encap> > pre_selected;
    std::map<int, std::shared_ptr<TableGeneric> > pre_selected_gen;
    for (auto condition : table_conditions)
    {
        std::shared_ptr<TableGeneric> temp(new TableGeneric(std::to_string(sf) + "x/lineitem_skew_ex" + std::to_string(condition) + ".tbl", '|', 1, 1));
        pre_selected_gen[condition] = std::shared_ptr<TableGeneric>(temp);
        pre_selected[condition] = std::shared_ptr<TableGeneric_encap>(new TableGeneric_encap(temp));
    }

    table1->Build_indexes();
    table2->Build_indexes();
    table3->Build_indexes();

    std::shared_ptr<TableGeneric_encap> table1G(new TableGeneric_encap(table1));
    std::shared_ptr<TableGeneric_encap> table2G(new TableGeneric_encap(table2));
    std::shared_ptr<TableGeneric_encap> table3G(new TableGeneric_encap(table3));

    std::default_random_engine generator;
    generator.seed(std::chrono::steady_clock::now().time_since_epoch().count());

    std::vector<weight_t> max_fanout;

    std::cout << "adaptive..." << std::endl;
    // do experiment for adaptive
    for(auto condition : table_conditions)
    {
        std::ofstream output_file;
        output_file.open(std::to_string(sf) + "x_Q3_adaptive_condition_" + std::to_string(condition) + ".txt");

        for (auto samples_to_collect : sample_count)
        {
            std::vector<int> results(3);

            DynamicIndex d_idx(3);

            d_idx.set_final_selection_min_condition(condition);

            d_idx.add_Table(0, table1, ((int64_t)5000) * 5000 * 5000);
            d_idx.add_Table(1, table2, ((int64_t)5000) * 5000);
            //d_idx.add_Table(2, table3, ((int64_t)5000));
            d_idx.add_Table(2, pre_selected_gen[condition], ((int64_t)5000));
            d_idx.initialize();

            std::chrono::steady_clock::time_point build_t1 = std::chrono::steady_clock::now();
            d_idx.initialize(2);
            d_idx.warmup();
            std::chrono::steady_clock::time_point build_t2 = std::chrono::steady_clock::now();

            std::chrono::steady_clock::time_point sample_t1 = std::chrono::steady_clock::now();

            for (int good_samples = 0; good_samples < samples_to_collect; )
            {
                bool good = d_idx.sample_join(results);
                if (good)
                    ++good_samples;
            }

            std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

            output_file << samples_to_collect << '\t'
                << std::chrono::duration_cast<std::chrono::duration<double>>(build_t2 - build_t1).count() << '\t'
                << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count() << '\n';
        }

        output_file.close();
    }

    std::cout << "DP..." << std::endl;
    // do experiment for DP
    for (auto condition : table_conditions)
    {
        std::ofstream output_file;
        output_file.open(std::to_string(sf) + "x_Q3_jefast_condition_" + std::to_string(condition) + ".txt");

        for (auto samples_to_collect : sample_count)
        {
            std::vector<jefastKey_t> results;
            std::vector<weight_t> random_values(samples_to_collect);

            std::chrono::steady_clock::time_point build_t1 = std::chrono::steady_clock::now();

            JefastBuilder jefast_index_builder;
            jefast_index_builder.AppendTable(table1G, -1, 1, 0);
            jefast_index_builder.AppendTable(table2G, 0, 1, 1);
            jefast_index_builder.AppendTable(pre_selected[condition], 0, -1, 2);

            auto jefast = jefast_index_builder.Build();

            std::chrono::steady_clock::time_point build_t2 = std::chrono::steady_clock::now();
            std::chrono::steady_clock::time_point sample_t1 = std::chrono::steady_clock::now();

            std::uniform_int_distribution<weight_t> distribution(0, jefast->GetTotal() - 1);

            //std::generate(random_values.begin(), random_values.end(), [&]() {return distribution(generator);});
            //std::sort(random_values.begin(), random_values.end());

            //for (int i : random_values) {
            //    jefast->GetJoinNumber(i, results);
            //}

            for (int i = 0; i < samples_to_collect; ++i)
            {
                jefast->GetJoinNumber(distribution(generator), results);
            }

            std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

            output_file << samples_to_collect << '\t'
                << std::chrono::duration_cast<std::chrono::duration<double>>(build_t2 - build_t1).count() << '\t'
                << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count() << '\n';


            // this is to be used by baseline join
            if (max_fanout.size() == 0)
                max_fanout = jefast->MaxOutdegree();
        }

        output_file.close();
    }

    std::cout << "baseline..." << std::endl;
    // do experiment for baseline
    for (auto condition : table_conditions)
    {
        std::ofstream output_file;
        output_file.open(std::to_string(sf) + "x_Q3_baseline.txt");
        for (auto i : max_fanout)
            std::cout << i << " ";
        std::cout << std::flush;

        olkenIndex Ojoin(3);
        Ojoin.add_Table(0, table1, 1.0);
        Ojoin.add_Table(1, table2, max_fanout[0]);
        Ojoin.add_Table(2, table3, max_fanout[1]);
        //Ojoin.add_Table(3, table4, max_fanout[2]);
        //Ojoin.add_Table(4, table5, max_fanout[3]);

        for (int trial = 0; trial < settings.trials; ++trial) {
            for (auto samples_to_collect : sample_count)
            {
                std::vector<jefastKey_t> results;

                std::chrono::steady_clock::time_point sample_t1 = std::chrono::steady_clock::now();

                int trials = 0;

                for (int good_samples = 0; good_samples < samples_to_collect; ++trials)
                {
                    if (std::chrono::duration_cast<std::chrono::duration<double>>(std::chrono::steady_clock::now() - sample_t1).count() > experiment_timeout)
                        break;

                    if (Ojoin.sample_join(results) && table1->get_column1_value(results[0]) > condition)
                    {
                        ++good_samples;
                    }
                }


                std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

                output_file << samples_to_collect << '\t'
                    << 0.0 /* we say build time is nothing */ << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count() << std::endl;

                std::cout << trials << ' ' << samples_to_collect << std::endl;
            }
        }
    }
}

void TCPX(int sf = 10)
{
    // open tables
    //std::vector<int> sample_count = { 1000, 10000, 100000, 1000000 };
    std::vector<int> sample_count = { 1000000 };

    //std::vector<int> table_conditions = { 10000, 30000, 50000, 70000 };
    int condition = settings.n_nationkey;
    int condition2 = settings.l_extendedprice;

    // load the data
    std::cout << "opening tables" << std::endl;
    std::shared_ptr<TableGeneric> table1(new TableGeneric(std::to_string(sf) + "x/nation.tbl", '|', 1, 1));
    std::shared_ptr<TableGeneric> table2(new TableGeneric(std::to_string(sf) + "x/supplier.tbl", '|', 4, 4));
    std::shared_ptr<TableGeneric> table3(new TableGeneric(std::to_string(sf) + "x/customer.tbl", '|', 4, 1));
    std::shared_ptr<TableGeneric> table4(new TableGeneric(std::to_string(sf) + "x/orders.tbl", '|', 2, 1));
    std::shared_ptr<TableGeneric> table5(new TableGeneric(std::to_string(sf) + "x/lineitem_skew.tbl", '|', 1, 6));
    
    std::cout << "nation.size() = " << table1->get_size() << std::endl;
    std::cout << "lineitem.size() = " << table5->get_size() << std::endl;
    std::cout << "start filtering" << std::endl;
    std::chrono::steady_clock::time_point filter_t1 = std::chrono::steady_clock::now();
    table1->filter_column(1, condition);
    table5->filter_column(2, condition2);
    std::chrono::steady_clock::time_point filter_t2 = std::chrono::steady_clock::now();
    std::cout << "filter time: " 
        << std::chrono::duration_cast<std::chrono::duration<double>>(filter_t2 - filter_t1).count() << std::endl;
    std::cout << "nation_filtered.size() = " << table1->get_size() << std::endl;
    std::cout << "lineitem_filtered.size() = " << table5->get_size() << std::endl;

    std::cout << "building index" << std::endl;
    table1->Build_indexes();
    table2->Build_indexes();
    table3->Build_indexes();
    table4->Build_indexes();
    table5->Build_indexes();

    std::map<int, std::shared_ptr<TableGeneric_encap> > pre_selected;
    std::map<int, std::shared_ptr<TableGeneric> > pre_selected_gen;
    /*for (auto condition : table_conditions)
    {
        std::shared_ptr<TableGeneric> temp(new TableGeneric(std::to_string(sf) + "x/lineitem_skew_ex" + std::to_string(condition) + ".tbl", '|', 1, 1));
        pre_selected_gen[condition] = std::shared_ptr<TableGeneric>(temp);
        pre_selected[condition] = std::shared_ptr<TableGeneric_encap>(new TableGeneric_encap(temp));
    } */

    std::shared_ptr<TableGeneric_encap> table1G(new TableGeneric_encap(table1));
    std::shared_ptr<TableGeneric_encap> table2G(new TableGeneric_encap(table2));
    std::shared_ptr<TableGeneric_encap> table3G(new TableGeneric_encap(table3));
    std::shared_ptr<TableGeneric_encap> table4G(new TableGeneric_encap(table4));
    std::shared_ptr<TableGeneric_encap> table5G(new TableGeneric_encap(table5));

    std::default_random_engine generator;
    generator.seed(std::chrono::steady_clock::now().time_since_epoch().count());

    std::vector<weight_t> max_fanout;

    // do experiment for adaptive
    std::cout << "doing adaptive" << std::endl;
    for (int warmup_size = 1; warmup_size < 2; ++warmup_size)
    {
        //for (auto condition : table_conditions)
        {
            std::ofstream output_file;
            output_file.open(std::to_string(sf) + "x_QX_adaptive_W" + std::to_string(warmup_size) + "_condition_" + std::to_string(condition) + "_" + std::to_string(condition2) + ".txt");
            output_file << "filter time: " 
                << std::chrono::duration_cast<std::chrono::duration<double>>(filter_t2 - filter_t1).count() << std::endl;

            for (auto samples_to_collect : sample_count)
            {
                std::vector<int> results(5);

                DynamicIndex d_idx(5);

                //d_idx.set_final_selection_min_condition(condition);

                d_idx.add_Table(0, table1, (int64_t)1000 * 1000 * 1000 * 1000 * 1000);
                d_idx.add_Table(1, table2, (int64_t)1000 * 1000 * 1000 * 1000);
                d_idx.add_Table(2, table3, (int64_t)1000 * 1000 * 1000);
                d_idx.add_Table(3, table4, (int64_t)1000 * 1000);
                d_idx.add_Table(4, table5, (int64_t)1000);

                std::chrono::steady_clock::time_point build_t1 = std::chrono::steady_clock::now();
                d_idx.initialize(1);
                d_idx.warmup();
                std::chrono::steady_clock::time_point build_t2 = std::chrono::steady_clock::now();

                std::chrono::steady_clock::time_point sample_t1 = std::chrono::steady_clock::now();

                for (int good_samples = 0; good_samples < samples_to_collect; )
                {
                    bool good = d_idx.sample_join(results);
                    if (good)
                        ++good_samples;
                }

                std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

                output_file << samples_to_collect << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(build_t2 - build_t1).count() << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count() << '\n';
            }

            output_file.close();
        }
    }

    // do experiment for DP
    std::cout << "doing DP" << std::endl;
    //for (auto condition : table_conditions)
    {
        std::ofstream output_file;
        output_file.open(std::to_string(sf) + "x_QX_jefast_condition_" + std::to_string(condition) + "_" + std::to_string(condition2) + ".txt");
        output_file << "filter time: " 
            << std::chrono::duration_cast<std::chrono::duration<double>>(filter_t2 - filter_t1).count() << std::endl;

        for (auto samples_to_collect : sample_count)
        {
            std::vector<jefastKey_t> results;
            std::vector<weight_t> random_values(samples_to_collect);

            std::chrono::steady_clock::time_point build_t1 = std::chrono::steady_clock::now();

            JefastBuilder jefast_index_builder;
            jefast_index_builder.AppendTable(table1G, -1, 1, 0);
            jefast_index_builder.AppendTable(table2G, 0, 1, 1);
            jefast_index_builder.AppendTable(table3G, 0, 1, 2);
            jefast_index_builder.AppendTable(table4G, 0, 1, 3);
            jefast_index_builder.AppendTable(table5G, 0, -1, 4);

            auto jefast = jefast_index_builder.Build();

            std::chrono::steady_clock::time_point build_t2 = std::chrono::steady_clock::now();
            std::chrono::steady_clock::time_point sample_t1 = std::chrono::steady_clock::now();

            std::uniform_int_distribution<weight_t> distribution(0, jefast->GetTotal() - 1);

            //std::generate(random_values.begin(), random_values.end(), [&]() {return distribution(generator);});
            //std::sort(random_values.begin(), random_values.end());

            for (int i = 0; i < samples_to_collect; ++i)
            {
                jefast->GetJoinNumber(distribution(generator), results);
            }

            std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

            output_file << samples_to_collect << '\t'
                << std::chrono::duration_cast<std::chrono::duration<double>>(build_t2 - build_t1).count() << '\t'
                << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count() << '\n';


            // this is to be used by baseline join
            if (max_fanout.size() == 0)
                max_fanout = jefast->MaxOutdegree();
        }

        output_file.close();
    }

    // do experiment for baseline
    std::cout << "doing baseline" << std::endl;
    //for (auto condition : table_conditions)
    {
        std::ofstream output_file;
        output_file.open(std::to_string(sf) + "x_QX_baseline_condition_" + std::to_string(condition) + "_" + std::to_string(condition2) + ".txt");
        output_file << "filter time: " 
            << std::chrono::duration_cast<std::chrono::duration<double>>(filter_t2 - filter_t1).count() << std::endl;

        for (auto i : max_fanout)
            std::cout << i << " ";
        std::cout << std::endl;

        olkenIndex Ojoin(5);
        Ojoin.add_Table(0, table1, 1.0);
        Ojoin.add_Table(1, table2, max_fanout[0]);
        Ojoin.add_Table(2, table3, max_fanout[1]);
        Ojoin.add_Table(3, table4, max_fanout[2]);
        Ojoin.add_Table(4, table5, max_fanout[3]);

        for (int trial = 0; trial < settings.trials; ++trial) {
            for (auto samples_to_collect : sample_count )
            {
                std::vector<jefastKey_t> results;

                std::chrono::steady_clock::time_point sample_t1 = std::chrono::steady_clock::now();

                int trials = 0;

                for (int good_samples = 0; good_samples < samples_to_collect; ++trials)
                {
                    if (std::chrono::duration_cast<std::chrono::duration<double>>(std::chrono::steady_clock::now() - sample_t1).count() > experiment_timeout)
                        break;

                    if (Ojoin.sample_join(results)
                    //&& (table5->get_column1_value(results[4]) > condition)
                    )
                    {
                        ++good_samples;
                    }
                }


                std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

                output_file << samples_to_collect << '\t'
                    << 0.0 /* we say build time is nothing */ << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count() << std::endl;

                std::cout << trials << ' ' << samples_to_collect << std::endl;
            }
        }
    }
}

//void triangle(std::string file_name)
//{
//    std::cout << "loading data..." << std::endl;
//
//    std::vector<int> sample_count = { 1000, 10000, 100000, 1000000 };
//    //std::vector<int> sample_count = { 1000, 10000 };
//    std::vector<int> table_conditions{ 25000000, 50000000, 75000000 };
//
//    // load the data
//    std::shared_ptr<TableGeneric> table(new TableGeneric(file_name, '\t', 1, 2));
//
//    table->Build_indexes();
//
//    std::map<int, std::shared_ptr<TableGeneric_encap> > pre_selected;
//    for (auto condition : table_conditions)
//    {
//        std::shared_ptr<TableGeneric> temp(new TableGeneric(file_name + "_" + std::to_string(condition), '\t', 1, 2));
//        pre_selected[condition] = pre_selected[condition] = std::shared_ptr<TableGeneric_encap>(new TableGeneric_encap(temp));
//    }
//
//    std::shared_ptr<TableGeneric_encap> table1G(new TableGeneric_encap(table));
//    std::shared_ptr<TableGeneric_encap> table2G(new TableGeneric_encap(table));
//    std::shared_ptr<TableGeneric_encap> table3G(new TableGeneric_encap(table));
//
//    std::default_random_engine generator;
//    generator.seed(std::chrono::steady_clock::now().time_since_epoch().count());
//
//    std::vector<weight_t> max_fanout;
//
//
//    // do experiment for adaptive
//    for (auto condition : table_conditions)
//    {
//        std::ofstream output_file;
//        output_file.open("generic_TW_adaptive.txt");
//
//        for (auto samples_to_collect : sample_count)
//        {
//            std::vector<int> results(3);
//
//            DynamicIndex d_idx(3);
//
//            d_idx.set_first_selection_min_condition(condition);
//
//            d_idx.add_Table(0, table, ((int64_t)9000) * 9000 * 9000);
//            d_idx.add_Table(1, table, ((int64_t)9000) * 9000);
//            d_idx.add_Table(2, table, ((int64_t)9000));
//            d_idx.initialize();
//
//            std::chrono::steady_clock::time_point sample_t1 = std::chrono::steady_clock::now();
//
//            for (int good_samples = 0; good_samples < samples_to_collect; )
//            {
//                bool good = d_idx.sample_join(results);
//                if (good // for triangle join we must also validate the triangle part of the join
//                    //&& table->get_column1_value(results[0]) == table->get_column2_value(results[2]))
//                    )
//                    ++good_samples;
//            }
//
//            std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();
//
//            output_file << samples_to_collect << '\t'
//                << 0.0 /* we say build time is nothing */ << '\t'
//                << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count() << '\n';
//        }
//
//        output_file.close();
//    }
//
//    // do experiment for DP
//    for (auto condition : table_conditions)
//    {
//        std::ofstream output_file;
//        output_file.open("generic_TW_jefast.txt");
//
//        for (auto samples_to_collect : sample_count)
//        {
//            std::vector<jefastKey_t> results;
//            //std::vector<weight_t> random_values(samples_to_collect);
//
//            int remaining_samples = samples_to_collect;
//
//            std::chrono::steady_clock::time_point build_t1 = std::chrono::steady_clock::now();
//
//            JefastBuilder jefast_index_builder;
//            jefast_index_builder.AppendTable(pre_selected[condition], -1, 1, 0);
//            jefast_index_builder.AppendTable(table2G, 0, 1, 1);
//            jefast_index_builder.AppendTable(table3G, 0, -1, 2);
//
//            auto jefast = jefast_index_builder.Build();
//
//            std::chrono::steady_clock::time_point build_t2 = std::chrono::steady_clock::now();
//            std::chrono::steady_clock::time_point sample_t1 = std::chrono::steady_clock::now();
//
//            std::uniform_int_distribution<weight_t> distribution(0, jefast->GetTotal() - 1);
//
//
//            //std::generate(random_values.begin(), random_values.end(), [&]() {return distribution(generator);});
//            //std::sort(random_values.begin(), random_values.end());
//
//            //for (int i : random_values) {
//            while (remaining_samples > 0) {
//                int64_t r_val = distribution(generator);
//                jefast->GetJoinNumber(r_val, results);
//                //if (table->get_column1_value(results[0]) == table->get_column2_value(results[2]))
//                    --remaining_samples;
//            }
//
//
//            std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();
//
//            output_file << samples_to_collect << '\t'
//                << std::chrono::duration_cast<std::chrono::duration<double>>(build_t2 - build_t1).count() << '\t'
//                << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count() << '\n';
//
//            // this is to be used by baseline join
//            if (max_fanout.size() == 0)
//                max_fanout = jefast->MaxOutdegree();
//        }
//
//
//
//        output_file.close();
//    }
//
//    // do baseline (takes too much time to process)
//    std::cout << "adaptive-O" << std::endl;
//    for (auto condition : table_conditions)
//    {
//        std::ofstream output_file;
//        output_file.open(file_name + "_generic_TW_adaptive-O.txt");
//
//        for (auto samples_to_collect : sample_count)
//        {
//            std::vector<int> results(3);
//
//            DynamicIndex d_idx(3);
//
//            d_idx.add_Table(0, table, ((int64_t)max_fanout[1] * max_fanout[0] * 10000 /* magic number! */));
//            d_idx.add_Table(1, table, ((int64_t)max_fanout[1] * max_fanout[0]));
//            d_idx.add_Table(2, table, ((int64_t)max_fanout[1]));
//
//            d_idx.set_first_selection_min_condition(condition);
//
//            d_idx.initialize();
//
//            std::chrono::steady_clock::time_point sample_t1 = std::chrono::steady_clock::now();
//
//            for (int good_samples = 0; good_samples < samples_to_collect; )
//            {
//                bool good = d_idx.sample_join(results);
//                if (good // for triangle join we must also validate the triangle part of the join
//                    //&& table->get_column1_value(results[0]) == table->get_column2_value(results[2]))
//                    )
//                    ++good_samples;
//            }
//
//            std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();
//
//            output_file << samples_to_collect << '\t'
//                << 0.0 /* we say build time is nothing */ << '\t'
//                << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count() << '\n';
//        }
//
//        output_file.close();
//    }
//}

void parse_args_condition(int argc, char ** argv)
{
    TCLAP::CmdLine cmd("Experiments for generic samples with scale factor options", ' ');

    TCLAP::ValueArg<int> arg_SF("S", "SF", "scale factor to use for experiments.", false, 10, "int");

    TCLAP::SwitchArg arg_doQ3("", "do_Q3", "skip query 3 experiments", cmd, false);
    TCLAP::SwitchArg arg_doQX("", "do_QX", "skip query X experiments", cmd, false);
    TCLAP::SwitchArg arg_doSTri("", "do_smallTri", "skip the small triangle join", cmd, false);
    TCLAP::ValueArg<int> arg_l_extendedprice("", "l_extendedprice", "add filter l_extendedprice >=", false, 0, "int");
    TCLAP::ValueArg<int> arg_n_nationkey("", "n_nationkey", "add filter n_nationkey >=", false, 0, "int");

    cmd.add(arg_SF);
    cmd.add(arg_l_extendedprice);
    cmd.add(arg_n_nationkey);

    cmd.parse(argc, argv);

    settings.scale_factor = arg_SF.getValue();

    settings.do_Q3 = arg_doQ3.getValue();
    settings.do_QX = arg_doQX.getValue();
    settings.do_triangle = arg_doSTri.getValue();
    settings.l_extendedprice = arg_l_extendedprice.getValue();
    settings.n_nationkey = arg_n_nationkey.getValue();
    settings.trials = 1;
}

int main(int argc, char **argv)
{
    parse_args_condition(argc, argv);

    std::cout << "using sf=" << settings.scale_factor << std::endl;

    if (settings.do_Q3) {
        std::cout << "doing tcp q3 experiments" << std::endl;
        TCP3(settings.scale_factor);
    }
    else
    {
        std::cout << "skipping tcp q3 experiments" << std::endl;
    }

    if (settings.do_QX) {
        std::cout << "doing tcp qx experiments" << std::endl;
        TCPX(settings.scale_factor);
    }
    else
    {
        std::cout << "skipping tcp query x experiments" << std::endl;
    }

    //if (settings.do_triangle) {
    //    std::cout << "doing triangle query" << std::endl;
    //    triangle("twitter_edges_uniq.txt");
    //}
    //else
    //{
    //    std::cout << "skipping small triangle join" << std::endl;
    //}

    std::cout << "done" << std::endl;
    return 0;
}
