#include <iostream>
#include <string>
#include <chrono>
#include <ratio>
#include <cmath>

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
double experiment_timeout = 3600.0 * 24;

struct generic_settings {
    std::string base_directory = ".";

    int scale_factor;
    double experiment_timeout;

    int trials;

    bool do_Q3;
    bool do_QX;
    bool do_QY;
    bool do_QY_timed;
    bool do_QN;
    bool do_triangle;
    bool do_bigtriangle;
    bool do_smallsquare;
    bool do_bigsquare;
    bool do_SkewTri;
    bool do_extra;
    
    bool no_adaptive;
    bool no_DP;
};

generic_settings settings;

void TCP3(int sf=10)
{
    std::cout << "loading data..." << std::endl;

    std::vector<int> sample_count = { 1000, 10000, 100000, 1000000 };
    //std::vector<int> sample_count = { 1000 };

    // load the data
    std::shared_ptr<TableGeneric> table1(new TableGeneric(std::to_string(sf) + "x/customer.tbl", '|', 1, 1));
    std::shared_ptr<TableGeneric> table2(new TableGeneric(std::to_string(sf) + "x/orders.tbl", '|', 2, 1));
    std::shared_ptr<TableGeneric> table3(new TableGeneric(std::to_string(sf) + "x/lineitem_skew.tbl", '|', 1, 1));

    std::cout << "building indexes..." << std::endl;
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
    for (int warmup_size = 1; warmup_size < 3; ++warmup_size)
    {
        std::ofstream output_file;
        output_file.open(std::to_string(sf) + "x_Q3_W" + std::to_string(warmup_size) + "_adaptive.txt");

        for (int trial = 0; trial < settings.trials; ++trial) {
            for (auto samples_to_collect : sample_count)
            {
                std::vector<int> results(3);

                DynamicIndex d_idx(3);

                d_idx.add_Table(0, table1, ((int64_t)5000) * 5000 * 5000);
                d_idx.add_Table(1, table2, ((int64_t)5000) * 5000);
                d_idx.add_Table(2, table3, ((int64_t)5000));

                std::chrono::steady_clock::time_point build_t1 = std::chrono::steady_clock::now();
                d_idx.initialize(1);
                d_idx.warmup();
                std::chrono::steady_clock::time_point build_t2 = std::chrono::steady_clock::now();

                std::chrono::steady_clock::time_point sample_t1 = std::chrono::steady_clock::now();
                
                //int tot_samples = 0;
                for (int good_samples = 0; good_samples < samples_to_collect; )
                {
                    //++tot_samples;
                    bool good = d_idx.sample_join(results);
                    if (good)
                        ++good_samples;
                }

                std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

                output_file << samples_to_collect << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(build_t2 - build_t1).count() << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count()
                    //<< '\t' << tot_samples << '\t' << 1.0 * samples_to_collect / tot_samples
                    << '\n';
            }
        }

        output_file.close();
    }

    std::cout << "DP..." << std::endl;
    // do experiment for DP
    {
        std::ofstream output_file;
        output_file.open(std::to_string(sf) + "x_Q3_jefast.txt");
        for (int trial = 0; trial < settings.trials; ++trial) {
            std::chrono::steady_clock::time_point build_t1 = std::chrono::steady_clock::now();

            JefastBuilder jefast_index_builder;
            jefast_index_builder.AppendTable(table1G, -1, 1, 0);
            jefast_index_builder.AppendTable(table2G, 0, 1, 1);
            jefast_index_builder.AppendTable(table3G, 0, -1, 2);

            auto jefast = jefast_index_builder.Build();

            std::chrono::steady_clock::time_point build_t2 = std::chrono::steady_clock::now();
            for (auto samples_to_collect : sample_count)
            {
                std::vector<jefastKey_t> results;
                std::vector<weight_t> random_values(samples_to_collect);

                std::chrono::steady_clock::time_point sample_t1 = std::chrono::steady_clock::now();

                std::uniform_int_distribution<weight_t> distribution(0, jefast->GetTotal() - 1);

                for (int i = 0; i < samples_to_collect; ++i)
                {
                    jefast->GetJoinNumber(distribution(generator), results);
                }

                //std::generate(random_values.begin(), random_values.end(), [&]() {return distribution(generator);});
                //std::sort(random_values.begin(), random_values.end());

                //for (int i : random_values) {
                //    jefast->GetJoinNumber(i, results);
                //}

                std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

                output_file << samples_to_collect << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(build_t2 - build_t1).count() << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count() << '\n';


                // this is to be used by baseline join
                if (max_fanout.size() == 0)
                    max_fanout = jefast->MaxOutdegree();
            }
        }
        output_file.close();
    }

    std::cout << "baseline..." << std::endl;
    // do experiment for baseline
    {
        std::ofstream output_file;
        output_file.open(std::to_string(sf) + "x_Q3_baseline.txt");
        for (auto i : max_fanout)
            std::cout << i << " ";
        std::cout << std::endl;

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

                    if (Ojoin.sample_join(results))
                    {
                        ++good_samples;
                    }
                }


                std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

                output_file << samples_to_collect << '\t'
                    << 0.0 /* we say build time is nothing */ << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count()
                    //<< '\t' << trials << '\t' << 1.0 * samples_to_collect / trials
                    << std::endl;

                std::cout << trials << ' ' << samples_to_collect << std::endl;
            }
        }
    }

}

void TCP3_reverse(int sf = 10)
{
    std::cout << "loading data..." << std::endl;

    std::vector<int> sample_count = { 1000, 10000, 100000, 1000000 };
    //std::vector<int> sample_count = { 1000 };

    // load the data
    //std::shared_ptr<TableGeneric> table1(new TableGeneric(std::to_string(sf) + "x/customer.tbl", '|', 1, 1));
    //std::shared_ptr<TableGeneric> table2(new TableGeneric(std::to_string(sf) + "x/orders.tbl", '|', 2, 1));
    //std::shared_ptr<TableGeneric> table3(new TableGeneric(std::to_string(sf) + "x/lineitem_skew.tbl", '|', 1, 1));
    std::shared_ptr<TableGeneric> table3(new TableGeneric(std::to_string(sf) + "x/lineitem_skew.tbl", '|', 1, 1));
    std::shared_ptr<TableGeneric> table2(new TableGeneric(std::to_string(sf) + "x/orders.tbl", '|', 1, 2));
    std::shared_ptr<TableGeneric> table1(new TableGeneric(std::to_string(sf) + "x/customer.tbl", '|', 1, 1));

    table1->Build_indexes();
    table2->Build_indexes();
    table3->Build_indexes();

    std::shared_ptr<TableGeneric_encap> table1G(new TableGeneric_encap(table1));
    std::shared_ptr<TableGeneric_encap> table2G(new TableGeneric_encap(table2));
    std::shared_ptr<TableGeneric_encap> table3G(new TableGeneric_encap(table3));

    std::default_random_engine generator;
    generator.seed(std::chrono::steady_clock::now().time_since_epoch().count());

    //std::vector<weight_t> max_fanout;

    std::cout << "baseline..." << std::endl;
    // do experiment for baseline
    {
        std::ofstream output_file;
        output_file.open(std::to_string(sf) + "x_Q3_baseline_reverse.txt");
        //for (auto i : max_fanout)
        //    std::cout << i << " ";
        std::cout << std::flush;

        olkenIndex Ojoin(3);
        Ojoin.add_Table(0, table1, 1.0);
        Ojoin.add_Table(1, table2, 1);
        Ojoin.add_Table(2, table3, 1);
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

                    Ojoin.sample_join(results);
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

void TCPX(int sf=10)
{
    // open tables
    std::vector<int> sample_count = { 1000, 10000, 100000, 1000000 };
    //std::vector<int> sample_count = { 10, 1000 };

    // load the data
    std::shared_ptr<TableGeneric> table1(new TableGeneric(std::to_string(sf) + "x/nation.tbl", '|', 1, 1));
    std::shared_ptr<TableGeneric> table2(new TableGeneric(std::to_string(sf) + "x/supplier.tbl", '|', 1, 4));
    std::shared_ptr<TableGeneric> table3(new TableGeneric(std::to_string(sf) + "x/customer.tbl", '|', 4, 1));
    std::shared_ptr<TableGeneric> table4(new TableGeneric(std::to_string(sf) + "x/orders.tbl", '|', 2, 1));
    std::shared_ptr<TableGeneric> table5(new TableGeneric(std::to_string(sf) + "x/lineitem_skew.tbl", '|', 1, 3));

    //std::shared_ptr<TableGeneric> table1(new TableGeneric(std::to_string(sf) + "x/nation.tbl", '|', 1, 1));
    //std::shared_ptr<TableGeneric> table2(new TableGeneric(std::to_string(sf) + "x/supplier.tbl", '|', 4, 1));
    //std::shared_ptr<TableGeneric> table3(new TableGeneric(std::to_string(sf) + "x/lineitem_skew.tbl", '|', 3, 1));
    //std::shared_ptr<TableGeneric> table4(new TableGeneric(std::to_string(sf) + "x/orders.tbl", '|', 1, 2));
    //std::shared_ptr<TableGeneric> table5(new TableGeneric(std::to_string(sf) + "x/customer.tbl", '|', 1, 4));
    
    table1->Build_indexes();
    table2->Build_indexes();
    table3->Build_indexes();
    table4->Build_indexes();
    table5->Build_indexes();

    std::vector<decltype(table1)> table_list = {table1, table2, table3, table4, table5};

    std::shared_ptr<TableGeneric_encap> table1G(new TableGeneric_encap(table1));
    std::shared_ptr<TableGeneric_encap> table2G(new TableGeneric_encap(table2));
    std::shared_ptr<TableGeneric_encap> table3G(new TableGeneric_encap(table3));
    std::shared_ptr<TableGeneric_encap> table4G(new TableGeneric_encap(table4));
    std::shared_ptr<TableGeneric_encap> table5G(new TableGeneric_encap(table5));

    std::vector<decltype(table1G)> table_list_encap = {table1G, table2G, table3G, table4G, table5G};

    std::default_random_engine generator;
    generator.seed(std::chrono::steady_clock::now().time_since_epoch().count());

    std::vector<weight_t> max_fanout;

    // do experiment for adaptive
    if (!settings.no_adaptive)
    for (int warmup_size = 1; warmup_size < 3; ++warmup_size)
    {
        std::cout << "adaptive " << warmup_size << std::endl;
        std::ofstream output_file;
        output_file.open(std::to_string(sf) + "x_QX_adaptive_W" + std::to_string(warmup_size) + ".txt");
        int orientation = 0;

        for (int trial = 0; trial < settings.trials; ++trial) {
            for (auto samples_to_collect : sample_count)
            {
                std::vector<int> results(5);

                DynamicIndex d_idx(5);

                d_idx.add_Table(0, table_list[(orientation) % 5], (int64_t)1000 * 1000 * 1000 * 1000 * 1000);
                d_idx.add_Table(1, table_list[(orientation + 1) % 5], (int64_t)1000 * 1000 * 1000 * 1000);
                d_idx.add_Table(2, table_list[(orientation + 2) % 5], (int64_t)1000 * 1000 * 1000);
                d_idx.add_Table(3, table_list[(orientation + 3) % 5], (int64_t)1000 * 1000);
                d_idx.add_Table(4, table_list[(orientation + 4) % 5], (int64_t)1000);

                std::chrono::steady_clock::time_point build_t1 = std::chrono::steady_clock::now();
                d_idx.initialize(warmup_size);
                d_idx.warmup();
                std::chrono::steady_clock::time_point build_t2 = std::chrono::steady_clock::now();

                std::chrono::steady_clock::time_point sample_t1 = std::chrono::steady_clock::now();
     //           int tot_samples = 0;
                for (int good_samples = 0; good_samples < samples_to_collect; )
                {
      //              ++tot_samples;
                    bool good = d_idx.sample_join(results);
                    if (good)
                    {
                        ++good_samples;
                    }
                }

                std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

                output_file << samples_to_collect << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(build_t2 - build_t1).count() << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count() 
       //             << '\t' << tot_samples << '\t' << 1.0 * samples_to_collect / tot_samples
                    << std::endl;
            }
        }
        output_file.close();
    }
    
    std::cout << "DP" << std::endl;
    // do experiment for DP
    {
        std::ofstream output_file;
        output_file.open(std::to_string(sf) + "x_QX_jefast.txt");

        for (int trial = 0; trial < settings.trials; ++trial) {
            int orientation = 0;
            std::chrono::steady_clock::time_point build_t1 = std::chrono::steady_clock::now();

            JefastBuilder jefast_index_builder;
            jefast_index_builder.AppendTable(table_list_encap[(orientation) % 5], -1, 1, 0);
            jefast_index_builder.AppendTable(table_list_encap[(orientation + 1) % 5], 0, 1, 1);
            jefast_index_builder.AppendTable(table_list_encap[(orientation + 2) % 5], 0, 1, 2);
            jefast_index_builder.AppendTable(table_list_encap[(orientation + 3) % 5], 0, 1, 3);
            jefast_index_builder.AppendTable(table_list_encap[(orientation + 4) % 5], 0, -1, 4);
            
            auto jefast = jefast_index_builder.Build();

            std::chrono::steady_clock::time_point build_t2 = std::chrono::steady_clock::now();
            for (auto samples_to_collect : sample_count)
            {
                std::vector<jefastKey_t> results;
                std::vector<weight_t> random_values(samples_to_collect);

                std::chrono::steady_clock::time_point sample_t1 = std::chrono::steady_clock::now();

                std::uniform_int_distribution<weight_t> distribution(0, jefast->GetTotal() - 1);

                //std::generate(random_values.begin(), random_values.end(), [&]() {return distribution(generator);});
                //std::sort(random_values.begin(), random_values.end());

                //for (int i : random_values) {
                //    jefast->GetJoinNumber(i, results);
                //}
                
                int good_samples = 0;
                for (; good_samples < samples_to_collect;)
                {
                    jefast->GetJoinNumber(distribution(generator), results);
                    ++good_samples;
                }

                std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

                output_file << samples_to_collect << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(build_t2 - build_t1).count() << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count() << std::endl;


                // this is to be used by baseline join
                if (max_fanout.size() == 0)
                    max_fanout = jefast->MaxOutdegree();
            }
        }

        output_file.close();
    }

    // do experiment for baseline
    {
        std::ofstream output_file;
        output_file.open(std::to_string(sf) + "x_QX_baseline.txt");

        for (auto i : max_fanout)
            std::cout << i << " ";
        std::cout << std::endl;

        int orientation = 0;

        olkenIndex Ojoin(5);
        Ojoin.add_Table(0, table_list[(orientation) % 5], 1.0);
        Ojoin.add_Table(1, table_list[(orientation + 1) % 5], max_fanout[0]);
        Ojoin.add_Table(2, table_list[(orientation + 2) % 5], max_fanout[1]);
        Ojoin.add_Table(3, table_list[(orientation + 3) % 5], max_fanout[2]);
        Ojoin.add_Table(4, table_list[(orientation + 4) % 5], max_fanout[3]);
        
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

                    if (Ojoin.sample_join(results))
                    {
                            ++good_samples;
                    }
                }


                std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

                output_file << samples_to_collect << '\t'
                    << 0.0 /* we say build time is nothing */ << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count()
        //            << '\t' << trials << '\t' << 1.0 * samples_to_collect / trials
                    << std::endl;

                std::cout << trials << ' ' << samples_to_collect << std::endl;
            }
        }
    }
}

void TCPY_timed(int sf = 10)
{
    // open tables
    //std::vector<int> sample_count = { 1000, 10000, 100000, 1000000 };
    //std::vector<int> sample_count = { 1 };
    int total_sample_count = 10000000;
    int print_time_every = 100000;

    // load the data
    std::shared_ptr<TableGeneric> table1(new TableGeneric(std::to_string(sf) + "x/lineitem_skew.tbl", '|', 2, 1));
    std::shared_ptr<TableGeneric> table2(new TableGeneric(std::to_string(sf) + "x/orders.tbl", '|', 1, 2));
    std::shared_ptr<TableGeneric> table3(new TableGeneric(std::to_string(sf) + "x/customer.tbl", '|', 1, 4));
    std::shared_ptr<TableGeneric> table4(new TableGeneric(std::to_string(sf) + "x/supplier.tbl", '|', 4, 4));
    std::shared_ptr<TableGeneric> table5(new TableGeneric(std::to_string(sf) + "x/customer.tbl", '|', 4, 1));
    std::shared_ptr<TableGeneric> table6(new TableGeneric(std::to_string(sf) + "x/orders.tbl", '|', 2, 1));
    std::shared_ptr<TableGeneric> table7(new TableGeneric(std::to_string(sf) + "x/lineitem_skew.tbl", '|', 1, 2));

    std::vector<std::shared_ptr<TableGeneric> > table_list;
    table_list.push_back(table1);
    table_list.push_back(table2);
    table_list.push_back(table3);
    table_list.push_back(table4);
    table_list.push_back(table5);
    table_list.push_back(table6);
    table_list.push_back(table7);


    //std::shared_ptr<TableGeneric> table1(new TableGeneric(std::to_string(sf) + "x/nation.tbl", '|', 3, 1));
    //std::shared_ptr<TableGeneric> table2(new TableGeneric(std::to_string(sf) + "x/supplier.tbl", '|', 4, 1));
    //std::shared_ptr<TableGeneric> table3(new TableGeneric(std::to_string(sf) + "x/partsupp.tbl", '|', 2, 1));
    //std::shared_ptr<TableGeneric> table4(new TableGeneric(std::to_string(sf) + "x/lineitem_skew.tbl", '|', 2, 1));
    //std::shared_ptr<TableGeneric> table5(new TableGeneric(std::to_string(sf) + "x/orders.tbl", '|', 1, 2));
    //std::shared_ptr<TableGeneric> table6(new TableGeneric(std::to_string(sf) + "x/customer.tbl", '|', 1, 4));
    //std::shared_ptr<TableGeneric> table7(new TableGeneric(std::to_string(sf) + "x/nation.tbl", '|', 1, 3));


    table1->Build_indexes();
    table2->Build_indexes();
    table3->Build_indexes();
    table4->Build_indexes();
    table5->Build_indexes();
    table6->Build_indexes();
    table7->Build_indexes();

    std::vector<std::shared_ptr<TableGeneric_encap> > table_list_encap;
    table_list_encap.push_back(std::shared_ptr<TableGeneric_encap>(new TableGeneric_encap(table1)));
    table_list_encap.push_back(std::shared_ptr<TableGeneric_encap>(new TableGeneric_encap(table2)));
    table_list_encap.push_back(std::shared_ptr<TableGeneric_encap>(new TableGeneric_encap(table3)));
    table_list_encap.push_back(std::shared_ptr<TableGeneric_encap>(new TableGeneric_encap(table4)));
    table_list_encap.push_back(std::shared_ptr<TableGeneric_encap>(new TableGeneric_encap(table5)));
    table_list_encap.push_back(std::shared_ptr<TableGeneric_encap>(new TableGeneric_encap(table6)));
    table_list_encap.push_back(std::shared_ptr<TableGeneric_encap>(new TableGeneric_encap(table7)));

    //std::shared_ptr<TableGeneric_encap> table1G(new TableGeneric_encap(table1));
    //std::shared_ptr<TableGeneric_encap> table2G(new TableGeneric_encap(table2));
    //std::shared_ptr<TableGeneric_encap> table3G(new TableGeneric_encap(table3));
    //std::shared_ptr<TableGeneric_encap> table4G(new TableGeneric_encap(table4));
    //std::shared_ptr<TableGeneric_encap> table5G(new TableGeneric_encap(table5));
    //std::shared_ptr<TableGeneric_encap> table6G(new TableGeneric_encap(table6));
    //std::shared_ptr<TableGeneric_encap> table7G(new TableGeneric_encap(table7));

    std::default_random_engine generator;
    generator.seed(std::chrono::steady_clock::now().time_since_epoch().count());

    std::vector<weight_t> max_fanout;

    //{    
    //    std::ofstream output_file;
    //    output_file.open(std::to_string(sf) + "x_QY_orientation_sizes_full.txt");

    //    for (int orientation = 0; orientation <= 7; ++orientation)
    //    {
    //        std::vector<int> results(6);

    //        DynamicIndex d_idx(7);

    //        d_idx.add_Table(0, table_list[orientation % 7], (int64_t)10 * 10 * 10 * 10 * 10 * 10 * 10);
    //        d_idx.add_Table(1, table_list[(orientation + 1) % 7], (int64_t)10 * 10 * 10 * 10 * 10 * 10);
    //        d_idx.add_Table(2, table_list[(orientation + 2) % 7], (int64_t)10 * 10 * 10 * 10 * 10);
    //        d_idx.add_Table(3, table_list[(orientation + 3) % 7], (int64_t)10 * 10 * 10 * 10);
    //        d_idx.add_Table(4, table_list[(orientation + 4) % 7], (int64_t)10 * 10 * 10);
    //        d_idx.add_Table(5, table_list[(orientation + 5) % 7], (int64_t)10 * 10);
    //        d_idx.add_Table(6, table_list[(orientation + 6) % 7], (int64_t)10);


    //        d_idx.initialize(1);
    //        d_idx.warmup();

    //        std::cout << "orientation " << orientation << " size est: " << d_idx.size_est() << std::endl;
    //        output_file << "orientation " << orientation << " size est: " << d_idx.size_est() << std::endl;
    //    }
    //}

    // do experiment for adaptive
    std::cout << "starting adaptive" << std::endl;
    if(!settings.no_adaptive)
    {
        //int orientation = 3;
        for (int orientation : {3}) {
            for (int warmup_size = 1; warmup_size < 2; ++warmup_size)
            {
                std::ofstream output_file;
                //output_file.open(std::to_string(sf) + "x_W" + std::to_string(warmup_size) + "_O" + std::to_string(orientation) + "_QY_adaptive_timed.txt");

                //for (int trial = 0; trial < settings.trials; ++trial) 
                {
                    //for (auto samples_to_collect : sample_count)
                    {
                        std::vector<int> results(7);

                        DynamicIndex d_idx(6);

                        d_idx.add_Table(0, table_list[orientation % 7], (int64_t)10 * 10 * 10 * 10 * 10 * 10 * 10);
                        d_idx.add_Table(1, table_list[(orientation + 1) % 7], (int64_t)10 * 10 * 10 * 10 * 10 * 10);
                        d_idx.add_Table(2, table_list[(orientation + 2) % 7], (int64_t)10 * 10 * 10 * 10 * 10);
                        d_idx.add_Table(3, table_list[(orientation + 3) % 7], (int64_t)10 * 10 * 10 * 10);
                        d_idx.add_Table(4, table_list[(orientation + 4) % 7], (int64_t)10 * 10 * 10);
                        d_idx.add_Table(5, table_list[(orientation + 5) % 7], (int64_t)10 * 10);
                        //d_idx.add_Table(6, table_list[(orientation + 6) % 7], (int64_t)10);

                        auto s_table = table_list[orientation % 7];
                        auto e_table = table_list[(orientation + 5) % 7];
                        auto check_table = table_list[(orientation + 6) % 7];
                        //d_idx.add_Table(6, table7, (int64_t)10);

                        std::chrono::steady_clock::time_point build_t1 = std::chrono::steady_clock::now();
                        d_idx.initialize(warmup_size);
                        d_idx.warmup();
                        std::chrono::steady_clock::time_point build_t2 = std::chrono::steady_clock::now();
                        //std::cout << "warm up complete" << std::endl;
                        std::cout << "warm up time: "<< '\t'
                            << std::chrono::duration_cast<std::chrono::duration<double>>(build_t2 - build_t1).count() << std::endl;

                        std::chrono::steady_clock::time_point sample_t1 = std::chrono::steady_clock::now();

                        int trials = 0;
                        int good_samples = 0;
                        int good_samples_mod = 0;
                        for (;; ++trials)
                        {
                            bool good = d_idx.sample_join(results);
                            if (good
                                //&& s_table->get_column1_value(results[0]) == e_table->get_column2_value(results[6])
                                && check_table->get_index_of_values(e_table->get_column2_value(results[5]), s_table->get_column1_value(results[0])) != -1
                                ) {
                                if (++good_samples_mod == print_time_every) {
                                    good_samples += good_samples_mod;
                                    good_samples_mod = 0;
                                    std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();
                                    std::cout << trials << ' ' << good_samples << ' ' << 
                                    std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count() << std::endl;
                                    if (good_samples >= total_sample_count) {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                //output_file.close();
            }
        }
    }
    else {
        std::cout << "skipping QY adaptive" << std::endl;
    }
}

void TCPY(int sf = 10)
{
    // open tables
    std::vector<int> sample_count = { 1000, 10000, 100000, 1000000 };
    //std::vector<int> sample_count = { 1 };

    // load the data
    std::shared_ptr<TableGeneric> table1(new TableGeneric(std::to_string(sf) + "x/lineitem_skew.tbl", '|', 2, 1));
    std::shared_ptr<TableGeneric> table2(new TableGeneric(std::to_string(sf) + "x/orders.tbl", '|', 1, 2));
    std::shared_ptr<TableGeneric> table3(new TableGeneric(std::to_string(sf) + "x/customer.tbl", '|', 1, 4));
    std::shared_ptr<TableGeneric> table4(new TableGeneric(std::to_string(sf) + "x/supplier.tbl", '|', 4, 4));
    std::shared_ptr<TableGeneric> table5(new TableGeneric(std::to_string(sf) + "x/customer.tbl", '|', 4, 1));
    std::shared_ptr<TableGeneric> table6(new TableGeneric(std::to_string(sf) + "x/orders.tbl", '|', 2, 1));
    std::shared_ptr<TableGeneric> table7(new TableGeneric(std::to_string(sf) + "x/lineitem_skew.tbl", '|', 1, 2));

    std::vector<std::shared_ptr<TableGeneric> > table_list;
    table_list.push_back(table1);
    table_list.push_back(table2);
    table_list.push_back(table3);
    table_list.push_back(table4);
    table_list.push_back(table5);
    table_list.push_back(table6);
    table_list.push_back(table7);


    //std::shared_ptr<TableGeneric> table1(new TableGeneric(std::to_string(sf) + "x/nation.tbl", '|', 3, 1));
    //std::shared_ptr<TableGeneric> table2(new TableGeneric(std::to_string(sf) + "x/supplier.tbl", '|', 4, 1));
    //std::shared_ptr<TableGeneric> table3(new TableGeneric(std::to_string(sf) + "x/partsupp.tbl", '|', 2, 1));
    //std::shared_ptr<TableGeneric> table4(new TableGeneric(std::to_string(sf) + "x/lineitem_skew.tbl", '|', 2, 1));
    //std::shared_ptr<TableGeneric> table5(new TableGeneric(std::to_string(sf) + "x/orders.tbl", '|', 1, 2));
    //std::shared_ptr<TableGeneric> table6(new TableGeneric(std::to_string(sf) + "x/customer.tbl", '|', 1, 4));
    //std::shared_ptr<TableGeneric> table7(new TableGeneric(std::to_string(sf) + "x/nation.tbl", '|', 1, 3));


    table1->Build_indexes();
    table2->Build_indexes();
    table3->Build_indexes();
    table4->Build_indexes();
    table5->Build_indexes();
    table6->Build_indexes();
    table7->Build_indexes();

    std::vector<std::shared_ptr<TableGeneric_encap> > table_list_encap;
    table_list_encap.push_back(std::shared_ptr<TableGeneric_encap>(new TableGeneric_encap(table1)));
    table_list_encap.push_back(std::shared_ptr<TableGeneric_encap>(new TableGeneric_encap(table2)));
    table_list_encap.push_back(std::shared_ptr<TableGeneric_encap>(new TableGeneric_encap(table3)));
    table_list_encap.push_back(std::shared_ptr<TableGeneric_encap>(new TableGeneric_encap(table4)));
    table_list_encap.push_back(std::shared_ptr<TableGeneric_encap>(new TableGeneric_encap(table5)));
    table_list_encap.push_back(std::shared_ptr<TableGeneric_encap>(new TableGeneric_encap(table6)));
    table_list_encap.push_back(std::shared_ptr<TableGeneric_encap>(new TableGeneric_encap(table7)));

    //std::shared_ptr<TableGeneric_encap> table1G(new TableGeneric_encap(table1));
    //std::shared_ptr<TableGeneric_encap> table2G(new TableGeneric_encap(table2));
    //std::shared_ptr<TableGeneric_encap> table3G(new TableGeneric_encap(table3));
    //std::shared_ptr<TableGeneric_encap> table4G(new TableGeneric_encap(table4));
    //std::shared_ptr<TableGeneric_encap> table5G(new TableGeneric_encap(table5));
    //std::shared_ptr<TableGeneric_encap> table6G(new TableGeneric_encap(table6));
    //std::shared_ptr<TableGeneric_encap> table7G(new TableGeneric_encap(table7));

    std::default_random_engine generator;
    generator.seed(std::chrono::steady_clock::now().time_since_epoch().count());

    std::vector<weight_t> max_fanout;

    //{    
    //    std::ofstream output_file;
    //    output_file.open(std::to_string(sf) + "x_QY_orientation_sizes_full.txt");

    //    for (int orientation = 0; orientation <= 7; ++orientation)
    //    {
    //        std::vector<int> results(6);

    //        DynamicIndex d_idx(7);

    //        d_idx.add_Table(0, table_list[orientation % 7], (int64_t)10 * 10 * 10 * 10 * 10 * 10 * 10);
    //        d_idx.add_Table(1, table_list[(orientation + 1) % 7], (int64_t)10 * 10 * 10 * 10 * 10 * 10);
    //        d_idx.add_Table(2, table_list[(orientation + 2) % 7], (int64_t)10 * 10 * 10 * 10 * 10);
    //        d_idx.add_Table(3, table_list[(orientation + 3) % 7], (int64_t)10 * 10 * 10 * 10);
    //        d_idx.add_Table(4, table_list[(orientation + 4) % 7], (int64_t)10 * 10 * 10);
    //        d_idx.add_Table(5, table_list[(orientation + 5) % 7], (int64_t)10 * 10);
    //        d_idx.add_Table(6, table_list[(orientation + 6) % 7], (int64_t)10);


    //        d_idx.initialize(1);
    //        d_idx.warmup();

    //        std::cout << "orientation " << orientation << " size est: " << d_idx.size_est() << std::endl;
    //        output_file << "orientation " << orientation << " size est: " << d_idx.size_est() << std::endl;
    //    }
    //}

    // do experiment for adaptive
    std::cout << "starting adaptive" << std::endl;
    if(!settings.no_adaptive)
    {
        //int orientation = 3;
        for (int orientation : {3}) {
            for (int warmup_size = 1; warmup_size < 2; ++warmup_size)
            {
                std::ofstream output_file;
                output_file.open(std::to_string(sf) + "x_W" + std::to_string(warmup_size) + "_O" + std::to_string(orientation) + "_QY_adaptive.txt");

                for (int trial = 0; trial < settings.trials; ++trial) {
                    for (auto samples_to_collect : sample_count)
                    {
                        std::vector<int> results(7);

                        DynamicIndex d_idx(6);

                        d_idx.add_Table(0, table_list[orientation % 7], (int64_t)10 * 10 * 10 * 10 * 10 * 10 * 10);
                        d_idx.add_Table(1, table_list[(orientation + 1) % 7], (int64_t)10 * 10 * 10 * 10 * 10 * 10);
                        d_idx.add_Table(2, table_list[(orientation + 2) % 7], (int64_t)10 * 10 * 10 * 10 * 10);
                        d_idx.add_Table(3, table_list[(orientation + 3) % 7], (int64_t)10 * 10 * 10 * 10);
                        d_idx.add_Table(4, table_list[(orientation + 4) % 7], (int64_t)10 * 10 * 10);
                        d_idx.add_Table(5, table_list[(orientation + 5) % 7], (int64_t)10 * 10);
                        //d_idx.add_Table(6, table_list[(orientation + 6) % 7], (int64_t)10);

                        auto s_table = table_list[orientation % 7];
                        auto e_table = table_list[(orientation + 5) % 7];
                        auto check_table = table_list[(orientation + 6) % 7];
                        //d_idx.add_Table(6, table7, (int64_t)10);

                        std::chrono::steady_clock::time_point build_t1 = std::chrono::steady_clock::now();
                        d_idx.initialize(warmup_size);
                        d_idx.warmup();
                        std::chrono::steady_clock::time_point build_t2 = std::chrono::steady_clock::now();
                        std::cout << "warm up complete" << std::endl;

                        std::chrono::steady_clock::time_point sample_t1 = std::chrono::steady_clock::now();

                        int trials = 0;
                        for (int good_samples = 0; good_samples < samples_to_collect; ++trials)
                        {
                            bool good = d_idx.sample_join(results);
                            if (good
                                //&& s_table->get_column1_value(results[0]) == e_table->get_column2_value(results[6])
                                && check_table->get_index_of_values(e_table->get_column2_value(results[5]), s_table->get_column1_value(results[0])) != -1
                                )
                                ++good_samples;
                        }

                        std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

                        //std::cout << "size est for query y: " << d_idx.size_est() << std::endl;
                        std::cout << "took " << trials << " trials to get " << samples_to_collect << " samples" << std::endl;


                        output_file << samples_to_collect << '\t'
                            << std::chrono::duration_cast<std::chrono::duration<double>>(build_t2 - build_t1).count() << '\t'
                            << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count() 
                        //    << '\t' << trials << '\t' << 1.0 * samples_to_collect / trials 
                            << std::endl;
                    }
                }
                output_file.close();
            }
        }
    }
    else {
        std::cout << "skipping QY adaptive" << std::endl;
    }

    // do experiment for DP
    std::cout << "starting DP" << std::endl;
    if(!settings.no_DP)
    {
        int orientation = 3;

        std::ofstream output_file;
        output_file.open(std::to_string(sf) + "x_QY_jefast.txt");

        for (int trial = 0; trial < settings.trials; ++trial) {
            std::chrono::steady_clock::time_point build_t1 = std::chrono::steady_clock::now();
            JefastBuilder jefast_index_builder;
            jefast_index_builder.AppendTable(table_list_encap[(orientation) % 7], -1, 1, 0);
            jefast_index_builder.AppendTable(table_list_encap[(orientation + 1) % 7], 0, 1, 1);
            jefast_index_builder.AppendTable(table_list_encap[(orientation + 2) % 7], 0, 1, 2);
            jefast_index_builder.AppendTable(table_list_encap[(orientation + 3) % 7], 0, 1, 3);
            jefast_index_builder.AppendTable(table_list_encap[(orientation + 4) % 7], 0, 1, 4);
            jefast_index_builder.AppendTable(table_list_encap[(orientation + 5) % 7], 0, -1, 5);
            //jefast_index_builder.AppendTable(table_list_encap[(orientation + 6) % 7], 0, -1, 6);

            auto s_table = table_list[orientation % 7];
            auto e_table = table_list[(orientation + 5) % 7];
            auto check_table = table_list[(orientation + 6) % 7];

            auto jefast = jefast_index_builder.Build();

            std::chrono::steady_clock::time_point build_t2 = std::chrono::steady_clock::now();

            std::cout << "finished building" << std::endl;
            for (auto samples_to_collect : sample_count )
            {
                std::chrono::steady_clock::time_point sample_t1 = std::chrono::steady_clock::now();
                std::vector<jefastKey_t> results;
                std::vector<weight_t> random_values(samples_to_collect);



                std::uniform_int_distribution<weight_t> distribution(0, jefast->GetTotal() - 1);

                //std::generate(random_values.begin(), random_values.end(), [&]() {return distribution(generator);});
                //std::sort(random_values.begin(), random_values.end());

                //for (int i : random_values) {
                //    jefast->GetJoinNumber(i, results);
                //}
                int64_t good_samples{ 0 };
                int trials = 0;
                for (; good_samples < samples_to_collect; ++trials)
                {
                    jefast->GetJoinNumber(distribution(generator), results);
                    if (check_table->get_index_of_values(e_table->get_column2_value(results[5]), s_table->get_column1_value(results[0])) != -1)
                    //if(s_table->get_column1_value(results[0]) == e_table->get_column2_value(results[6]))
                    {
                        ++good_samples;
                    }
                }

                std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

                std::cout << "reporting" << std::endl;

                output_file << samples_to_collect << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(build_t2 - build_t1).count() << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count()
                    //<< '\t' << trials << '\t' << 1.0 * samples_to_collect / trials
                    << std::endl;


                // this is to be used by baseline join
                if (max_fanout.size() == 0)
                    max_fanout = jefast->MaxOutdegree();
            }
        }

        output_file.close();
    }
        else {
            std::cout << "skipped adaptive in QY" << std::endl;
        }

    // do experiment for baseline
    std::cout << "starting baseline" << std::endl;
    if (true) {
        std::ofstream output_file;
        output_file.open(std::to_string(sf) + "x_QY_baseline.txt");

        for (auto i : max_fanout)
            std::cout << i << " ";
        std::cout << std::flush;

        int orientation = 3;

        olkenIndex Ojoin(6);
        Ojoin.add_Table(0, table_list[(orientation) % 7], 1.0);
        Ojoin.add_Table(1, table_list[(orientation + 1) % 7], max_fanout[0]);
        Ojoin.add_Table(2, table_list[(orientation + 2) % 7], max_fanout[1]);
        Ojoin.add_Table(3, table_list[(orientation + 3) % 7], max_fanout[2]);
        Ojoin.add_Table(4, table_list[(orientation + 4) % 7], max_fanout[3]);
        Ojoin.add_Table(5, table_list[(orientation + 5) % 7], max_fanout[4]);
        //Ojoin.add_Table(6, table_list[(orientation + 6) % 7], max_fanout[5]);

        auto s_table = table_list[orientation % 7];
        auto e_table = table_list[(orientation + 5) % 7];
        auto check_table = table_list[(orientation + 6) % 7];

        for (int trial = 0; trial < settings.trials; ++trial) {
            for (auto samples_to_collect : {1000, 10000, 100000} )
            {
                std::vector<jefastKey_t> results;

                std::chrono::steady_clock::time_point sample_t1 = std::chrono::steady_clock::now();

                int trials = 0;

                for (int good_samples = 0; good_samples < samples_to_collect; ++trials)
                {
                    if (std::chrono::duration_cast<std::chrono::duration<double>>(std::chrono::steady_clock::now() - sample_t1).count() > experiment_timeout)
                        break;

                    if (Ojoin.sample_join(results)
                        && (check_table->get_index_of_values(e_table->get_column2_value(results[5]), s_table->get_column1_value(results[0])) != -1)
                        )
                    {
                        ++good_samples;
                    }
                }


                std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

                output_file << samples_to_collect << '\t'
                    << 0.0 /* we say build time is nothing */ << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count()
                    //<< '\t' << trials << '\t' << 1.0 * samples_to_collect / trials
                    << std::endl;

                std::cout << trials << ' ' << samples_to_collect << " ratio: " << ((double) trials / samples_to_collect) << std::endl;
            }
        }
    }
}

void TPCH_QN(int sf = 10) {
/*
 * SELECT *
 * FROM nation, supplier, lineitem l1, orders o1, customer, orders o2, lineitem l2
 * WHERE s_nationkey = n_nationkey
 * AND s_suppkey = l1.l_suppkey
 * AND l1.l_orderkey = o1.o_orderkey
 * AND o1.o_custkey = c_custkey
 * AND c_custkey = o2.o_custkey
 * AND o2.o_orderkey = l2.l_orderkey;
 */

    //std::vector<int> sample_count = { 10000, 20000, 50000, 70000, 100000, 150000, 200000 };
    std::vector<int> sample_count = { 1000000 };
    
    std::cout << "Loading tables..." << std::endl;
    typedef TableGeneric nation_type;
    std::shared_ptr<nation_type> nation(
        new nation_type(
            std::to_string(sf) + "x/nation.tbl", '|', 3, 1
        )); /* n_regionkey, n_nationkey */

    typedef TableGenericImpl<ColumnExtractor<double, Colno<6>>> supplier_type;
    std::shared_ptr<supplier_type> supplier(
        new supplier_type(
            std::to_string(sf) + "x/supplier.tbl", '|', 4, 1
        )); /* s_nationkey, s_suppkey, s_acctbal */

    typedef TableGenericImpl<ColumnExtractor<
            int, Colno<5>,
            double, Colno<6>,
            double, Colno<7>,
            std::string, Colno<9>,
            datetime_t, Colno<11>
            >> lineitem_type;
    std::shared_ptr<lineitem_type> lineitem1(
        new lineitem_type(
            std::to_string(sf) + "x/lineitem_skew.tbl", '|', 3, 1
        ));
    /* l_suppkey, l_orderkey, l_quantity, l_extendedprice, l_discount, l_returnflag*/
    
    typedef TableGenericImpl<ColumnExtractor<
            std::string, Colno<3>,
            double, Colno<4>,
            std::string, Colno<6>>> orders_type;
    std::shared_ptr<orders_type> orders1(
        new orders_type(
            std::to_string(sf) + "x/orders.tbl", '|', 1, 2
        )); /* o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderpriority */
    
    //std::cout << "customer " << std::endl;
    typedef TableGenericImpl<ColumnExtractor<
            double, Colno<6>,
            std::string, Colno<7>>> customer_type;
    std::shared_ptr<customer_type> customer(
        new customer_type(
            std::to_string(sf) + "x/customer.tbl", '|', 1, 1
        )); /* c_custkey, c_acctbal, c_mktsegment */
    
    std::shared_ptr<orders_type> orders2(orders1->reverse_columns12());

    std::shared_ptr<lineitem_type> lineitem2(lineitem1->reverse_columns12());

    std::vector<std::shared_ptr<TableGenericBase>> table_list({
            std::static_pointer_cast<TableGenericBase>(nation),
            std::static_pointer_cast<TableGenericBase>(supplier),
            std::static_pointer_cast<TableGenericBase>(lineitem1),
            std::static_pointer_cast<TableGenericBase>(orders1),
            std::static_pointer_cast<TableGenericBase>(customer)
        });
    std::cout << "Building indexes..." << std::endl;
    std::for_each(table_list.begin(), table_list.end(), [](auto p) {
        p->Build_indexes();
    });
    table_list.push_back(orders2); orders2->Build_indexes(true);
    table_list.push_back(lineitem2); lineitem2->Build_indexes(true);

    std::vector<std::shared_ptr<TableGeneric_encap>> table_list_encap;
    std::transform(table_list.begin(), table_list.end(),
            std::back_inserter(table_list_encap), [](auto p) {
        return std::shared_ptr<TableGeneric_encap>(new TableGeneric_encap(p));  
    });

    std::default_random_engine generator;
    generator.seed(std::chrono::steady_clock::now().time_since_epoch().count());

    std::vector<weight_t> max_fanout;

    auto write_tuple = [&](
            std::ofstream &out,
            int orientation,
            const std::vector<int>& results) {
        
        int i = (table_list.size() - orientation) % table_list.size();
        int nation_idx = results[i];
        int supplier_idx = results[(i + 1) % table_list.size()];
        int lineitem1_idx = results[(i + 2) % table_list.size()];
        int orders1_idx = results[(i + 3) % table_list.size()];
        int customer_idx = results[(i + 4) % table_list.size()];
        int orders2_idx = results[(i + 5) % table_list.size()];
        int lineitem2_idx = results[(i + 6) % table_list.size()];

        /* label: l1.l_returnflag */ 
        if (std::get<3>(lineitem1->get_auxcols(lineitem1_idx)) == "R") {
            out << 1;
        } else {
            out << 0;
        }

        /* 1-5:n_regionkey */
        out << ' ' << 1 + nation->get_column1_value(nation_idx) << ":1";

        /* 6:s_acctbal */
        {
            auto auxcols = supplier->get_auxcols(supplier_idx);
            auto value = std::get<0>(auxcols);
            out << " 6:" << value;
            //out << " 6:" << std::get<0>(supplier->get_auxcols(supplier_idx));
        }

        /* 7:l1.l_quantity 8:l1.l_extendedprice * (1 - l1.l_discount) */
        {
            auto &auxcols = lineitem1->get_auxcols(lineitem1_idx);
            out << " 7:" << std::get<0>(auxcols)
                << " 8:" << std::get<1>(auxcols) * (1.0 - std::get<2>(auxcols));
        }
        
        /* 9: l1.l_shipdate - l2.l_shipdate */
        {
            auto &auxcols1 = lineitem1->get_auxcols(lineitem1_idx);
            auto &auxcols2 = lineitem2->get_auxcols(lineitem2_idx);
            out << " 9:" << std::get<4>(auxcols1) - std::get<4>(auxcols2);
        }

        /* 10-12:o1.o_orderstatus 13:o1.o_totalprice 14:o1.o_orderpriority */
        {
            auto &auxcols = orders1->get_auxcols(orders1_idx);
            out << ' ';
           /* if (std::get<0>(auxcols) == "F") {
                out << 10;
            } else if (std::get<0>(auxcols) == "O") {
                out << 11;
            } else {
                out << 12;
            } out << ":1"; */
            out << " 13:" << std::get<1>(auxcols)
                << " 14:" << std::get<2>(auxcols)[0] - '0';
        }

        /* 15:c_acctbal 16-20:mktsegment */
        /*{
            auto &auxcols = customer->get_auxcols(customer_idx);
            out << " 15:" << std::get<0>(auxcols) << ' ';
            auto &mktsegment = std::get<1>(auxcols);
            switch (mktsegment[0]) {
            case 'A': out << 16; break;
            case 'B': out << 17; break;
            case 'F': out << 18; break;
            case 'H': out << 19; break;
            case 'M': out << 20; break;
            }
            out << ":1";
        } */
        
        /* 21-23:o2.o_orderstatus 24:o2.o_totalprice 25:o2.o_orderpriority */
        /*{
            auto &auxcols = orders2->get_auxcols(orders2_idx);
            out << ' ';
            if (std::get<0>(auxcols) == "F") {
                out << 21;
            } else if (std::get<0>(auxcols) == "O") {
                out << 22;
            } else {
                out << 23;
            } out << ":1";
            out << " 24:" << std::get<1>(auxcols)
                << " 25:" << std::get<2>(auxcols)[0] - '0';
        } */

        /* 26:l2.l_quantity 27:l2.l_extendedprice * (1 - l2.l_discount) 28:l2.l_returnflag */
        {
            auto &auxcols = lineitem2->get_auxcols(lineitem2_idx);
            out << " 26:" << std::get<0>(auxcols)
                << " 27:" << std::get<1>(auxcols) * (1 - std::get<2>(auxcols))
                << " 28:" << (std::get<3>(auxcols) == "R" ? 1 : 0);
        }

        out << std::endl;
    };

    std::cout << "starting adaptive" << std::endl;
    if (!settings.no_adaptive) {
        std::vector<int> results(table_list.size());
        for (int orientation: {0}) {
            for (int warmup_size = 1; warmup_size < 2; ++warmup_size) {
                std::ofstream output_file;
                output_file.open(std::to_string(sf) + "x_W" + std::to_string(warmup_size)
                    + "_O" + std::to_string(orientation) + "_QN_adaptive.txt", std::ios_base::app);
                for (int trial = 0; trial < settings.trials; ++trial) {
                    for (auto samples_to_collect: sample_count) {

                        std::ofstream tuple_file;
                        tuple_file.open(std::to_string(sf) + "x_W"
                            + std::to_string(warmup_size)
                            + "_O" + std::to_string(orientation)
                            + "_T" + std::to_string(trial)
                            + "_S" + std::to_string(samples_to_collect)
                            + "_QN_adaptive_tuples.txt");
                        DynamicIndex d_idx(table_list.size());
                        
                        int64_t max_agm = 1000;
                        for (int i = table_list.size() - 1; i >= 0; --i) {
                            d_idx.add_Table(i, table_list[(i + orientation) % table_list.size()], max_agm);
                            max_agm *= 10;
                        }
                        auto build_t1 = std::chrono::steady_clock::now();
                        d_idx.initialize(warmup_size);
                        d_idx.warmup();
                        auto build_t2 = std::chrono::steady_clock::now();

                        auto sample_t1 = std::chrono::steady_clock::now();
                        for (int good_samples = 0; good_samples < samples_to_collect; ) {
                            bool good = d_idx.sample_join(results);
                            if (good) {
                                ++good_samples;
                                write_tuple(tuple_file, orientation, results);
                            }
                        }
                        auto sample_t2 = std::chrono::steady_clock::now();

                        output_file << samples_to_collect << '\t'
                            << std::chrono::duration_cast<std::chrono::duration<double>>(build_t2 - build_t1).count() << '\t'
                            << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count() << std::endl;

                        tuple_file.close();
                    }
                }
                output_file.close(); 
            }
        }
    }
    
    std::cout << "DP" << std::endl;
    {
        std::ofstream output_file;
        output_file.open(std::to_string(sf) + "x_QN_jefast.txt", std::ios_base::app);

        std::vector<jefastKey_t> results;
        std::vector<int> results_i(table_list.size());
        for (int trial = 0; trial < settings.trials; ++trial) {
            int orientation = 0;

            auto build_t1 = std::chrono::steady_clock::now();
            JefastBuilder jefast_index_builder;
            for (auto i = 0; i != table_list_encap.size(); ++i) {
                jefast_index_builder.AppendTable(
                    table_list_encap[(orientation + i) % table_list_encap.size()],
                    (i == 0) ? -1 : 0,
                    (i + 1 == table_list_encap.size()) ? -1 : 1,
                    i);
            }
            auto jefast = jefast_index_builder.Build();
            auto build_t2 = std::chrono::steady_clock::now();

            for (auto samples_to_collect : sample_count) {
                std::ofstream tuple_file;
                tuple_file.open(std::to_string(sf)
                    + "x_T" + std::to_string(trial) +
                    + "x_S" + std::to_string(samples_to_collect)
                    + "_QN_jefast.txt");


                auto sample_t1 = std::chrono::steady_clock::now();
                std::uniform_int_distribution<weight_t> distribution(0, jefast->GetTotal() - 1);
                int good_samples = 0;
                for (; good_samples < samples_to_collect;) {
                    auto join_number = distribution(generator);
                    jefast->GetJoinNumber(join_number, results); //distribution(generator), results);
                    ++good_samples;
                    std::copy(results.begin(), results.end(), results_i.begin());
                    write_tuple(tuple_file, orientation, results_i);
                }
                auto sample_t2 = std::chrono::steady_clock::now();

                output_file << samples_to_collect << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(build_t2 - build_t1).count() << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count() << std::endl;

                if (max_fanout.size() == 0) {
                    max_fanout = jefast->MaxOutdegree();
                }
                tuple_file.close();
            }
        }
        output_file.close();
    }
    
    // baseline skipped
}

void hover(std::string file_name1, std::string file_name2, std::string option)
{
    std::cout << "loading data..." << std::endl;

    //std::vector<int> sample_count = { 1000, 10000, 100000 };
    std::vector<int> sample_count = { 1000, 10000 };
    //std::vector<int> sample_count = { 1000000 };

    // load the data
    std::shared_ptr<TableGeneric> popular(new TableGeneric(file_name1, '\t', 1, 1));
    std::shared_ptr<TableGeneric> twitter(new TableGeneric(file_name2, '\t', 1, 2));
    std::shared_ptr<TableGeneric> table1(popular);
    std::shared_ptr<TableGeneric> table2(twitter);
    std::shared_ptr<TableGeneric> table3(popular);
    std::shared_ptr<TableGeneric> table4(twitter);

    popular->Build_indexes();
    twitter->Build_indexes();

    std::shared_ptr<TableGeneric_encap> table1G(new TableGeneric_encap(table1));
    std::shared_ptr<TableGeneric_encap> table2G(new TableGeneric_encap(table2));
    std::shared_ptr<TableGeneric_encap> table3G(new TableGeneric_encap(table3));
    std::shared_ptr<TableGeneric_encap> table4G(new TableGeneric_encap(table4));

    std::default_random_engine generator;
    generator.seed(std::chrono::steady_clock::now().time_since_epoch().count());

    std::cout << "size of input table1=" << table1->get_size() << std::endl;
    std::cout << "size of input table2=" << table2->get_size() << std::endl;
    std::cout << "size of input table3=" << table3->get_size() << std::endl;
    std::cout << "size of input table4=" << table4->get_size() << std::endl;


    std::vector<weight_t> max_fanout;

    // do experiment for adaptive
    std::cout << "adaptive" << std::endl;
    if (!settings.no_adaptive) {
        std::ofstream output_file;
        output_file.open(option + "_generic_snowflake_adaptiveB.txt");

        for (int trial = 0; trial < settings.trials; ++trial) {
            for (auto samples_to_collect : {1000, 10000, 100000, 1000000})
            {
                std::vector<int> results(4);

                DynamicIndex d_idx(4);

                d_idx.add_Table(0, table1, ((int64_t)10000) * 10000 * 10000 * 10000);
                d_idx.add_Table(1, table2, ((int64_t)10000) * 10000 * 10000);
                d_idx.add_Table(2, table3, ((int64_t)10000) * 10000);
                d_idx.add_Table(3, table4, ((int64_t)10000));

                std::chrono::steady_clock::time_point build_t1 = std::chrono::steady_clock::now();
                d_idx.initialize(1);
                d_idx.warmup();
                std::chrono::steady_clock::time_point build_t2 = std::chrono::steady_clock::now();

                std::chrono::steady_clock::time_point sample_t1 = std::chrono::steady_clock::now();

                int trials = 0;
                for (int good_samples = 0; good_samples < samples_to_collect; )
                {
                    bool good = d_idx.sample_join(results);
                    if (good // with manually checking the result
                        )
                    {
                            ++good_samples;
                    }


                    ++trials;
                }

                std::cout << "trials: " << trials << " collected: " << samples_to_collect << std::endl;

                std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

                output_file << samples_to_collect << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(build_t2 - build_t1).count() << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count()
                    //<< '\t' << trials << '\t' << 1.0 * samples_to_collect / trials
                    << std::endl;
            }
        }
        output_file.close();
    }

    // do experiment for DP
    std::cout << "DP" << std::endl;
    {
        std::ofstream output_file;
        output_file.open(option + "_generic_snowflake_jefastB.txt");

        for (int trial = 0; trial < settings.trials; ++trial) {
            std::chrono::steady_clock::time_point build_t1 = std::chrono::steady_clock::now();

            JefastBuilder jefast_index_builder;
            jefast_index_builder.AppendTable(table1G, -1, 1, 0);
            jefast_index_builder.AppendTable(table2G, 0, 1, 1);
            jefast_index_builder.AppendTable(table3G, 0, 1, 2);
            jefast_index_builder.AppendTable(table4G, 0, -1, 3);

            auto jefast = jefast_index_builder.Build();

            std::chrono::steady_clock::time_point build_t2 = std::chrono::steady_clock::now();
            std::cout << "DP build done" << std::endl;
            for (auto samples_to_collect : {1000, 10000, 100000, 1000000})
            {
                std::vector<jefastKey_t> results;
                //std::vector<weight_t> random_values(samples_to_collect);

                int remaining_samples = samples_to_collect;

                std::chrono::steady_clock::time_point sample_t1 = std::chrono::steady_clock::now();

                std::uniform_int_distribution<weight_t> distribution(0, jefast->GetTotal() - 1);


                //std::generate(random_values.begin(), random_values.end(), [&]() {return distribution(generator);});
                //std::sort(random_values.begin(), random_values.end());

                //for (int i : random_values) {
                int trials = 0;
                while (remaining_samples > 0) {
                    ++trials;
                    int64_t r_val = distribution(generator);
                    jefast->GetJoinNumber(r_val, results);
                    {
                        --remaining_samples;
                    }
                }


                std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

                output_file << samples_to_collect << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(build_t2 - build_t1).count() << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count()
                    //<< '\t' << trials << '\t' << 1.0 * samples_to_collect / trials
                    << std::endl;

                // this is to be used by baseline join
                if (max_fanout.size() == 0)
                    max_fanout = jefast->MaxOutdegree();
            }
        }
        output_file.close();
    }

    // do experiment for baseline
    {
        std::ofstream output_file;
        output_file.open(option + "_generic_snowflake_baseline.txt");

        for (auto i : max_fanout)
            std::cout << i << " ";
        std::cout << std::flush;

        olkenIndex Ojoin(4);
        Ojoin.add_Table(0, table1, 1.0);
        Ojoin.add_Table(1, table2, max_fanout[0]);
        Ojoin.add_Table(2, table3, max_fanout[1]);
        Ojoin.add_Table(3, table4, max_fanout[2]);

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

                    if (Ojoin.sample_join(results))
                    {
                        ++good_samples;
                    }
                }


                std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

                output_file << samples_to_collect << '\t'
                    << 0.0 /* we say build time is nothing */ << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count()
                    << '\t' << trials << '\t' << 1.0 * samples_to_collect / trials
                    << std::endl;

                std::cout << trials << ' ' << samples_to_collect << std::endl;
            }
        }
    }
}

void triangle(std::string file_name1, std::string file_name2, std::string file_name3, std::string option)
{
    std::cout << "loading data..." << std::endl;

    //std::vector<int> sample_count = { 1000, 10000, 100000 };
    std::vector<int> sample_count = { 1000000 };
    //std::vector<int> sample_count = { 1000000 };

    // load the data
    std::shared_ptr<TableGeneric> table1(new TableGeneric(file_name1, '\t', 1, 2));
    std::shared_ptr<TableGeneric> table2(new TableGeneric(file_name2, '\t', 1, 2));
    std::shared_ptr<TableGeneric> table3(new TableGeneric(file_name3, '\t', 1, 2));

    table1->Build_indexes();
    table2->Build_indexes();
    table3->Build_indexes();

    std::shared_ptr<TableGeneric_encap> table1G(new TableGeneric_encap(table1));
    std::shared_ptr<TableGeneric_encap> table2G(new TableGeneric_encap(table2));
    std::shared_ptr<TableGeneric_encap> table3G(new TableGeneric_encap(table3));

    std::default_random_engine generator;
    generator.seed(std::chrono::steady_clock::now().time_since_epoch().count());

    std::cout << "size of input table1=" << table1->get_size() << std::endl;
    std::cout << "size of input table2=" << table2->get_size() << std::endl;
    std::cout << "size of input table3=" << table3->get_size() << std::endl;


    std::vector<weight_t> max_fanout;

    // do experiment for adaptive
    std::cout << "adaptive" << std::endl;
    if (true) {
        std::ofstream output_file;
        output_file.open(option + "_generic_TW_adaptiveB.txt");

        for (int trial = 0; trial < settings.trials; ++trial) {
            for (auto samples_to_collect : sample_count )
            {
                std::vector<int> results(3);

                DynamicIndex d_idx(2);

                d_idx.add_Table(0, table1, ((int64_t)65225));
                d_idx.add_Table(1, table2, ((int64_t)1));
                //d_idx.add_Table(2, table3, ((int64_t)10000));

                std::chrono::steady_clock::time_point build_t1 = std::chrono::steady_clock::now();
                d_idx.initialize(1);
                d_idx.warmup();
                std::chrono::steady_clock::time_point build_t2 = std::chrono::steady_clock::now();

                std::cout << "warm up done" << std::endl;

                std::chrono::steady_clock::time_point sample_t1 = std::chrono::steady_clock::now();

                int trials = 0;
                for (int good_samples = 0; good_samples < samples_to_collect; )
                {
                    bool good = d_idx.sample_join(results);
                    //int last_idx = table3->get_index_of_values(table1->get_column2_value(results[1]), table2->get_column1_value(results[0]));
                    if (good // with manually checking the result
                        )
                    {
                        //std::cout << results[0] << ' ' << results[1] << std::endl;

                        int dst = table1->get_column1_value(results[0]);
                        int src = table2->get_column2_value(results[1]);

                        if(table3->get_index_of_values(src, dst) != -1)
                        //if (table1->get_column1_value(results[0]) == table3->get_column2_value(results[2]))
                            ++good_samples;
                    }


                    ++trials;
                }

                std::cout << "trials: " << trials << " collected: " << samples_to_collect << std::endl;

                std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

                output_file << samples_to_collect << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(build_t2 - build_t1).count() << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count() 
                    << '\t' << trials << '\t' << 1.0 * samples_to_collect / trials
                    << std::endl;
            }
        }
        output_file.close();
    }

    // do experiment for DP
    std::cout << "DP" << std::endl;
    if (true) {
        std::ofstream output_file;
        output_file.open(option + "_generic_TW_jefastB.txt");

        for (int trial = 0; trial < settings.trials; ++trial) {
            std::chrono::steady_clock::time_point build_t1 = std::chrono::steady_clock::now();

            JefastBuilder jefast_index_builder;
            jefast_index_builder.AppendTable(table1G, -1, 1, 0);
            jefast_index_builder.AppendTable(table2G, 0, -1, 1);
            //jefast_index_builder.AppendTable(table3G, 0, -1, 2);

            auto jefast = jefast_index_builder.Build();

            std::chrono::steady_clock::time_point build_t2 = std::chrono::steady_clock::now();

            std::cout << "dp build done" << std::endl;
            for (auto samples_to_collect : sample_count)
            {
                std::vector<jefastKey_t> results;
                //std::vector<weight_t> random_values(samples_to_collect);

                int remaining_samples = samples_to_collect;

                std::chrono::steady_clock::time_point sample_t1 = std::chrono::steady_clock::now();

                std::uniform_int_distribution<weight_t> distribution(0, jefast->GetTotal() - 1);


                //std::generate(random_values.begin(), random_values.end(), [&]() {return distribution(generator);});
                //std::sort(random_values.begin(), random_values.end());

                //for (int i : random_values) {
                int trials = 0;
                while (remaining_samples > 0) {
                    ++trials;
                    int64_t r_val = distribution(generator);
                    jefast->GetJoinNumber(r_val, results);
                    //if (table1->get_column1_value(results[0]) == table3->get_column2_value(results[2]))
                    {
                        int dst = table1->get_column1_value(results[0]);
                        int src = table2->get_column2_value(results[1]);

                        if (table3->get_index_of_values(src, dst) != -1)
                            --remaining_samples;
                    }
                }


                std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

                output_file << samples_to_collect << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(build_t2 - build_t1).count() << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count()
                    << '\t' << trials << '\t' << 1.0 * samples_to_collect / trials 
                    << std::endl;

                // this is to be used by baseline join
                if (max_fanout.size() == 0)
                    max_fanout = jefast->MaxOutdegree();
            }
        }
        output_file.close();
    }

     //do experiment for baseline
    if (false) {
        std::ofstream output_file;
        output_file.open(option + "_generic_TW_baseline.txt");

        for (auto i : max_fanout)
            std::cout << i << " ";
        std::cout << std::flush;

        olkenIndex Ojoin(2);
        Ojoin.add_Table(0, table1, 1.0);
        Ojoin.add_Table(1, table2, max_fanout[0]);
        //Ojoin.add_Table(2, table3, max_fanout[1]);

        for (int trial = 0; trial < settings.trials; ++trial) {
            for (auto samples_to_collect : {10000})
            {
                std::vector<jefastKey_t> results;

                std::chrono::steady_clock::time_point sample_t1 = std::chrono::steady_clock::now();

                int trials = 0;

                for (int good_samples = 0; good_samples < samples_to_collect; ++trials)
                {
                    if (std::chrono::duration_cast<std::chrono::duration<double>>(std::chrono::steady_clock::now() - sample_t1).count() > experiment_timeout)
                        break;

                    if (Ojoin.sample_join(results))
                        //&& (table1->get_column1_value(results[0]) == table3->get_column2_value(results[2])))
                    {
                        int dst = table1->get_column1_value(results[0]);
                        int src = table2->get_column2_value(results[1]);

                        if (table3->get_index_of_values(src, dst) != -1)
                        ++good_samples;
                    }
                }


                std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

                output_file << samples_to_collect << '\t'
                    << 0.0 /* we say build time is nothing */ << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count()
                    << '\t' << trials << '\t' << 1.0 * samples_to_collect / trials 
                    << std::endl;

                std::cout << trials << ' ' << samples_to_collect << std::endl;
            }
        }
    }
}


void square(std::string file_name1, std::string file_name2, std::string file_name3, std::string file_name4, std::string option)
{
    std::cout << "loading data..." << std::endl;

    //std::vector<int> sample_count = { 1, 1, 1};
    std::vector<int> sample_count = { 50000 };

    // load the data
    std::shared_ptr<TableGeneric> table1(new TableGeneric(file_name1, '\t', 1, 2));
    std::shared_ptr<TableGeneric> table2(new TableGeneric(file_name2, '\t', 1, 2));
    std::shared_ptr<TableGeneric> table3(table2);
    std::shared_ptr<TableGeneric> table4(table2);
    //std::shared_ptr<TableGeneric> table3(new TableGeneric(file_name3, '\t', 1, 2));
    //std::shared_ptr<TableGeneric> table4(new TableGeneric(file_name4, '\t', 1, 2));

    table1->Build_indexes();
    table2->Build_indexes();
    //table3->Build_indexes();
    //table4->Build_indexes();

    std::vector<std::shared_ptr<TableGeneric> > table_list;
    table_list.push_back(table1);
    table_list.push_back(table2);
    table_list.push_back(table3);
    table_list.push_back(table4);

    std::shared_ptr<TableGeneric_encap> table1G(new TableGeneric_encap(table1));
    std::shared_ptr<TableGeneric_encap> table2G(new TableGeneric_encap(table2));
    std::shared_ptr<TableGeneric_encap> table3G(new TableGeneric_encap(table3));
    std::shared_ptr<TableGeneric_encap> table4G(new TableGeneric_encap(table4));

    std::default_random_engine generator;
    generator.seed(std::chrono::steady_clock::now().time_since_epoch().count());

    std::vector<weight_t> max_fanout;

    //{    
    //    std::ofstream output_file;
    //    output_file.open("twitter_square_orientation_sizes_full.txt");

    //    for (int orientation = 0; orientation <= 4; ++orientation)
    //    {
    //        std::vector<int> results(4);

    //        DynamicIndex d_idx(4);

    //        d_idx.add_Table(0, table_list[orientation % 4], (int64_t)10 * 10 * 10 * 10 * 10 * 10 * 10);
    //        d_idx.add_Table(1, table_list[(orientation + 1) % 4], (int64_t)10 * 10 * 10 * 10 * 10 * 10);
    //        d_idx.add_Table(2, table_list[(orientation + 2) % 4], (int64_t)10 * 10 * 10 * 10 * 10);
    //        d_idx.add_Table(3, table_list[(orientation + 3) % 4], (int64_t)10 * 10 * 10 * 10);


    //        d_idx.initialize(1);
    //        d_idx.warmup();

    //        std::cout << "orientation " << orientation << " size est: " << d_idx.size_est() << std::endl;
    //        output_file << "orientation " << orientation << " size est: " << d_idx.size_est() << std::endl;
    //    }
    //}

    //{
    //    std::ofstream output_file;
    //    output_file.open("twitter_square_orientation_sizes.txt");

    //    for (int orientation = 0; orientation <= 4; ++orientation)
    //    {
    //        std::vector<int> results(3);

    //        DynamicIndex d_idx(3);

    //        d_idx.add_Table(0, table_list[orientation % 4], (int64_t)10 * 10 * 10 * 10 * 10 * 10 * 10);
    //        d_idx.add_Table(1, table_list[(orientation + 1) % 4], (int64_t)10 * 10 * 10 * 10 * 10 * 10);
    //        d_idx.add_Table(2, table_list[(orientation + 2) % 4], (int64_t)10 * 10 * 10 * 10 * 10);
    //        //d_idx.add_Table(3, table_list[(orientation + 3) % 4], (int64_t)10 * 10 * 10 * 10);


    //        d_idx.initialize(1);
    //        d_idx.warmup();

    //        std::cout << "orientation " << orientation << " size est: " << d_idx.size_est() << std::endl;
    //        output_file << "orientation " << orientation << " size est: " << d_idx.size_est() << std::endl;
    //    }
    //}

    //return;

    // do experiment for adaptive
    std::cout << "adaptive" << std::endl;
    if(!settings.no_adaptive)
    {
        std::ofstream output_file;
        output_file.open(option + "_generic_SW_adaptive_first.txt");

        for (int trial = 0; trial < settings.trials; ++trial) {
            for (auto samples_to_collect : sample_count)
            {
                std::vector<int> results(4);

                DynamicIndex d_idx(3);

                d_idx.add_Table(0, table1, ((int64_t)9000) * 9000 * 9000 * 9000);
                d_idx.add_Table(1, table2, ((int64_t)9000) * 9000 * 9000);
                d_idx.add_Table(2, table3, ((int64_t)9000) * 9000);
                //d_idx.add_Table(3, table4, ((int64_t)9000));

                std::chrono::steady_clock::time_point build_t1 = std::chrono::steady_clock::now();
                d_idx.initialize(1);
                d_idx.warmup();
                std::chrono::steady_clock::time_point build_t2 = std::chrono::steady_clock::now();

                std::chrono::steady_clock::time_point sample_t1 = std::chrono::steady_clock::now();
                
               int trials = 0;
               for (int good_samples = 0; good_samples < samples_to_collect; )
                {
                    ++trials;
                    bool good = d_idx.sample_join(results);
                    if (good // manually check results
                        //&& table1->get_column1_value(results[0]) == table4->get_column2_value(results[3])
                        )
                    {
                        int dst = table1->get_column1_value(results[0]);
                        int src = table3->get_column2_value(results[2]);

                        if (table4->get_index_of_values(src, dst) != -1)
                            ++good_samples;
                    }
                }

                std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

                output_file << samples_to_collect << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(build_t2 - build_t1).count() << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count()
                    //<< '\t' << trials << '\t' << 1.0 * samples_to_collect / trials 
                    << '\n';
            }
        }
        output_file.close();
    }

    // do experiment for DP
    std::cout << "DP" << std::endl;
    //if(!settings.no_DP)
    {
        std::ofstream output_file;
        output_file.open(option + "_generic_SW_jefast_first.txt");

        for (int trial = 0; trial < settings.trials; ++trial) {
            //for (auto samples_to_collect : sample_count)
            std::chrono::steady_clock::time_point build_t1 = std::chrono::steady_clock::now();

            JefastBuilder jefast_index_builder;
            jefast_index_builder.AppendTable(table1G, -1, 1, 0);
            jefast_index_builder.AppendTable(table2G, 0, 1, 1);
            jefast_index_builder.AppendTable(table3G, 0, -1, 2);
            //jefast_index_builder.AppendTable(table4G, 0, -1, 3);

            auto jefast = jefast_index_builder.Build();

            std::chrono::steady_clock::time_point build_t2 = std::chrono::steady_clock::now();
            for(auto samples_to_collect : sample_count)
            {
                std::vector<jefastKey_t> results;
                //std::vector<weight_t> random_values(samples_to_collect);

                int remaining_samples = samples_to_collect;

                std::chrono::steady_clock::time_point sample_t1 = std::chrono::steady_clock::now();

                std::uniform_int_distribution<weight_t> distribution(0, jefast->GetTotal() - 1);


                //std::generate(random_values.begin(), random_values.end(), [&]() {return distribution(generator);});
                //std::sort(random_values.begin(), random_values.end());

                //for (int i : random_values) {
                int trials = 0;
                while (remaining_samples > 0) {
                    ++trials;
                    int64_t r_val = distribution(generator);
                    jefast->GetJoinNumber(r_val, results);
                    //if (table1->get_column1_value(results[0]) == table4->get_column2_value(results[3]))
                    {
                        int dst = table1->get_column1_value(results[0]);
                        int src = table3->get_column2_value(results[2]);

                        if (table4->get_index_of_values(src, dst) != -1)
                            --remaining_samples;
                    }
                }


                std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

                output_file << samples_to_collect << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(build_t2 - build_t1).count() << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count()
                    //<< '\t' << trials << '\t' << 1.0 * samples_to_collect / trials 
                    << '\n';

                // this is to be used by baseline join
                if (max_fanout.size() == 0)
                    max_fanout = jefast->MaxOutdegree();
            }
        }
        output_file.close();
    }

     //do experiment for baseline
    {
        std::ofstream output_file;
        output_file.open(option + "_generic_SW_baseline_first.txt");

        for (auto i : max_fanout)
            std::cout << i << " ";
        std::cout << std::endl;

        olkenIndex Ojoin(3);
        Ojoin.add_Table(0, table1, 1.0);
        Ojoin.add_Table(1, table2, max_fanout[0]);
        Ojoin.add_Table(2, table3, max_fanout[1]);
        //Ojoin.add_Table(3, table4, max_fanout[2]);

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
                        //&& (table1->get_column1_value(results[0]) == table4->get_column2_value(results[3]))
                        )
                    {
                        int dst = table1->get_column1_value(results[0]);
                        int src = table3->get_column2_value(results[2]);

                        if (table4->get_index_of_values(src, dst) != -1)
                            ++good_samples;
                    }
                }


                std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

                output_file << samples_to_collect << '\t'
                    << 0.0 /* we say build time is nothing */ << '\t'
                    << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count()
                    //<< '\t' << trials << '\t' << 1.0 * samples_to_collect / trials
                    << std::endl;

                std::cout << trials << ' ' << samples_to_collect << std::endl;
            }
        }
    }

    // do baseline (takes too much time to process)
    // do experiment for adaptive
    //std::cout << "adaptive-o" << std::endl;
    //{
    //    std::ofstream output_file;
    //    output_file.open(file_name + "_generic_SW_adaptive-o.txt");

    //    for (auto samples_to_collect : sample_count)
    //    {
    //        std::vector<int> results(4);

    //        DynamicIndex d_idx(4);

    //        d_idx.add_Table(0, table, ((int64_t)max_fanout[2] * max_fanout[1] * max_fanout[0]) * 10000 /* magic number! */);
    //        d_idx.add_Table(1, table, ((int64_t)max_fanout[2] * max_fanout[1] * max_fanout[0]));
    //        d_idx.add_Table(2, table, ((int64_t)max_fanout[2] * max_fanout[1]));
    //        d_idx.add_Table(3, table, ((int64_t)max_fanout[2]));
    //        d_idx.initialize();

    //        std::chrono::steady_clock::time_point sample_t1 = std::chrono::steady_clock::now();

    //        for (int good_samples = 0; good_samples < samples_to_collect; )
    //        {
    //            bool good = d_idx.sample_join(results);
    //            if (good // for triangle join we must also validate the triangle part of the join
    //                && table->get_column1_value(results[0]) == table->get_column2_value(results[3])
    //                )
    //                ++good_samples;
    //        }

    //        std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

    //        output_file << samples_to_collect << '\t'
    //            << 0.0 /* we say build time is nothing */ << '\t'
    //            << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count() << '\n';
    //    }

    //    output_file.close();
    //}
}

void parse_args_generic(int argc, char ** argv)
{
    TCLAP::CmdLine cmd("Experiments for generic samples with scale factor options", ' ');

    TCLAP::ValueArg<int> arg_SF("S", "SF", "scale factor to use for experiments.", false, 10, "int");
    TCLAP::ValueArg<int> arg_trials("T", "trials", "number of trials to do for each experiment", false, 1, "int");

    TCLAP::SwitchArg arg_doQ3("", "do_Q3", "do query 3 experiments", cmd, false);
    TCLAP::SwitchArg arg_doQX("", "do_QX", "do query X experiments", cmd, false);
    TCLAP::SwitchArg arg_doQY("", "do_QY", "do query Y experiments", cmd, false);
    TCLAP::SwitchArg arg_doQY_timed("", "do_QY_timed", "do query Y experiments", cmd, false);
    TCLAP::SwitchArg arg_doQN("", "do_QN", "do query N experiments", cmd, false);
    TCLAP::SwitchArg arg_doSTri("", "do_smallTri", "do the small triangle join", cmd, false);
    //TCLAP::SwitchArg arg_doBTri("", "do_bigTri", "do the big triangle join", cmd, false);
    TCLAP::SwitchArg arg_doSSqua("", "do_smallSquare", "do the square join", cmd, false);
    TCLAP::SwitchArg arg_doBSqua("", "do_bigSquare", "do big square experiment", cmd, false);
    TCLAP::SwitchArg arg_doSkewTri("", "do_skewTri", "do skewTriangle", cmd, false);
    TCLAP::SwitchArg arg_doHover("", "do_extra", "do extra", cmd, false);

    TCLAP::SwitchArg arg_noAdapt("", "skip_adaptive", "skip the adaptive experiences", cmd, false);
    TCLAP::SwitchArg arg_noDP("", "skip_DP", "skip the DP experiments", cmd, false);

    cmd.add(arg_SF);
    cmd.add(arg_trials);

    cmd.parse(argc, argv);

    settings.scale_factor = arg_SF.getValue();
    settings.trials = arg_trials.getValue();

    settings.do_Q3 = arg_doQ3.getValue();
    settings.do_QX = arg_doQX.getValue();
    settings.do_QY = arg_doQY.getValue();
    settings.do_QY_timed = arg_doQY_timed.getValue();
    settings.do_QN = arg_doQN.getValue();
    settings.do_triangle = arg_doSTri.getValue();
    //settings.do_bigtriangle = arg_doBTri.getValue();
    settings.do_smallsquare = arg_doSSqua.getValue();
    settings.do_bigsquare = arg_doBSqua.getValue();
    settings.do_SkewTri = arg_doSkewTri.getValue();
    settings.do_extra = arg_doHover.getValue();

    settings.no_adaptive = arg_noAdapt.getValue();
    settings.no_DP = arg_noDP.getValue();
}

int main(int argc, char **argv)
{
    parse_args_generic(argc, argv);

    std::cout << "using sf=" << settings.scale_factor << std::endl;
    ////
    if (settings.do_Q3) {
        std::cout << "doing tcp q3 reverse experiments" << std::endl;
        TCP3_reverse(settings.scale_factor);

        std::cout << "doing tcp q3 experiments" << std::endl;
        TCP3(settings.scale_factor);
    }
    else
    {
        std::cout << "skipping tcp q3 experiments" << std::endl;
    }
    ////
    if (settings.do_QX) {
        std::cout << "doing tcp qx experiments" << std::endl;
        TCPX(settings.scale_factor);
    }
    else
    {
        std::cout << "skipping tcp query x experiments" << std::endl;
    }
    ////
    if (settings.do_QY) {
        std::cout << "doing tcp qy experiments" << std::endl;
        TCPY(settings.scale_factor);
    }
    else
    {
        std::cout << "skipping tcp query y experiments" << std::endl;
    }
    if (settings.do_QY_timed) {
        std::cout << "doing tpc-h qy timed experiments" << std::endl;
        TCPY_timed(settings.scale_factor);
    } else {
        std::cout << "skipping tpc-h qy timed experiments" << std::endl;
    }
    
    if (settings.do_QN) {
        std::cout << "doing TPC-H QN experiments" << std::endl;
        TPCH_QN(settings.scale_factor);
    } else {
        std::cout << "skipping TPC-H QN experiments" << std::endl;
    }

    ////
    if (settings.do_triangle) {
        std::cout << "doing triangle query 122" << std::endl;
        //triangle("1.txt", "2.txt", "2.txt", "twitter_debug_triangle");
        triangle("1_50x.txt", "2_50x.txt", "2_50x.txt", "twitter_file122_50x");
        //triangle("twitter_2.txt", "twitter_2.txt", "twitter_1.txt", "twitter_BBA");
        //triangle("twitter_2.txt", "twitter_1.txt", "twitter_2.txt", "twitter_BAB");
        //triangle("twitter_10.txt", "twitter_20.txt", "twitter_1000.txt", "twitter_ABC");
        //triangle("twitter_20.txt", "twitter_1000.txt", "twitter_10.txt", "twitter_BCA");
        //triangle("twitter_1000.txt", "twitter_10.txt", "twitter_20.txt", "twitter_CAB");
    } 
    else
    {
        std::cout << "skipping small triangle join" << std::endl;
    }
    ////
    if (settings.do_smallsquare) {
        std::cout << "doing square query 1222" << std::endl;
        square("1_50x.txt", "2_50x.txt", "2_50x.txt", "2_50x.txt", "twitter_1222_50x");


        //square("twitter_1.txt", "twitter_2.txt", "twitter_2.txt", "twitter_2.txt", "twitter_ABBB");
        //square("twitter_2.txt", "twitter_2.txt", "twitter_2.txt", "twitter_1.txt", "twitter_BBBA");
        //square("twitter_2.txt", "twitter_2.txt", "twitter_1.txt", "twitter_2.txt", "twitter_BBAB");
        //square("twitter_2.txt", "twitter_1.txt", "twitter_2.txt", "twitter_2.txt", "twitter_BABB");
        //square("twitter_10.txt", "twitter_20.txt", "twitter_30.txt", "twitter_1000.txt", "twitter_ABCD");
        //square("twitter_20.txt", "twitter_30.txt", "twitter_1000.txt", "twitter_10.txt", "twitter_BCDA");
        //square("twitter_30.txt", "twitter_1000.txt", "twitter_10.txt", "twitter_20.txt", "twitter_CDAB");
        //square("twitter_1000.txt", "twitter_10.txt", "twitter_20.txt", "twitter_30.txt", "twitter_DABC");
    }
    else
    {
        std::cout << "skipping small square join" << std::endl;
    }

    if (settings.do_extra) {
        std::cout << "doing extra qeury " << std::endl;
        hover("1_50x.txt", "2_50x.txt", "twitter_snowflake_50x");
    }


    ////
    //if (settings.do_bigsquare) {
    //    std::cout << "doing square query" << std::endl;
    //    square("gplus.edge.uniq");
    //}
    //else
    //{
    //    std::cout << "skipping big square join" << std::endl;
    //}

    //if (settings.do_SkewTri) {
    //    std::cout << "doing skew tri" << std::endl;
    //    triangle("graph_A.txt", "graph_B.txt", "graph_C.txt");
    //    triangle("graph_B.txt", "graph_C.txt", "graph_A.txt");
    //    triangle("graph_C.txt", "graph_A.txt", "graph_B.txt");
    //}
    //else
    //{
    //    std::cout << "skipping skew triangle" << std::endl;
    //}

    std::cout << "done" << std::endl;
    return 0;
}
