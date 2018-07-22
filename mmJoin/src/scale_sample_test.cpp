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
};

generic_settings settings;

void TCPX(int sf = 10)
{
    // open tables
    //std::vector<int> sample_count = { 1000, 10000, 100000, 1000000 };
    //std::vector<int> sample_count = { 10, 1000 };
    std::vector<int> sample_count = { 1000000 };

    // load the data
    std::shared_ptr<TableGeneric> table1(new TableGeneric(std::to_string(sf) + "x/nation.tbl", '|', 1, 1));
    std::shared_ptr<TableGeneric> table2(new TableGeneric(std::to_string(sf) + "x/supplier.tbl", '|', 4, 4));
    std::shared_ptr<TableGeneric> table3(new TableGeneric(std::to_string(sf) + "x/customer.tbl", '|', 4, 1));
    std::shared_ptr<TableGeneric> table4(new TableGeneric(std::to_string(sf) + "x/orders.tbl", '|', 2, 1));
    std::shared_ptr<TableGeneric> table5(new TableGeneric(std::to_string(sf) + "x/lineitem_skew.tbl", '|', 1, 1));

    table1->Build_indexes();
    table2->Build_indexes();
    table3->Build_indexes();
    table4->Build_indexes();
    table5->Build_indexes();

    std::shared_ptr<TableGeneric_encap> table1G(new TableGeneric_encap(table1));
    std::shared_ptr<TableGeneric_encap> table2G(new TableGeneric_encap(table2));
    std::shared_ptr<TableGeneric_encap> table3G(new TableGeneric_encap(table3));
    std::shared_ptr<TableGeneric_encap> table4G(new TableGeneric_encap(table4));
    std::shared_ptr<TableGeneric_encap> table5G(new TableGeneric_encap(table5));

    std::default_random_engine generator;
    generator.seed(std::chrono::steady_clock::now().time_since_epoch().count());

    std::vector<weight_t> max_fanout;

    // do experiment for adaptive
    {
        //std::ofstream output_file;
        std::ofstream output_samples;
        //output_file.open(std::to_string(sf) + "x_QX_adaptive.txt");
        output_samples.open(std::to_string(sf) + "x_QX_adaptive_results.txt");

        for (int trial = 0; trial < settings.trials; ++trial) {
            for (auto samples_to_collect : sample_count)
            {
                std::vector<int> results(5);

                DynamicIndex d_idx(5);

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
                    {
                        output_samples << results[0] << ' ' << results[1] << ' ' << results[2] << ' ' << results[3] << ' ' << results[4] << '\n';
                        ++good_samples;
                    }
                }

                std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

                //output_file << samples_to_collect << '\t'
                //    << std::chrono::duration_cast<std::chrono::duration<double>>(build_t2 - build_t1).count() << '\t'
                //    << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count() << '\n';
            }
        }
        output_samples.close();
    }

    // do experiment for DP
    {
        std::ofstream output_results;
        output_results.open(std::to_string(sf) + "x_QX_jefast_results.txt");

        for (int trial = 0; trial < settings.trials; ++trial) {
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

                //for (int i : random_values) {
                //    jefast->GetJoinNumber(i, results);
                //}

                for (int i = 0; i < samples_to_collect; ++i)
                {
                    jefast->GetJoinNumber(distribution(generator), results);
                    output_results << results[0] << ' ' << results[1] << ' ' << results[2] << ' ' << results[3] << ' ' << results[4] << '\n';
                }

                std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();


                // this is to be used by baseline join
                if (max_fanout.size() == 0)
                    max_fanout = jefast->MaxOutdegree();
            }
        }

        output_results.close();
    }

    // do experiment for baseline
    {
        std::ofstream output_results;
        output_results.open(std::to_string(sf) + "x_QX_baseline_results.txt");

        for (auto i : max_fanout)
            std::cout << i << " ";
        std::cout << std::flush;

        olkenIndex Ojoin(5);
        Ojoin.add_Table(0, table1, 1.0);
        Ojoin.add_Table(1, table2, max_fanout[0]);
        Ojoin.add_Table(2, table3, max_fanout[1]);
        Ojoin.add_Table(3, table4, max_fanout[2]);
        Ojoin.add_Table(4, table5, max_fanout[3]);

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
                        output_results << results[0] << ' ' << results[1] << ' ' << results[2] << ' ' << results[3] << ' ' << results[4] << '\n';
                        ++good_samples;
                    }
                }

                std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

                std::cout << trials << ' ' << samples_to_collect << std::endl;
            }
        }
    }
}


void triangle(std::string file_name1, std::string file_name2, std::string file_name3, std::string option)
{
    std::cout << "loading data..." << std::endl;

    std::vector<int> sample_count = { 1000, 10000, 100000 };
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
    {
        std::ofstream output_results;
        output_results.open(option + "_generic_TW_adaptive_results.txt");

        for (int trial = 0; trial < settings.trials; ++trial) {
            for (auto samples_to_collect : sample_count)
            {
                std::vector<int> results(2);

                DynamicIndex d_idx(2);

                d_idx.add_Table(0, table1, ((int64_t)10000) * 10000 * 10000);
                d_idx.add_Table(1, table2, ((int64_t)10000) * 10000);
                //d_idx.add_Table(2, table3, ((int64_t)10000));

                std::chrono::steady_clock::time_point build_t1 = std::chrono::steady_clock::now();
                d_idx.initialize(1);
                d_idx.warmup();
                std::chrono::steady_clock::time_point build_t2 = std::chrono::steady_clock::now();

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

                        if (table3->get_index_of_values(src, dst) != -1)
                        {
                            output_results << results[0] << ' ' << results[1] << ' ' << table3->get_index_of_values(src, dst) << '\n';
                            ++good_samples;
                        }
                    }


                    ++trials;
                }

                std::cout << "trials: " << trials << " collected: " << samples_to_collect << std::endl;

                std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

            }
        }
        output_results.close();
    }

    // do experiment for DP
    std::cout << "DP" << std::endl;
    {
        std::ofstream output_results;
        output_results.open(option + "_generic_TW_jefast_results.txt");

        for (int trial = 0; trial < settings.trials; ++trial) {
            for (auto samples_to_collect : sample_count)
            {
                std::vector<jefastKey_t> results;
                //std::vector<weight_t> random_values(samples_to_collect);

                int remaining_samples = samples_to_collect;

                std::chrono::steady_clock::time_point build_t1 = std::chrono::steady_clock::now();

                JefastBuilder jefast_index_builder;
                jefast_index_builder.AppendTable(table1G, -1, 1, 0);
                jefast_index_builder.AppendTable(table2G, 0, -1, 1);
                //jefast_index_builder.AppendTable(table3G, 0, -1, 2);

                auto jefast = jefast_index_builder.Build();

                std::chrono::steady_clock::time_point build_t2 = std::chrono::steady_clock::now();
                std::chrono::steady_clock::time_point sample_t1 = std::chrono::steady_clock::now();

                std::uniform_int_distribution<weight_t> distribution(0, jefast->GetTotal() - 1);


                //std::generate(random_values.begin(), random_values.end(), [&]() {return distribution(generator);});
                //std::sort(random_values.begin(), random_values.end());

                //for (int i : random_values) {
                while (remaining_samples > 0) {
                    int64_t r_val = distribution(generator);
                    jefast->GetJoinNumber(r_val, results);
                    //if (table1->get_column1_value(results[0]) == table3->get_column2_value(results[2]))
                    //{
                    int dst = table1->get_column1_value(results[0]);
                    int src = table2->get_column2_value(results[1]);

                    if (table3->get_index_of_values(src, dst) != -1)
                    {
                        output_results << results[0] << ' ' << results[1] << ' ' << table3->get_index_of_values(src, dst) << '\n';
                        --remaining_samples;
                    }
                    //}
                }


                std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

                // this is to be used by baseline join
                if (max_fanout.size() == 0)
                    max_fanout = jefast->MaxOutdegree();
            }
        }
        output_results.close();
    }

    // do experiment for baseline
    {
        std::ofstream output_results;
        output_results.open(option + "_generic_TW_baseline_results.txt");

        for (auto i : max_fanout)
            std::cout << i << " ";
        std::cout << std::flush;

        olkenIndex Ojoin(2);
        Ojoin.add_Table(0, table1, 1.0);
        Ojoin.add_Table(1, table2, max_fanout[0]);
        //Ojoin.add_Table(2, table3, max_fanout[1]);

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
                        //&& (table1->get_column1_value(results[0]) == table3->get_column2_value(results[2])))
                    {
                        int dst = table1->get_column1_value(results[0]);
                        int src = table2->get_column2_value(results[1]);

                        if (table3->get_index_of_values(src, dst) == -1) {
                            output_results << results[0] << ' ' << results[1] << ' ' << table3->get_index_of_values(src, dst) << '\n';
                            ++good_samples;
                        }
                    }
                }


                std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

                std::cout << trials << ' ' << samples_to_collect << std::endl;
            }
        }
    }
}


void parse_args_generic(int argc, char ** argv)
{
    TCLAP::CmdLine cmd("Experiments for generic samples with scale factor options", ' ');

    TCLAP::ValueArg<int> arg_SF("S", "SF", "scale factor to use for experiments.", false, 10, "int");
    TCLAP::ValueArg<int> arg_trials("T", "trials", "number of trials to do for each experiment", false, 3, "int");

    //TCLAP::SwitchArg arg_doQ3("", "do_Q3", "do query 3 experiments", cmd, false);
    TCLAP::SwitchArg arg_doQX("", "do_QX", "do query X experiments", cmd, false);
    //TCLAP::SwitchArg arg_doQY("", "do_QY", "do query Y experiments", cmd, false);
    TCLAP::SwitchArg arg_doSTri("", "do_smallTri", "do the small triangle join", cmd, false);
    //TCLAP::SwitchArg arg_doBTri("", "do_bigTri", "do the big triangle join", cmd, false);
    //TCLAP::SwitchArg arg_doSSqua("", "do_smallSquare", "do the square join", cmd, false);
    //TCLAP::SwitchArg arg_doBSqua("", "do_bigSquare", "do big square experiment", cmd, false);
    //TCLAP::SwitchArg arg_doSkewTri("", "do_skewTri", "do skewTriangle", cmd, false);

    cmd.add(arg_SF);

    cmd.parse(argc, argv);

    settings.scale_factor = arg_SF.getValue();
    settings.trials = arg_trials.getValue();

    //settings.do_Q3 = arg_doQ3.getValue();
    settings.do_QX = arg_doQX.getValue();
    //settings.do_QY = arg_doQY.getValue();
    settings.do_triangle = arg_doSTri.getValue();
    //settings.do_bigtriangle = arg_doBTri.getValue();
    //settings.do_smallsquare = arg_doSSqua.getValue();
    //settings.do_bigsquare = arg_doBSqua.getValue();
    //settings.do_SkewTri = arg_doSkewTri.getValue();
}

int main(int argc, char **argv)
{
    parse_args_generic(argc, argv);

    std::cout << "using sf=" << settings.scale_factor << std::endl;
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
    if (settings.do_triangle) {
        std::cout << "doing triangle query" << std::endl;
        triangle("twitter_10.txt", "twitter_20.txt", "twitter_1000.txt", "twitter_ABC");
        triangle("twitter_20.txt", "twitter_1000.txt", "twitter_10.txt", "twitter_BCA");
        triangle("twitter_1000.txt", "twitter_10.txt", "twitter_20.txt", "twitter_CAB");
    }
    else
    {
        std::cout << "skipping small triangle join" << std::endl;
    }
    ////

    std::cout << "done" << std::endl;
    return 0;
}