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

// This will dump samples which can be used for analysis of how good the samples
// reported are
struct iid_test_settings
{
    std::string TableA_filename;
    std::string TableB_filename;
    std::string TableC_filename;
    std::string TableD_filename;

    std::string output_prefix;
    std::string input_prefix;

    int AL;
    int AR;

    int BL;
    int BR;

    int CL;
    int CR;

    int DL;
    int DR;

    char delimiter;

    int accept_until_restart;
    int threads;
    int trials;

    bool do_DP;
};

iid_test_settings settings;
std::atomic<int> current_trial;

void parse_args_iid(int argc, char ** argv)
{
    TCLAP::CmdLine cmd("4 table join to test for independence (region--nation--customer--orders)", ' ');

    TCLAP::ValueArg<int> arg_acctToRestart("", "batch", "how many samples should be collected before restarting a trial", false, 10000, "int");
    TCLAP::ValueArg<int> arg_threads("t", "threads", "how many concurrent trials should be allowed", false, 1, "int");
    TCLAP::ValueArg<int> arg_trials("n", "trials", "number of trials to run before exiting the program", false, 100, "int");
    TCLAP::SwitchArg arg_doDP("d", "doDP", "set this to set the experiment to report samples from DP algorithm", cmd, false);

    TCLAP::ValueArg<std::string> arg_outputPrefix("o", "out_prefix", "the prefix of the output files to be written for each trial", false, "output", "string");
    TCLAP::ValueArg<std::string> arg_inputPrefix("i", "in_prefix", "the folder where the source data resides", false, ".", "string"); 


    cmd.add(arg_acctToRestart);
    cmd.add(arg_threads);
    cmd.add(arg_trials);

    cmd.add(arg_outputPrefix);
    cmd.add(arg_inputPrefix);

    cmd.parse(argc, argv);

    settings.output_prefix = arg_outputPrefix.getValue();
    settings.input_prefix = arg_inputPrefix.getValue();

    settings.TableA_filename = settings.input_prefix + "/customer.tbl";
    settings.TableB_filename = settings.input_prefix + "/orders.tbl";
    settings.TableC_filename = settings.input_prefix + "/lineitem_numbered.tbl";
    //settings.TableD_filename = settings.input_prefix + "/orders.tbl";

    settings.AL = 1;
    settings.AR = 1;

    settings.BL = 2;
    settings.BR = 1;

    settings.CL = 1;
    settings.CR = 2;

    settings.DL = 2;
    settings.DR = 1;

    //settings.TableA_filename = settings.input_prefix + "/region.tbl";
    //settings.TableB_filename = settings.input_prefix + "/nation.tbl";
    //settings.TableC_filename = settings.input_prefix + "/customer.tbl";
    //settings.TableD_filename = settings.input_prefix + "/orders.tbl";

    //settings.AL = 1;
    //settings.AR = 1;

    //settings.BL = 3;
    //settings.BR = 1;

    //settings.CL = 4;
    //settings.CR = 1;

    //settings.DL = 2;
    //settings.DR = 1;



    settings.output_prefix = arg_outputPrefix.getValue();
    settings.input_prefix = arg_inputPrefix.getValue();

    settings.accept_until_restart = arg_acctToRestart.getValue();
    settings.threads = arg_threads.getValue();
    settings.trials = arg_trials.getValue();

    settings.do_DP = arg_doDP.getValue();

    settings.delimiter = '|';
}

int jefast_trials(std::shared_ptr<jefastIndexLinear> jefast)
{
    int trials_run = 0;
    char out_file[128];


    while (true)
    {
        int my_work_id = current_trial.fetch_add(1);
        if (my_work_id >= settings.trials)
            return trials_run;

        sprintf(out_file, "%s%05d", settings.output_prefix.c_str(), my_work_id);
        std::ofstream output_file;
        output_file.open(out_file);

        std::vector<int64_t> results;
        std::uniform_int_distribution<weight_t> distribution(0, jefast->GetTotal() - 1);
        std::default_random_engine generator;
        generator.seed(std::chrono::system_clock::now().time_since_epoch().count() * my_work_id);

        for (int i = 0; i < settings.accept_until_restart; ++i)
        {
            jefast->GetJoinNumber(distribution(generator), results);

            // output each item of the tuple
            for (auto &i : results) {
                output_file << i << ' ';
            }
            output_file << '\n';
        }
        ++trials_run;
    }
}

struct table_list
{
    std::shared_ptr<TableGeneric> table1;
    std::shared_ptr<TableGeneric> table2;
    std::shared_ptr<TableGeneric> table3;
    std::shared_ptr<TableGeneric> table4;
    std::shared_ptr<TableGeneric> table5;
};

int adaptive_trials(table_list tables)
{
    int trials_run = 0;
    char out_file[128];

    while (true)
    {
            int my_work_id = current_trial.fetch_add(1);
            if (my_work_id >= settings.trials)
                return trials_run;

            sprintf(out_file, "%s%05d", settings.output_prefix.c_str(), my_work_id);
            std::ofstream output_file;
            output_file.open(out_file);

            std::vector<int> results(3);

            DynamicIndex d_idx(5);
            //d_idx.add_Table(0, tables.table1, ((int64_t)10000) * 10000 * 10000 * 10000);
            // need to use max int64_t, otherwise it overflows
            d_idx.add_Table(0, tables.table1, std::numeric_limits<int64_t>::max());
            d_idx.add_Table(1, tables.table2, ((int64_t)10000) * 10000 * 10000 * 10000);
            d_idx.add_Table(2, tables.table3, ((int64_t)10000) * 10000 * 10000);
            d_idx.add_Table(3, tables.table4, ((int64_t)10000) * 10000);
            d_idx.add_Table(4, tables.table5, (int64_t)10000);
            d_idx.initialize();

            for (int i = 0; i < settings.accept_until_restart;)
            {
                bool good = d_idx.sample_join(results);
                if (good) {
                    ++i;
                    for (auto &i : results) {
                        output_file << i << ' ';
                    }
                    output_file << '\n';
                }
            }
            trials_run++;
    }
}

int main(int argc, char** argv)
{
    parse_args_iid(argc, argv);

    // for Q3
    std::cout << "opening tables" << std::endl;
    std::chrono::steady_clock::time_point load_t1 = std::chrono::steady_clock::now();
    std::shared_ptr<TableGeneric> table1(new TableGeneric(settings.input_prefix + "/customer.tbl", '|', 1, 1));
    std::shared_ptr<TableGeneric> table2(new TableGeneric(settings.input_prefix + "/orders.tbl", '|', 2, 1));
    std::shared_ptr<TableGeneric> table3(new TableGeneric(settings.input_prefix + "/lineitem_numbered.tbl", '|', 1, 2));
    std::chrono::steady_clock::time_point load_t2 = std::chrono::steady_clock::now();

    std::cout << "building indexes" << std::endl;
    std::chrono::steady_clock::time_point build_t1 = std::chrono::steady_clock::now();
    table1->Build_indexes();
    table2->Build_indexes();
    table3->Build_indexes();
    std::chrono::steady_clock::time_point build_t2 = std::chrono::steady_clock::now();

    // for QX
    //std::cout << "opening tables" << std::endl;
    //std::chrono::steady_clock::time_point load_t1 = std::chrono::steady_clock::now();
    //std::shared_ptr<TableGeneric> table1(new TableGeneric(settings.input_prefix + "/nation.tbl", '|', 1, 1));
    //std::shared_ptr<TableGeneric> table2(new TableGeneric(settings.input_prefix + "/supplier.tbl", '|', 4, 4));
    //std::shared_ptr<TableGeneric> table3(new TableGeneric(settings.input_prefix + "/customer.tbl", '|', 4, 1));
    //std::shared_ptr<TableGeneric> table4(new TableGeneric(settings.input_prefix + "/orders.tbl", '|', 2, 1));
    //std::shared_ptr<TableGeneric> table5(new TableGeneric(settings.input_prefix + "/lineitem.tbl", '|', 1, 6));
    //std::chrono::steady_clock::time_point load_t2 = std::chrono::steady_clock::now();

    //std::cout << "building indexes" << std::endl;
    //std::chrono::steady_clock::time_point build_t1 = std::chrono::steady_clock::now();
    //table1->Build_indexes();
    //table2->Build_indexes();
    //table3->Build_indexes();
    //table4->Build_indexes();
    //table5->Build_indexes();
    //std::chrono::steady_clock::time_point build_t2 = std::chrono::steady_clock::now();

    // for triangle query
    //std::cout << "opening tables" << std::endl;
    //std::chrono::steady_clock::time_point load_t1 = std::chrono::steady_clock::now();
    //std::shared_ptr<TableGeneric> table1(new TableGeneric(settings.input_prefix + "/twitter_edges_uniq.txt", "\n", 1, 1));
    //std::shared_ptr<TableGeneric> table2(table1);
    //std::shared_ptr<TableGeneric> table3(table1);
    //// leave available for square join
    ////std::shared_ptr<TableGeneric> table4(table1);
    //std::chrono::steady_clock::time_point load_t2 = std::chrono::steady_clock::now();

    //std::cout << "building indexes" << std::endl;
    //std::chrono::steady_clock::time_point build_t1 = std::chrono::steady_clock::now();
    //table1->Build_indexes();
    //std::chrono::steady_clock::time_point build_t2 = std::chrono::steady_clock::now();


    if (!settings.do_DP)
    {
        table_list my_list;
        my_list.table1 = table1;
        my_list.table2 = table2;
        my_list.table3 = table3;
        //my_list.table4 = table4;
        //my_list.table5 = table5;

        std::vector<std::thread> thread_list;
        current_trial = 0;

        std::cout << "launching threads";
        for (int i = 0; i < settings.threads; ++i)
        {
            thread_list.emplace_back(adaptive_trials, my_list);
        }

        // now we just wait for all threads to finish
        for (int i = 0; i < thread_list.size(); ++i)
        {
            thread_list[i].join();
            std::cout << i << ' ' << std::flush;
        }

    }
    ////////// this is DP solution /////////
    else {
        JefastBuilder jefast_index_builder;
        std::shared_ptr<TableGeneric_encap> table1G(new TableGeneric_encap(table1));
        std::shared_ptr<TableGeneric_encap> table2G(new TableGeneric_encap(table2));
        std::shared_ptr<TableGeneric_encap> table3G(new TableGeneric_encap(table3));
        //std::shared_ptr<TableGeneric_encap> table4G(new TableGeneric_encap(table4));
        //std::shared_ptr<TableGeneric_encap> table5G(new TableGeneric_encap(table5));
        jefast_index_builder.AppendTable(table1G, -1, 1, 0);
        jefast_index_builder.AppendTable(table2G, 0, 1, 1);
        jefast_index_builder.AppendTable(table3G, 0, -1, 2);
        //jefast_index_builder.AppendTable(table4G, 0, -1, 3);
        //jefast_index_builder.AppendTable(table5G, 0, -1, 4);

        auto jefast = jefast_index_builder.Build();
        
        std::vector<std::thread> thread_list;
        current_trial = 0;

        std::cout << "launching threads";
        for (int i = 0; i < settings.threads; ++i)
        {
            thread_list.emplace_back(jefast_trials, jefast);
        }

        // now we just wait for all threads to finish
        for (int i = 0; i < thread_list.size(); ++i)
        {
            thread_list[i].join();
            std::cout << i << ' ' << std::flush;
        }
    }
}
