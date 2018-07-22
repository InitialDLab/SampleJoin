#include <iostream>
#include <string>
#include <chrono>
#include <ratio>

#include "tclap-1.2.1/include/tclap/CmdLine.h"

#include <cstdint>
#include <vector>
#include <fstream>
#include <thread>

#include "database/TableGeneric.h"
#include "database/AdaptiveProbabilityIndex.h"

#include "database/jefastBuilder.h"
#include "database/jefastFilter.h"
#include "database/jefastBuilderWJoinAttribSelection.h"
#include "database/jefastBuilderWNonJoinAttribSelection.h"
#include "database/DynamicIndex.h"

struct adaptive_settings
{
    std::string TableA_filename;
    std::string TableB_filename;
    std::string TableC_filename;

    std::string output_filename;

    int AR;

    int BL;
    int BR;

    int CL;

    char delimiter;
};

adaptive_settings settings;

// this will output a graph that shows how many items are rejected or accepted for
// a particular batch
void formated_output(int total_attemptes, int accepted, int rejected, int width)
{
    printf("%10d:", total_attemptes);
    int reject_to_print = rejected * width / (accepted + rejected);
    for (int i = 0; i < reject_to_print; ++i)
        printf("-");
    for (int i = 0; i < width - reject_to_print; ++i)
        printf("A");
    std::cout << std::endl;
}

void parse_args_play(int argc, char ** argv)
{
    TCLAP::CmdLine cmd("generic 3 table chain join", ' ');

    TCLAP::ValueArg<int> arg_table1R("", "1R", "Column in table 1 to use to join with table 2", true, 1, "int");
    TCLAP::ValueArg<int> arg_table2L("", "2L", "Column in table 2 to use to join with table 1", true, 1, "int");
    TCLAP::ValueArg<int> arg_table2R("", "2R", "Column in table 2 to use to join with table 3", true, 1, "int");
    TCLAP::ValueArg<int> arg_table3L("", "3L", "Column in table 3 to use to join with table 2", true, 1, "int");

    TCLAP::ValueArg<std::string> arg_table1filename("", "table1", "Number of inserts and deletes to perform in modify experiment", true, "data1.txt", "string");
    TCLAP::ValueArg<std::string> arg_table2filename("", "table2", "Number of inserts and deletes to perform in modify experiment", true, "data2.txt", "string");
    TCLAP::ValueArg<std::string> arg_table3filename("", "table3", "Number of inserts and deletes to perform in modify experiment", true, "data3.txt", "string");

    //TCLAP::ValueArg<char> arg_delimiter("d", "delimiter", "delimiter to use to separate columns in the table files", false, '\t', "char");
    TCLAP::ValueArg<char> arg_delimiter("d", "delimiter", "delimiter to use to separate columns in the table files", false, '|', "char");
    //TCLAP::ValueArg<char> arg_delimiter("d", "delimiter", "delimiter to use to separate columns in the table files", false, ' ', "char");

    TCLAP::ValueArg<std::string> arg_outputFile("o", "output", "output file name to dump results", false, "output.txt", "string");

    cmd.add(arg_table1R);
    cmd.add(arg_table2L);
    cmd.add(arg_table2R);
    cmd.add(arg_table3L);

    cmd.add(arg_table1filename);
    cmd.add(arg_table2filename);
    cmd.add(arg_table3filename);

    cmd.add(arg_delimiter);

    cmd.parse(argc, argv);

    settings.AR = arg_table1R.getValue();
    settings.BL = arg_table2L.getValue();
    settings.BR = arg_table2R.getValue();
    settings.CL = arg_table3L.getValue();

    settings.TableA_filename = arg_table1filename.getValue();
    settings.TableB_filename = arg_table2filename.getValue();
    settings.TableC_filename = arg_table3filename.getValue();

    settings.output_filename = arg_outputFile.getValue();

    settings.delimiter = arg_delimiter.getValue();
}

int main(int argc, char** argv)
{
    parse_args_play(argc, argv);

    std::cout << "Opening Tables" << std::endl;

    std::chrono::steady_clock::time_point load_t1 = std::chrono::steady_clock::now();
    std::shared_ptr<TableGeneric> table1(new TableGeneric(settings.TableA_filename, settings.delimiter, settings.AR, settings.AR));
    std::shared_ptr<TableGeneric> table2(new TableGeneric(settings.TableB_filename, settings.delimiter, settings.BL, settings.BR));
    std::shared_ptr<TableGeneric> table3(new TableGeneric(settings.TableC_filename, settings.delimiter, settings.CL, settings.CL)); // we put the 'rhs' of the join as a dummy value.
    //std::shared_ptr<TableGeneric> table1(new TableGeneric(settings.TableA_filename, settings.delimiter, 1, 2));
    //std::shared_ptr<TableGeneric> table2(new TableGeneric(settings.TableB_filename, settings.delimiter, 1, 2));
    //std::shared_ptr<TableGeneric> table3(new TableGeneric(settings.TableC_filename, settings.delimiter, 1, 2)); // we put the 'rhs' of the join as a dummy value.
    std::chrono::steady_clock::time_point load_t2 = std::chrono::steady_clock::now();

    std::cout << "building indexes" << std::endl;

    std::chrono::steady_clock::time_point build_t1 = std::chrono::steady_clock::now();
    table1->Build_indexes();
    table2->Build_indexes();
    table3->Build_indexes();
    std::chrono::steady_clock::time_point build_t2 = std::chrono::steady_clock::now();

    //std::cout << "initializing search a" << std::endl;

    std::chrono::steady_clock::time_point inita_t1 = std::chrono::steady_clock::now();
    DynamicIndex d_idx(3);
    d_idx.add_Table(0, table1, ((int64_t) 9000) * 9000L * 9000L);
    d_idx.add_Table(1, table2, 9000L * 9000);
    d_idx.add_Table(2, table3, 9000L);
    d_idx.initialize(1);
    std::chrono::steady_clock::time_point inita_t2 = std::chrono::steady_clock::now();

    std::cout << "warmup" << std::endl;
    d_idx.warmup();

    std::vector<int> data(3);
    int accepteda = 0;
    int rejecteda = 0;

    int batch_sizea = 10000000 / 100;
    int batch_accepteda = 1;
    int batch_rejecteda = 1;
    std::chrono::steady_clock::time_point samplea_t1 = std::chrono::steady_clock::now();

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::ofstream output_file;
    output_file.open(settings.output_filename);

    std::chrono::steady_clock::time_point batcha_t1 = std::chrono::steady_clock::now();


    for (int i = 0; i < 10000000; ++i)
    {
        if (i%batch_sizea == 0)
        {
            std::chrono::steady_clock::time_point batch_t2 = std::chrono::steady_clock::now();
            //formated_output(i, batch_accepted, batch_rejected, 100);
            batch_accepteda = 0;
            batch_rejecteda = 0;

            //std::cout << "^ time " << std::chrono::duration_cast<std::chrono::duration<double>>(batch_t2 - batch_t1).count() << "total: " << std::chrono::duration_cast<std::chrono::duration<double>>(batch_t2 - sample_t1).count() << std::endl;

            std::cout << "accepted: " << accepteda << " rejected: " << rejecteda << " total_time: " << std::chrono::duration_cast<std::chrono::duration<double>>(batch_t2 - samplea_t1).count() << '\n';

            batcha_t1 = batch_t2;
        }

        if (i % (batch_sizea * 20) == 0)
        {
            data[0] = 1234;
            //std::cout << "total accepted: " << accepted << " total rejected: " << rejected << " max bound: " << j_idx.maxAGM() << std::endl;
        }

        if (d_idx.sample_join(data))
        {
            //if (output_file.good())
            //    output_file << data[0] << ' ' << data[1] << ' ' << data[2] << '\n';

            ++accepteda;
            ++batch_accepteda;
            //std::cout << table1->get_column2_value(data[0]) << '-' << table2->get_column1_value(data[1]) << ' ' << table2->get_column2_value(data[1]) << '-' << table3->get_column1_value(data[2]) << std::endl;
        }
        else
        {
            //std::cout << 'R';
            ++batch_rejecteda;
            ++rejecteda;
        }
    }

    ////////////////////////////////////////////////////////////////////////////////

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::cout << "initializing search b" << std::endl;

    std::chrono::steady_clock::time_point init_t1 = std::chrono::steady_clock::now();
    AdaptiveProbabilityIndex j_idx(table1, table2, table3);
    std::chrono::steady_clock::time_point init_t2 = std::chrono::steady_clock::now();

    //std::vector<int> data(3);
    int accepted = 0;
    int rejected = 0;
    int failed = 0;
    std::chrono::steady_clock::time_point sample_t1 = std::chrono::steady_clock::now();

    int batch_size = 100000000 / 100;
    int batch_accepted = 1;
    int batch_rejected = 1;

    //j_idx.test_scan();

    std::chrono::steady_clock::time_point batch_t1 = std::chrono::steady_clock::now();

    //std::ofstream output_file;
    output_file.open(settings.output_filename);

    //for (int i = 0; i < 100000000; ++i)
    //for(int i=0; accepted < 10000000; ++i)
    //{
    //    if (i%batch_size == 0)
    //    {
    //        std::chrono::steady_clock::time_point batch_t2 = std::chrono::steady_clock::now();
    //        //formated_output(i, batch_accepted, batch_rejected, 100);
    //        batch_accepted = 0;
    //        batch_rejected = 0;

    //        //std::cout << "^ time " << std::chrono::duration_cast<std::chrono::duration<double>>(batch_t2 - batch_t1).count() << "total: " << std::chrono::duration_cast<std::chrono::duration<double>>(batch_t2 - sample_t1).count() << std::endl;

    //        std::cout << "accepted: " << accepted << " rejected: " << rejected << " failed: " << failed << " total_time: " << std::chrono::duration_cast<std::chrono::duration<double>>(batch_t2 - sample_t1).count() << '\n';

    //        batch_t1 = batch_t2;
    //    }

    //    if (i % (batch_size * 20) == 0)
    //    {
    //        //std::cout << "total accepted: " << accepted << " total rejected: " << rejected << " max bound: " << j_idx.maxAGM() << std::endl;
    //    }

    //    auto ret_val = j_idx.sample_join(data);
    //    if (ret_val == GOOD_S)
    //    {
    //        //if (output_file.good())
    //            //    output_file << data[0] << ' ' << data[1] << ' ' << data[2] << '\n';

    //        //if (table1->get_column1_value(data[0]) == table3->get_column2_value(data[2]))
    //        {
    //            ++accepted;
    //            ++batch_accepted;
    //        }
    //        //else
    //        //{
    //        //    ++failed;
    //        //}
    //        //std::cout << table1->get_column2_value(data[0]) << '-' << table2->get_column1_value(data[1]) << ' ' << table2->get_column2_value(data[1]) << '-' << table3->get_column1_value(data[2]) << std::endl;
    //    }
    //    else if(ret_val == REJECT_S)
    //    {
    //        ++batch_rejected;
    //        ++rejected;
    //    }
    //    else
    //    {
    //        // fail in this case
    //        ++failed;
    //    }
    //}
    std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

    // build traditional setup
    std::chrono::steady_clock::time_point build_jefast_t1 = std::chrono::steady_clock::now();
    JefastBuilder jefast_index_builder;
    std::shared_ptr<TableGeneric_encap> table1G(new TableGeneric_encap(table1));
    std::shared_ptr<TableGeneric_encap> table2G(new TableGeneric_encap(table2));
    std::shared_ptr<TableGeneric_encap> table3G(new TableGeneric_encap(table3));


    //jefast_index_builder.AppendTable(table1G, -1, settings.AR - 1, 0);
    //jefast_index_builder.AppendTable(table2G, settings.BL - 1, settings.BR - 1, 1);
    //jefast_index_builder.AppendTable(table3G, settings.CL - 1, -1, 2);

    jefast_index_builder.AppendTable(table1G, -1, 1, 0);
    jefast_index_builder.AppendTable(table2G, 0, 1, 1);
    jefast_index_builder.AppendTable(table3G, 0, -1, 2);

    auto jefast = jefast_index_builder.Build();
    std::chrono::steady_clock::time_point build_jefast_t2 = std::chrono::steady_clock::now();

    std::uniform_int_distribution<weight_t> distribution(0, jefast->GetTotal() - 1);
    std::default_random_engine generator;
    std::int64_t DP_triangle_accpt = 0;
    std::vector<int64_t> results;

    std::chrono::steady_clock::time_point sample_jefast_t1 = std::chrono::steady_clock::now();
    // get as many samples as the other technique accepted
    // try to do accept for 10000000
    for (int i = 0; i < accepteda; ++i)
    {
        jefast->GetJoinNumber(distribution(generator), results);
        if (table1->get_column1_value(results[0]) == table3->get_column2_value(results[2]))
            ++DP_triangle_accpt;
    }
    std::chrono::steady_clock::time_point sample_jefast_t2 = std::chrono::steady_clock::now();

    std::cout << "loading data: " << std::chrono::duration_cast<std::chrono::duration<double>>(load_t2 - load_t1).count() << std::endl;
    std::cout << "building indexes: " << std::chrono::duration_cast<std::chrono::duration<double>>(build_t2 - build_t1).count() << std::endl;
    //std::cout << "sampling took " << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count() << std::endl;
    std::cout << "accepted " << accepted << " samples and rejected " << rejected << " samples" << std::endl;
    std::cout << "building jefast: " << std::chrono::duration_cast<std::chrono::duration<double>>(build_jefast_t2 - build_jefast_t1).count() << "seconds" << std::endl;
    std::cout << "triangle accepts for jefast: " << DP_triangle_accpt << " max cardinaltiy for jefast: " << jefast->GetTotal() << std::endl;
    std::cout << "sampling the same number as accepted: " << std::chrono::duration_cast<std::chrono::duration<double>>(sample_jefast_t2 - sample_jefast_t1).count() << "seconds" << std::endl;
    return 0;
}