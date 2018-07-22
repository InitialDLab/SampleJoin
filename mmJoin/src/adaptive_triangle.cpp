#include <iostream>
#include <string>
#include <chrono>
#include <ratio>

#include "tclap-1.2.1/include/tclap/CmdLine.h"

#include <cstdint>
#include <vector>
#include <fstream>

#include "database/TableGeneric.h"
#include "database/AdaptiveProbabilityIndexTriangle.h"


struct adaptive_settings
{
    std::string TableA_filename;
    std::string TableB_filename;
    std::string TableC_filename;

    std::string output_filename;

    int AL;
    int AR;

    int BL;
    int BR;

    int CL;
    int CR;

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

void  parse_args_tri(int argc, char ** argv)
{
    TCLAP::CmdLine cmd("generic 3 table chain join", ' ');

    TCLAP::ValueArg<int> arg_table1L("", "1L", "Column in table 1 to use to join with table 3", true, 1, "int");
    TCLAP::ValueArg<int> arg_table1R("", "1R", "Column in table 1 to use to join with table 2", true, 1, "int");
    TCLAP::ValueArg<int> arg_table2L("", "2L", "Column in table 2 to use to join with table 1", true, 1, "int");
    TCLAP::ValueArg<int> arg_table2R("", "2R", "Column in table 2 to use to join with table 3", true, 1, "int");
    TCLAP::ValueArg<int> arg_table3L("", "3L", "Column in table 3 to use to join with table 2", true, 1, "int");
    TCLAP::ValueArg<int> arg_table3R("", "3R", "Column in table 3 to use to join with table 1", true, 1, "int");

    TCLAP::ValueArg<std::string> arg_table1filename("", "table1", "Number of inserts and deletes to perform in modify experiment", true, "data1.txt", "string");
    TCLAP::ValueArg<std::string> arg_table2filename("", "table2", "Number of inserts and deletes to perform in modify experiment", true, "data2.txt", "string");
    TCLAP::ValueArg<std::string> arg_table3filename("", "table3", "Number of inserts and deletes to perform in modify experiment", true, "data3.txt", "string");

    TCLAP::ValueArg<char> arg_delimiter("d", "delimiter", "delimiter to use to separate columns in the table files", false, '\t', "char");

    TCLAP::ValueArg<std::string> arg_outputFile("o", "output", "output file name to dump results", false, "output.txt", "string");

    cmd.add(arg_table1L);
    cmd.add(arg_table1R);
    cmd.add(arg_table2L);
    cmd.add(arg_table2R);
    cmd.add(arg_table3L);
    cmd.add(arg_table3R);

    cmd.add(arg_table1filename);
    cmd.add(arg_table2filename);
    cmd.add(arg_table3filename);

    cmd.add(arg_delimiter);

    cmd.parse(argc, argv);

    settings.AL = arg_table1L.getValue();
    settings.AR = arg_table1R.getValue();
    settings.BL = arg_table2L.getValue();
    settings.BR = arg_table2R.getValue();
    settings.CL = arg_table3L.getValue();
    settings.CR = arg_table3R.getValue();

    settings.TableA_filename = arg_table1filename.getValue();
    settings.TableB_filename = arg_table2filename.getValue();
    settings.TableC_filename = arg_table3filename.getValue();

    settings.output_filename = arg_outputFile.getValue();

    settings.delimiter = arg_delimiter.getValue();
}

int main(int argc, char** argv)
{
    parse_args_tri(argc, argv);

    std::cout << "Opening Tables" << std::endl;

    std::chrono::steady_clock::time_point load_t1 = std::chrono::steady_clock::now();
    std::shared_ptr<TableGeneric> table1(new TableGeneric(settings.TableA_filename, settings.delimiter, settings.AL, settings.AR));
    std::shared_ptr<TableGeneric> table2(new TableGeneric(settings.TableB_filename, settings.delimiter, settings.BL, settings.BR));
    std::shared_ptr<TableGeneric> table3(new TableGeneric(settings.TableC_filename, settings.delimiter, settings.CL, settings.CR));
    std::chrono::steady_clock::time_point load_t2 = std::chrono::steady_clock::now();

    std::cout << "building indexes" << std::endl;

    std::chrono::steady_clock::time_point build_t1 = std::chrono::steady_clock::now();
    table1->Build_indexes();
    table2->Build_indexes();
    table3->Build_indexes();
    std::chrono::steady_clock::time_point build_t2 = std::chrono::steady_clock::now();

    std::cout << "initializing search" << std::endl;

    std::chrono::steady_clock::time_point init_t1 = std::chrono::steady_clock::now();
    AdaptiveProbabilityIndexTriangle j_idx(table1, table2, table3);
    std::chrono::steady_clock::time_point init_t2 = std::chrono::steady_clock::now();

    std::vector<int> data(3);
    int accepted = 0;
    int rejected = 0;
    std::chrono::steady_clock::time_point sample_t1 = std::chrono::steady_clock::now();

    int batch_size = 100000;
    int batch_accepted = 1;
    int batch_rejected = 1;

    //j_idx.test_scan();

    std::chrono::steady_clock::time_point batch_t1 = std::chrono::steady_clock::now();

    std::ofstream output_file;
    output_file.open(settings.output_filename);

    for (int i = 0; i < 40000000; ++i)
    {
        if (i%batch_size == 0)
        {
            std::chrono::steady_clock::time_point batch_t2 = std::chrono::steady_clock::now();
            formated_output(i, batch_accepted, batch_rejected, 100);
            batch_accepted = 0;
            batch_rejected = 0;

            std::cout << "^ time " << std::chrono::duration_cast<std::chrono::duration<double>>(batch_t2 - batch_t1).count() << "total: " << std::chrono::duration_cast<std::chrono::duration<double>>(batch_t2 - sample_t1).count() << std::endl;

            batch_t1 = batch_t2;
        }

        if (i % (batch_size) == 0)
        {
            std::cout << "total accepted: " << accepted << " total rejected: " << rejected << " max bound: " << j_idx.maxAGM() << std::endl;
        }

        if (j_idx.sample_join(data))
        {
            if (output_file.good())
                output_file << data[0] << ' ' << data[1] << ' ' << data[2] << '\n';

            ++accepted;
            ++batch_accepted;
            //std::cout << "1.A=" << table1->get_column1_value(data[0]) << " 1.B=" << table1->get_column2_value(data[0]) 
            //    << '-' << "2.B=" << table2->get_column1_value(data[1]) << " 2.C=" << table2->get_column2_value(data[1]) 
            //    << '-' << "3.C=" << table3->get_column1_value(data[2]) << " 3.A=" << table3->get_column2_value(data[2]) << std::endl;
        }
        else
        {
            //std::cout << 'R';
            ++batch_rejected;
            ++rejected;
        }
    }
    std::chrono::steady_clock::time_point sample_t2 = std::chrono::steady_clock::now();

    std::cout << "loading data: " << std::chrono::duration_cast<std::chrono::duration<double>>(load_t2 - load_t1).count() << std::endl;
    std::cout << "building indexes: " << std::chrono::duration_cast<std::chrono::duration<double>>(build_t2 - build_t1).count() << std::endl;
    std::cout << "sampling took " << std::chrono::duration_cast<std::chrono::duration<double>>(sample_t2 - sample_t1).count() << std::endl;
    std::cout << "accepted " << accepted << " samples and rejected " << rejected << " samples" << std::endl;
    return 0;
}