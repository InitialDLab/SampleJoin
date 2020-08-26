#include <iostream>
#include <string>
#include <chrono>
#include <memory>

#include "tclap-1.2.1/include/tclap/CmdLine.h"

#include "database/Int64CSVTable.h"
#include "database/jefastBuilder.h"
#include "database/jefastIndex.h"

// QA's join size is longer than 64 bits. Need to compile with
// -DUSE_UINT128_WEIGHT. See CMakeLists.txt and
// database/DatabaseSharedTypes.h for details.
static_assert(
    sizeof(weight_t) == 16,
    "tpcds_sample_queries need to be compiled with -DUSE_UINT128_WEIGHT");

double experiment_timeout = 3600.0 * 24;

struct settings_t {
    std::string data_path;
    int scale_factor;
    int trials;
    bool do_QA;
    bool do_QB;
};

settings_t settings;

void parse_settings(int argc, char *argv[]) {
    TCLAP::CmdLine cmd("Some sample queries on TPC-DS data.", ' ');

    TCLAP::ValueArg<std::string> arg_data_path(
        "D", // flag
        "data_path", // name
        "the path to the directory that contains the TPC-DS data. "
        "If empty, it defaults to ./tpcds_${SF}x/", // desc
        false,
        "",
        "string",
        cmd);

    TCLAP::ValueArg<int> arg_scale_factor(
        "S",
        "scale_factor",
        "scale factor to use for the experiments (default: 1). "
        "Not honored if data path is specified.",
        false,
        1,
        "int",
        cmd);

    TCLAP::ValueArg<int> arg_trials(
        "T",
        "trials",
        "number of trials of the queries",
        false,
        1,
        "int",
        cmd);
    
    TCLAP::SwitchArg arg_do_QA(
        "",
        "do_QA",
        "run Query A",
        cmd,
        false);

    TCLAP::SwitchArg arg_do_QB(
        "",
        "do_QB",
        "run Query B",
        cmd,
        false);

    cmd.parse(argc, argv);

    settings.data_path = arg_data_path.getValue();
    settings.scale_factor = arg_scale_factor.getValue();
    settings.trials = arg_trials.getValue();
    settings.do_QA = arg_do_QA.getValue();
    settings.do_QB = arg_do_QB.getValue();

    if (settings.data_path.empty()) {
        settings.data_path = std::string("./tpcds_")
            + std::to_string(settings.scale_factor)
            + "x/";
    } else {
        if (!settings.data_path[settings.data_path.length() - 1] == '/') {
            settings.data_path.push_back('/');
        }
    }
}

// (WS \bowtie_{web_page_sk} WR) \bowtie_{promo_sk}
// (CS \bowtie_{catalog_page_sk} CR) \bowtie_{promo_sk}
// (SS \bowtie_{store_sk} SR)
bool run_QA() {
    std::cout << std::endl << "run_QA()" << std::endl;
    std::cout << "loading data..." << std::endl;

    std::vector<int> sample_count = { 1000, 10000, 100000, 1000000 };
    
    auto web_sales = std::make_shared<Int64CSVTable>();
    if (!web_sales->load(
            settings.data_path + "web_sales.dat", {
                13, /* 0: ws_web_page_sk */
                17, /* 1: ws_promo_sk */
            }, '|')) {
        return false;
    }

    auto web_returns = std::make_shared<Int64CSVTable>();
    if (!web_returns->load(
            settings.data_path + "web_returns.dat", {
                12, /* 0: wr_web_page_sk */
            }, '|')) {
        return false;
    }

    auto catalog_sales = std::make_shared<Int64CSVTable>();
    if (!catalog_sales->load(
            settings.data_path + "catalog_sales.dat", {
                13, /* 0: cs_catalog_page_sk */
                17, /* 1: cs_promo_sk */
            }, '|')) {
        return false;
    }

    auto catalog_returns = std::make_shared<Int64CSVTable>();
    if (!catalog_returns->load(
            settings.data_path + "catalog_returns.dat", { 
                13, /* 0: cr_catalog_page_sk */
            }, '|')) {
        return false;
    }

    auto store_sales = std::make_shared<Int64CSVTable>();
    if (!store_sales->load(
            settings.data_path + "store_sales.dat", {
                8, /* 0: ss_store_sk */
                9, /* 1: ss_promo_sk */
            }, '|')) {
        return false;
    }

    auto store_returns = std::make_shared<Int64CSVTable>();
    if (!store_returns->load(
            settings.data_path + "store_returns.dat", {
                8, /* 0: sr_store_sk */
            }, '|')) {
        return false;
    }

    std::cout << "loading finished" << std::endl << std::endl;
    std::cout << "Table sizes:" << std::endl;
    std::cout << "web_sales = " << web_sales->row_count() << std::endl;
    std::cout << "web_returns = " << web_returns->row_count() << std::endl;
    std::cout << "catalog_sales = " << catalog_sales->row_count() << std::endl;
    std::cout << "catalog_returns = " << catalog_returns->row_count() << std::endl;
    std::cout << "store_sales = " << store_sales->row_count() << std::endl;
    std::cout << "store_returns = " << store_returns->row_count() << std::endl;
    
    std::cout << std::endl << "DP..." << std::endl;
    for (int trial = 0 ; trial < settings.trials; ++trial) {
        auto build_t1 = std::chrono::steady_clock::now();
        JefastBuilder builder;
        int WS = builder.AppendTable(web_sales, -1, 0, 0);
        
        // equivalent to
        // builder.AddTableToFork(
        //  web_returns,
        //  0, /* wr_web_page_sk */
        //  0, /* ws_web_page_sk */
        //  WS
        // );
        int WR = builder.AppendTable(web_returns, 0, -1, 0);
        
        int CS = builder.AddTableToFork(
            catalog_sales,
            1, /* cs_promo_sk */
            1, /* ws_promo_sk */
            WS
        );

        int CR = builder.AddTableToFork(
            catalog_returns,
            0, /* cr_catalog_page_sk */
            0, /* cs_catalog_page_sk */
            CS); 

        int SS = builder.AddTableToFork(
            store_sales,
            1, /* ss_promo_sk */
            1, /* cs_promo_sk */
            CS);

        /* int SR = */ (void) builder.AddTableToFork(
            store_returns,
            0, /* ss_store_sk */
            0, /* sr_store_sk */
            SS);

        auto index = builder.BuildFork();
        auto build_t2 = std::chrono::steady_clock::now();

        if (trial == 0) {
            if (index->GetTotal() == 0) {
                std::cout << "[WARN] empty join" << std::endl;
            } else {
                std::cout << "[INFO] join size = " << index->GetTotal()
                    << std::endl;
            }
            std::cout << "Sample size\tBuild time (s)\tSample time (s)"
                << std::endl;
        }

        std::vector<jefastKey_t> results;
        results.resize(6);
        for (auto samples_to_collect : sample_count) {
            auto sample_t1 = std::chrono::steady_clock::now();
            if (index->GetTotal() > 0) {
                for (int s = samples_to_collect; s--; ) {
                    index->GetRandomJoin(results);
                }
            }
            auto sample_t2 = std::chrono::steady_clock::now();
            std::cout << samples_to_collect << '\t'
                << std::chrono::duration_cast<std::chrono::duration<
                    double>>(build_t2 - build_t1).count() << '\t'
                << std::chrono::duration_cast<std::chrono::duration<
                    double>>(sample_t2 - sample_t1).count() << std::endl;
        }
    }

    return true;
}

// (WR \bowtie_{web_page_sk} WS)
// \bowtie_{(promo_sk, reason_sk)}
// (CR \bowtie_{catalog_page_sk} CS)
bool run_QB() {
    std::cout << std::endl << "run_QB()" << std::endl;
    std::cout << "loading data..." << std::endl;

    std::vector<int> sample_count = { 1000, 10000, 100000, 1000000 };
    
    auto web_sales = std::make_shared<Int64CSVTable>();
    if (!web_sales->load(
            settings.data_path + "web_sales.dat", {
                13, /* 0: ws_web_page_sk */
                17, /* 1: ws_promo_sk */
            }, '|')) {
        return false;
    }

    auto web_returns = std::make_shared<Int64CSVTable>();
    if (!web_returns->load(
            settings.data_path + "web_returns.dat", {
                12, /* 0: wr_web_page_sk */
                13, /* 1: wr_reason_sk */
            }, '|')) {
        return false;
    }

    auto catalog_sales = std::make_shared<Int64CSVTable>();
    if (!catalog_sales->load(
            settings.data_path + "catalog_sales.dat", {
                13, /* 0: cs_catalog_page_sk */
                17, /* 1: cs_promo_sk */
            }, '|')) {
        return false;
    }

    auto catalog_returns = std::make_shared<Int64CSVTable>();
    if (!catalog_returns->load(
            settings.data_path + "catalog_returns.dat", { 
                13, /* 0: cr_catalog_page_sk */
                16, /* 1, cr_reason_sk */
            }, '|')) {
        return false;
    }

    std::cout << "loading finished" << std::endl << std::endl;
    std::cout << "Table sizes:" << std::endl;
    std::cout << "web_sales = " << web_sales->row_count() << std::endl;
    std::cout << "web_returns = " << web_returns->row_count() << std::endl;
    std::cout << "catalog_sales = " << catalog_sales->row_count() << std::endl;
    std::cout << "catalog_returns = " << catalog_returns->row_count() << std::endl;
    
    std::cout << std::endl << "DP..." << std::endl;
    for (int trial = 0 ; trial < settings.trials; ++trial) {
        auto build_t1 = std::chrono::steady_clock::now();
        JefastBuilder builder;
        int WR = builder.AddTableToFork(
            web_returns, -1, -1, -1);
        
        int WS = builder.AddTableToFork(
            web_sales,
            0, /* ws_web_page_sk */
            0, /* wr_web_page_sk */
            WR);
        
        int CS = builder.AddTableToFork(
            catalog_sales,
            1, /* cs_promo_sk */ 
            1, /* ws_promo_sk */
            WS);

        int CR = builder.AddTableToFork(
            catalog_returns,
            0, /* cr_catalog_page_sk */
            0, /* cs_catalog_page_sk */
            CS);

        auto index = builder.BuildFork();
        auto build_t2 = std::chrono::steady_clock::now();

        if (trial == 0) {
            if (index->GetTotal() == 0) {
                std::cout << "[WARN] empty join" << std::endl;
            } else {
                std::cout << "[INFO] join size = " << index->GetTotal()
                    << std::endl;
            }
            std::cout << "Sample size\tBuild time (s)\tSample time (s)"
                << std::endl;
        }

        std::vector<jefastKey_t> results;
        results.resize(6);
        for (auto samples_to_collect : sample_count) {
            auto sample_t1 = std::chrono::steady_clock::now();
            if (index->GetTotal() > 0) {
                for (int s = samples_to_collect; s--; ) {
                    index->GetRandomJoin(results);
                    
                    // filter by wr_reason_sk == cr_reason_sk
                    if (catalog_returns->get_int64(results[CR], 1) !=
                        web_returns->get_int64(results[WR], 1)) {
                        // retry if not passing the filter
                        ++s;
                    }
                }
            }
            auto sample_t2 = std::chrono::steady_clock::now();
            std::cout << samples_to_collect << '\t'
                << std::chrono::duration_cast<std::chrono::duration<
                    double>>(build_t2 - build_t1).count() << '\t'
                << std::chrono::duration_cast<std::chrono::duration<
                    double>>(sample_t2 - sample_t1).count() << std::endl;
        }
    }

    return true;
    return true;
}

int main(int argc, char *argv[]) {
    parse_settings(argc, argv);
    
    std::cout << "data_path = " << settings.data_path << std::endl;
    std::cout << "scale_factor = " << settings.scale_factor << std::endl;
    std::cout << "trials = " << settings.trials << std::endl;

    if (settings.do_QA) {
        if (!run_QA()) {
            std::cout << "[ERROR] something went wrong with QA, "
                "maybe some data file is missing or is mal-formatted"
                << std::endl;
        }
    }

    if (settings.do_QB) {
        if (!run_QB()) {
            std::cout << "[ERROR] something went wrong with QB, "
                "maybe some data file is missing or is mal-formatted"
                << std::endl;
        }
    }

    return 0;
}
