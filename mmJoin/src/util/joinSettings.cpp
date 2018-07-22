#include "joinSettings.h"

#include <algorithm>
#include <random>

global_settings parse_args(int argc, char ** argv)
{
    TCLAP::CmdLine cmd("Query 0 Experiments", ' ');

    TCLAP::SwitchArg arg_hash               ("", "hashJoin"           , "Perform a hash join for query"                    , cmd, false);
    TCLAP::SwitchArg arg_jefastSam          ("", "jefastSample"       , "Perform a jefast sample for query"                , cmd, false);
    TCLAP::SwitchArg arg_olkenSam           ("", "olkenSample"        , "Perform an olken sample for query"                , cmd, false);
    TCLAP::SwitchArg arg_threading          ("", "threading"          , "Perform threading experiment for jefast"          , cmd, false);
    TCLAP::SwitchArg arg_jefastRate         ("", "jefastRate"         , "Perform sample rate experiment for jefast"        , cmd, false);
    TCLAP::SwitchArg arg_olkenRate          ("", "olkenRate"          , "Perform sample rate experiment for olken sampling", cmd, false);
    TCLAP::SwitchArg arg_joinAttribHash     ("", "joinAttribHash"     , "Perform hash join on join attributes"             , cmd, false);
    TCLAP::SwitchArg arg_joinAttribJefast   ("", "joinAttribJefast"   , "Perform jefast sample on join attributes"         , cmd, false);
    TCLAP::SwitchArg arg_nonJoinAttribHash  ("", "nonJoinAttribHash"  , "Perform hash join on non join attributes"         , cmd, false);
    TCLAP::SwitchArg arg_nonJoinAttribJefast("", "nonJoinAttribJefast", "Perform jefast on non join attributes"            , cmd, false);
    TCLAP::SwitchArg arg_jefastModify       ("", "jefastModify"       , "Perform jefast insert and delete experiment"      , cmd, false);

    TCLAP::ValueArg<int> arg_jefastModifyCount("", "jefastModifyCount", "Number of inserts and deletes to perform in modify experiment", true, 10000, "int");

    global_settings settings;

    cmd.parse(argc, argv);

    settings.hashJoin            = arg_hash.getValue();
    settings.jefastSample        = arg_jefastSam.getValue();
    settings.olkenSample         = arg_olkenSam.getValue();
    settings.threading           = arg_threading.getValue();
    settings.jefastRate          = arg_jefastRate.getValue();
    settings.OlkenRate           = arg_olkenRate.getValue();
    settings.joinAttribHash      = arg_joinAttribHash.getValue();
    settings.joinAttribJefast    = arg_joinAttribJefast.getValue();
    settings.nonJoinAttribHash   = arg_nonJoinAttribHash.getValue();
    settings.nonJoinAttribJefast = arg_nonJoinAttribJefast.getValue();
    settings.jefastModify        = arg_jefastModify.getValue();

    settings.modifyCount         = arg_jefastModifyCount.getValue();

    settings.buildJefast = settings.jefastRate 
                        || settings.jefastSample
                        || settings.jefastModify
                        || settings.joinAttribJefast
                        || settings.nonJoinAttribJefast
                        || settings.olkenSample
                        || settings.OlkenRate
                        || settings.threading;

    settings.buildIndex = settings.nonJoinAttribHash || settings.nonJoinAttribJefast
                       || settings.olkenSample
                       || settings.joinAttribHash;

#ifdef WIN32
    settings.null_file_name = "nul";
#else
    settings.null_file_name = "/dev/null";
#endif

    return settings;
}

std::vector<int64_t> generate_unique_random_numbers(size_t count, int64_t min, int64_t max)
{
    std::vector<int64_t> to_ret;

    to_ret.resize(count);
    std::uniform_int_distribution<int64_t> distribution(min, max - 1);
    static std::default_random_engine generator;

    auto ptr = to_ret.begin();

    while (ptr != to_ret.end())
    {
        // generate
        std::generate(ptr, to_ret.end(), [&]() {return distribution(generator);});

        // sort all
        std::sort(to_ret.begin(), to_ret.end());

        // remove duplicates
        ptr = std::unique(to_ret.begin(), to_ret.end());
    }

    return to_ret;
}
