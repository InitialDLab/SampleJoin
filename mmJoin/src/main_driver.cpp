#include <iostream>

#include "util/Timer.h"

#include "database/TableRepository.h"

#include "database/TableRegion.h"
#include "database/TableNation.h"
#include "database/TablePartsupp.h"
#include "database/TableSupplier.h"
#include "database/TableRepository.h"
#include "database/TableLineitem.h"

#include "database/jefastBuilder.h"

int main() {
    std::cout << "starting to load data into tables" << std::endl;

    Timer t1;

    t1.start();
    std::shared_ptr<Table> test_Region(new Table_Region(R"(C:\Users\robertc\Documents\code\SamplingJoin\mmJoin\built\data\region.tbl)", 5));
    test_Region->get_key_index(0);
    t1.stop();
    std::cout << "loaded Region table in " << t1.getMilliseconds() << "ms" << std::endl;
    t1.reset();

    t1.start();
    std::shared_ptr<Table> test_Nation(new Table_Nation(R"(C:\Users\robertc\Documents\code\SamplingJoin\mmJoin\built\data\nation.tbl)", 25));
    test_Nation->get_key_index(0);
    test_Nation->get_key_index(2);
    t1.stop();
    std::cout << "loaded Nation table in " << t1.getMilliseconds() << "ms" << std::endl;
    t1.reset();

    t1.start();
    std::shared_ptr<Table> test_Supplier(new Table_Supplier(R"(C:\Users\robertc\Documents\code\SamplingJoin\mmJoin\built\data\supplier.tbl)", 10000));
    test_Supplier->get_key_index(0);
    test_Supplier->get_key_index(3);
    t1.stop();
    std::cout << "loaded Supply table in " << t1.getMilliseconds() << "ms" << std::endl;

    t1.start();
    std::shared_ptr<Table> test_Partsupp(new Table_Partsupp(R"(C:\Users\robertc\Documents\code\SamplingJoin\mmJoin\built\data\partsupp.tbl)", 800000));
    test_Partsupp->get_key_index(0);
    test_Partsupp->get_key_index(1);
    t1.stop();
    std::cout << "loaded Partsupp table in " << t1.getMilliseconds() << "ms" << std::endl;
    t1.reset();

    t1.start();
    std::shared_ptr<Table> test_LineItem(new Table_Lineitem(R"(C:\Users\robertc\Documents\code\SamplingJoin\mmJoin\built\data\lineitem.tbl)", 6001215));
    t1.stop();
    std::cout << "loaded Partsupp table in " << t1.getMilliseconds() << "ms" << std::endl;
    t1.reset();

    std::cout << "Data load finished" << std::endl;

    // do a test join for jefast
    JefastBuilder test1;
    test1.AppendTable(test_Region, -1, 0, 0);
    test1.AppendTable(test_Nation, 2, 0, 1);
    test1.AppendTable(test_Supplier, 3, 0, 2);
    test1.AppendTable(test_Partsupp, 1, -1, 3);

    auto test_built = test1.Build();

    int64_t count = test_built->GetTotal();
    std::vector<int64_t> results;
    for (int i = 0; i < count; ++i)
    {
        test_built->GetJoinNumber(i, results);
        std::cout << i << ":" << results.at(0) << ' ' << results.at(1) << ' ' << results.at(2) << ' ' << results.at(3) << '\n';
    }


  return 0;
}