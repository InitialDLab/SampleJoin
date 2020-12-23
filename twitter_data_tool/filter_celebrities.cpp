#include <iostream>
#include <fstream>
#include <string>
#include <cstdlib>
#include <unordered_set>
using namespace std;

int main(int argc, char *argv[]) {
    
    if (argc < 3) {
        cout << "usage: " << argv[0] << " <infile> <outfile>" << endl;
        return 0;
    }

    const char *infile = argv[1];
    const char *outfile = argv[2];

    unordered_set<int> c(8000);
    ifstream cfin("celebrities_id.txt");
    string line;
    while (getline(cfin, line), !line.empty()) {
        int id = stoi(line);
        c.insert(id);
    }
    cfin.close();

    cout << "dict read" << endl;

    ifstream fin(infile);
    ofstream fout(outfile);
    
    while (getline(fin, line), !line.empty()) {
        int x = stoi(line);
        if (c.find(x) != c.end()) {
            fout << line << endl;
        }
    }

    fout.close();
    fin.close();

    return 0;
}
