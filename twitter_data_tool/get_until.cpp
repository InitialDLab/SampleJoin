#include <iostream>
#include <fstream>
#include <string>
#include <cstdlib>
using namespace std;

int main(int argc, char *argv[]) {
    if (argc < 3) {
        cout << "usage: " << argv[0] << " <n> <outfile>" << endl;
        return -1;
    }
    
    int n = atoi(argv[1]);
    const char *outfile = argv[2];

    ifstream fin("twitter_rv.net");
    ofstream fout(outfile);
    string line; 

    while (getline(fin, line), !line.empty()) {
        size_t pos;
        int x = stoi(line, &pos);
        if (x > n) {
            break;
        }
        int y = atoi(line.c_str() + pos + 1);
        if (y <= n) {
            fout << line << endl;
        }
    }
    
    fout.close();
    fin.close();

    return 0;
}

