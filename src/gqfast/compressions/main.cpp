#ifndef main_compressions_
#define main_compressions_

#include <cstdlib>
#include <iostream>
#include <vector>
#include <algorithm>
#include <map>
#include "dictionary.hpp"
#include "bitmap.hpp"

#define TEST_SIZE 2000
#define TEST_DOMAIN 1000000
using namespace std;

int main(int argc, char ** argv)
{
    srand(time(0));
    vector<int> test;
    map<int, int> test_map;

    for (int i=0; i<TEST_SIZE; i++) {
        while (1) {
            int candidate = rand() % TEST_DOMAIN + 1;
            if (!test_map[candidate]) {
                test_map[candidate] = 1;
                test.push_back(candidate);
                break;
            }
        }
    }

    sort(test.begin(), test.end());

    cout << "Num elements testing = " << test.size() << "\n";
    vector<unsigned char> compressed = compression(test, TEST_DOMAIN);

    cout << "Size of compressed data in bytes is: " << compressed.size() << "\n";

    vector<int> decompressed = decompression(compressed, TEST_DOMAIN);

    bool error = false;
    for (int i=0; i<test.size(); i++) {

        if (test[i] != decompressed[i]) {
            cerr << "Error at iteration "<< i << ": test = " << test[i] << " but decompressed = " << decompressed[i] << "\n";
            error = true;
        }
    }

    if (!error) {
        cout << "All elements in decompressed match original\n\n";
    }

    return 0;
}


#endif
