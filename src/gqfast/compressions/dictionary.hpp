#ifndef dictionary_hpp_
#define dictionary_hpp_

#include <vector>

using namespace std;

vector<unsigned char> compression(vector<int> T, int domain_size);

vector<int> decompression(vector<unsigned char> compressed_data, int domain_size);



#endif
