#ifndef bitmap_hpp_
#define bitmap_hpp_

#include <vector>

using namespace std;

vector<unsigned char> compression(vector<int> T);

vector<int> decompression(vector<unsigned char> compressed_data);





#endif
