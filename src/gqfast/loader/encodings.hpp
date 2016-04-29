#ifndef  encodings_hpp_
#define encodings_hpp_


#include <iostream>
#include <string>

#define		NUM_ENCODING_TYPES_SUPPORTED		4

#define     ENCODING_UNCOMPRESSED 				1
#define     ENCODING_BIT_ALIGNED_COMPRESSED	   	2
#define     ENCODING_BYTE_ALIGNED_BITMAP   		3
#define     ENCODING_HUFFMAN	    			4

using namespace std;

class Encodings
{

private:

    string columnName;
    int encoding;
public:

    Encodings(string c, int e);

    string getColumnName();

    void setColumnName(string c);

    int getEncoding();

    void setEncoding(int e);

};

#endif