#ifndef  encodings_hpp_
#define encodings_hpp_

#define		NUM_ENCODING_TYPES_SUPPORTED		4

#define     ENCODING_UNCOMPRESSED 				1
#define     ENCODING_BIT_ALIGNED_COMPRESSED	   	2
#define     ENCODING_BYTE_ALIGNED_BITMAP   		3
#define     ENCODING_HUFFMAN	    			4

using namespace std;

class Encodings
{

private:

    int columnIndex;
    int encoding;

public:

    Encodings(int c, int e)
    {
        columnIndex = c;
        encoding = e;
    }


    int getColumnIndex()
    {
        return columnIndex;
    }

    int getEncoding()
    {
        return encoding;
    }

};

#endif
