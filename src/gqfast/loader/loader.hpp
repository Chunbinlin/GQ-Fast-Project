#ifndef loader_
#define loader_

#include <iostream>
#include <string>
#include <sstream>
#include <fstream>
#include <chrono>
#include <cmath>
#include <cstddef>

#include "global_vars.hpp"

#define TERMINATING_BYTES 7

#define CHAR_1BYTE 1
#define INT_2BYTE 2
#define INT_4BYTE 4
#define INT_8BYTE 8


/*  Function:   read_in_file()
*   Input:
*               input_file:         Pointer to the table loaded in memory
*               filename:           Name of file to load table from
*               max_column_ids:     Array that stores the maximum value in each column
*   Output:     None
*   Notes:      input_file will contain the values of the table after function completes
*
*/
template <typename T>
void read_in_file(vector<T> * input_file, string filename, uint64_t max_column_ids[], uint64_t min_column_ids[])
{

    string line;
    ifstream myfile(filename);

    uint64_t lines_read_in = 0;

    // skip line 1
    getline(myfile, line);

    while (getline(myfile,line))
    {
        lines_read_in++;
        stringstream lineStream(line);
        string cell;
        int current;
        int counter = 0;
        while(getline(lineStream,cell,','))
        {
            current =  atoi(cell.c_str());
            input_file[counter].push_back(current);
            if (lines_read_in == 1) {
                min_column_ids[counter] = current;
            }
            else if (min_column_ids[counter] > current) {
                min_column_ids[counter] = current;
            }
            if (max_column_ids[counter] < current)
            {
                max_column_ids[counter] = current;
            }
            counter++;
        }
    }
    myfile.close();
    // cerr << "..." << lines_read_in << " lines read in.\n";

}

/*  Function:   init_dictionaries()
*   Input:
*               input_file:         Pointer to the table that is loaded in memory
*               dict:               An array of pointers to the dictionary for each column
*               encodings:          An array of Encodings that specify the encoding type for each encoded column
*               num_encodings:      The number of columns that are encoded by the loader
*               max_column_ids:     An array of int's that are the domain sizes of each column
*   Output:     None
*   Notes:
*               dict[x] is a pointer to the dictionary for column 'x', or null if the column
*               is not bit-aligned compressed.
*/
template <typename T>
void init_dictionaries(vector<T> * input_file, dictionary** dict, Encodings* encodings[], int num_encodings,
                       uint64_t max_column_ids[], uint64_t min_column_ids[])
{

    for (int i=0; i<num_encodings; i++)
    {
        if (encodings[i]->getEncoding() == ENCODING_BIT_ALIGNED_COMPRESSED)
        {

            uint64_t max = max_column_ids[i+1];
            uint64_t min = min_column_ids[i+1];
            uint64_t domain = max - min + 1;
            // Create the dictionary
            uint64_t offset = min-1;

            // Create the dictionary
            dict[i] = new dictionary(domain, offset);

            // cerr << "Dictionary created on encoding " << i << ":\n";
            // cerr << dict[i]->bits_info[0] << " bits needed to encode a value\n";

        }
        else
        {
            dict[i] = new dictionary(1, 0);
        }
    }
}

/*  Function:   init_huffman_structures()
*   Input:
*               input_file:                 Pointer to the table that is loaded in memory
*               encodings:                  An array of Encodings that specify the encoding type for each encoded
*                                           column
*               num_encodings:              The number of columns that are encoded by the loader
*               huffman_tree:               A reference to a vector of pointers that each point to the root of a
*                                           huffman tree
*               huffman_tree_array:         A reference to an array of pointers to arrays that each contain the huffman
*                                           tree decoding array
*               huffman_terminator_array:   A reference to an array of pointers to arrays that each contain the
*                                           huffman terminator decoding array
*               encoding_dictionary:        A reference to a vector of encoding dictionaries for decoding huffman
*                                           values
*               huffman_tree_sizes:         An array of the sizes in bytes of the huffman_tree_array decoding arrays
*   Output:     None
*   Notes:      This function calls functions located in "huffman.hpp". Upon completion of function, each column
*               that is to be encoded as Huffman will have all the necessary structures loaded into appropriate arrays.
*/
template <typename T>
void init_huffman_structures(vector<T> * input_file, Encodings* encodings[], int num_encodings,
                             vector<Node<T> *> & huffman_tree, T** & huffman_tree_array, bool** & huffman_terminator_array,
                             vector<encoding_dict<T> *> & encoding_dictionary, int huffman_tree_sizes[])
{

    for (int i=0; i<num_encodings; i++)
    {
        if (encodings[i]->getEncoding() == ENCODING_HUFFMAN)
        {

            // cerr << "Huffman: encoded column " << i << " is of size " << input_file[i+1].size() << "\n";

            Node<T> * tree;
            generate_array_tree_representation(input_file[i+1], input_file[i+1].size(), huffman_tree_array[i],
                                               huffman_tree_sizes[i], huffman_terminator_array[i], tree);

            encoding_dict<T> * dict_temp = new encoding_dict<T>();
            build_inverse_mapping(tree, *dict_temp);
            encoding_dictionary[i] = dict_temp;

            huffman_tree[i] = tree;
            // cerr << "...Huffman encoding complete for encoded column " << i << "\n";
        }
        else
        {
            // Encoding of column is not Huffman
            huffman_tree[i] = nullptr;
            huffman_tree_array[i] = new T[1]();
            huffman_terminator_array[i] = new bool[1]();
            huffman_tree_sizes[i] = 1;
            encoding_dictionary[i] = nullptr;
        }
    }
}


/*  Function:   process_old_value()
*   Input:
*               keys:                   A reference to a vector to contain each unique key in the table
*               key_counts:             A reference to a vector that contains the number of occurrences of each key
*               num_encodings:          The number of columns that are encoded by the loader
*               bit_count:              An array that specifies the bits per encoding for bit-aligned compressed
*                                       columns
*               byte_count:             A reference to a vector of vectors of the bytes needed for each compressed
*                                       fragment
*               fragment_to_encode:     A a reference to a vector of vectors that have the values for the current
*                                       fragment
*               key_counter:            The number of occurrences of the current key
*               encoding:dictionary:    A reference to a vector of encoding dictionaries for decoding huffman values
*               oldValue:               The key being processed in this function
*               encodings:              An array of Encodings that specify the encoding type for each encoded column
*               huffman_column          A reference to a vector of vectors that contains the array of the encoded
*                                       Huffman fragments
*               bitmap_column:          A reference to a vector of vectors that contains the array of the encoded
*                                       byte-aligned bitmap fragments
*               dict:                   An array of pointers to the dictionary for each column
*   Output:     None
*   Notes:      This function updates many of its vector parameters
*/
template <typename T>
void process_old_value(vector<T> & keys, vector<uint32_t> & key_counts, int num_encodings, uint32_t bit_count[],
                       vector<vector<uint32_t> > & byte_count, vector<vector<T> > & fragment_to_encode, uint32_t key_counter,
                       vector<encoding_dict<T> *> & encoding_dictionary, T oldValue, Encodings* encodings[],
                       vector<vector<unsigned char*> > & huffman_column, vector<vector<unsigned char *> > & bitmap_column,
                       dictionary ** dict)
{

    // Update with old key
    keys.push_back(oldValue);
    key_counts.push_back(key_counter);

    // Additional work for compression of columns
    for (int i=0; i<num_encodings; i++)
    {

        // Calculate bytes for bit-aligned compression
        if (encodings[i]->getEncoding() == ENCODING_BIT_ALIGNED_COMPRESSED)
        {
            uint32_t bytes = ceil((float)bit_count[i] * key_counter / 8);
            byte_count[i].push_back(bytes);
        }

        // Huffman fragment encoding is done here to preserve byte-alignment between fragments
        // encode() is found in the file "huffman.hpp"
        else if (encodings[i]->getEncoding() == ENCODING_HUFFMAN)
        {

            uint32_t bytes = 0;
            unsigned char* compressed_terms = encode(fragment_to_encode[i], key_counter, *(encoding_dictionary[i]),
                                              bytes);

            huffman_column[i].push_back(compressed_terms);
            byte_count[i].push_back(bytes);

            fragment_to_encode[i].clear();

        }
        // encode_bitmap() in byte_aligned_bitmap.hpp
        else if (encodings[i]->getEncoding() == ENCODING_BYTE_ALIGNED_BITMAP)
        {
            uint32_t bytes = 0;
            encode_bitmap(fragment_to_encode[i], bitmap_column[i], bytes);
            byte_count[i].push_back(bytes);
            fragment_to_encode[i].clear();
        }
    }
}

/*  Function:   assign_data_uncompressed()
*   Input:
*               fragment_column:    The fragment array that will hold the assigned column
*               index_map:          A 2D array that holds the offsets of all key positions in the fragment_column
*               map_size:           The number of positions in the index map
*               curr_col:           The index of the current column in question
*               keys:               A reference to a vector to contain each unique key in the table
*               key_counts:         A reference to a vector that contains the number of occurrences of each key
*               column:             A reference to a vector that contains the table column that will be assigned
*               data_type:          An integer that designates whether the values should be stored with 1 byte or
*                                   4 bytes
*   Output:     None
*   Notes:      Uncompressed array (UA) fragments are assigned here
*/
template <typename TValue, typename TIndexMap>
void assign_data_uncompressed(unsigned char * fragment_column, TIndexMap ** index_map, uint64_t map_size, int curr_col,
                              vector<TValue> & keys, vector<uint32_t> & key_counts, vector<TValue> & column, int data_type)
{

    // Assumes keys are sorted in ascending order
    TIndexMap fragment_col_ptr = 0;
    uint32_t column_iterator = 0;
    uint32_t key_iterator = 0;

    if (data_type == INT_4BYTE)
    {

        for (uint64_t i=0; i<map_size; i++)
        {

            index_map[i][curr_col] = fragment_col_ptr;

            // map position has fragment
            if (i == keys[key_iterator])
            {
                uint32_t curr_count = key_counts[key_iterator];
                for (uint32_t j=column_iterator; j<column_iterator+curr_count; j++)
                {

                    TValue * frag_value = (TValue *) &(fragment_column[fragment_col_ptr]);
                    *frag_value = column[j];
                    fragment_col_ptr += sizeof(TValue);
                }
                column_iterator += curr_count;
                key_iterator++;
            }

        }
        index_map[map_size][curr_col] = fragment_col_ptr;
    }
    else if (data_type == CHAR_1BYTE)
    {

        for (uint64_t i=0; i<map_size; i++)
        {

            index_map[i][curr_col] = fragment_col_ptr;
            // map position has fragment
            if (i == keys[key_iterator])
            {

                uint32_t curr_count = key_counts[key_iterator];
                for (uint32_t j=column_iterator; j<column_iterator+curr_count; j++)
                {
                    fragment_column[fragment_col_ptr++] = (unsigned char) column[j];
                }

                column_iterator += curr_count;
                key_iterator++;
            }

        }
        index_map[map_size][curr_col] = fragment_col_ptr;
    }
}

/*  Function:   assign_data_dictionary()
*   Input:
*               fragment_column:    The fragment array that will hold the assigned column
*               index_map:          A 2D array that holds the offsets of all key positions in the fragment_column
*               map_size:           The number of positions in the index map
*               curr_col:           The index of the current column in question
*               byte_count:         A reference to a vector that holds the size in bytes of each fragment
*               keys:               A reference to a vector to contain each unique key in the table
*               key_counts:         A reference to a vector that contains the number of occurrences of each key
*               column:             A reference to a vector that contains the table column that will be assigned
*               dict:               A pointer to the dictionary for the current column
*   Output:     None
*   Notes:      Bit-aligned compressed fragments are assigned here
*/
template <typename TValue, typename TIndexMap>
void assign_data_dictionary(unsigned char * fragment_column, TIndexMap ** index_map, uint64_t map_size, int curr_col,
                            vector<uint32_t> & byte_count, vector<TValue> & keys, vector<uint32_t> & key_counts, vector<TValue> & column,
                            dictionary * dict)
{

    TIndexMap fragment_col_ptr = 0;      // to maintain the position which we are at in fragment_column
    uint32_t column_iterator = 0;       // to iterate accross column
    uint32_t key_iterator = 0;

    int bits_size = dict->bits_info[0];
    uint64_t offset = dict->offset;

    for (uint64_t i=0; i<map_size; i++)
    {

        index_map[i][curr_col] = fragment_col_ptr;

        if (i == keys[key_iterator])
        {
            uint32_t curr_count = key_counts[key_iterator];

            int bit_pos = 0, byte_pos = 0;

            for (uint32_t j=column_iterator; j<column_iterator+curr_count; j++)
            {

                uint32_t encoded_val = column[j]-offset;

                // Get access to current + next 7 bytes
                uint64_t * column_address = (uint64_t *) &(fragment_column[fragment_col_ptr+byte_pos]);

                encoded_val = encoded_val << bit_pos;

                // Emplaces the value
                *column_address |= encoded_val;

                // Move the bit and byte pointers
                byte_pos += (bit_pos+bits_size)/8;
                bit_pos = (bit_pos + bits_size) % 8;

            }

            // Move the pointers
            fragment_col_ptr += byte_count[key_iterator];
            column_iterator += curr_count;
            key_iterator++;
        }
    }
    index_map[map_size][curr_col] = fragment_col_ptr;
}

/*  Function:   assign_data_huffma()
*   Input:
*               fragment_column:    The fragment array that will hold the assigned column
*               index_map:          A 2D array that holds the offsets of all key positions in the fragment_column
*               map_size:           The number of positions in the index map
*               curr_col:           The index of the current column in question
*               keys:               A reference to a vector to contain each unique key in the table
*               key_counts:         A reference to a vector that contains the number of occurrences of each key
*               byte_count:         A reference to a vector that holds the size in bytes of each fragment
*               huffman_column:     A reference to a vector that contains the already encoded fragments that will
*                                   be assigned to fragment_column
*   Output:     None
*   Notes:      Huffman fragments are assigned here
*/
template <typename TValue, typename TIndexMap>
void assign_data_huffman(unsigned char * fragment_column, TIndexMap ** index_map, uint64_t map_size, int curr_col,
                         vector<TValue> & keys, vector<uint32_t> & key_counts, vector<uint32_t> & byte_count,
                         vector<unsigned char*> & huffman_column)
{

    TIndexMap fragment_col_ptr = 0;
    uint32_t key_iterator = 0;

    for (uint64_t i=0; i<map_size; i++)
    {
        index_map[i][curr_col] = fragment_col_ptr;

        if (i == keys[key_iterator])
        {
            uint32_t curr_count = key_counts[key_iterator];

            unsigned char * my_char_ptr = huffman_column[key_iterator];

            for (uint32_t j=0; j<byte_count[key_iterator]; j++)
            {
                fragment_column[fragment_col_ptr++] = my_char_ptr[j];
            }

            key_iterator++;
        }

    }
    index_map[map_size][curr_col] = fragment_col_ptr;
    // Delete previously copied char arrays
    for (int i=0; i<huffman_column.size(); i++)
    {
        delete[] huffman_column[i];
    }

}

/*  Function:   assign_data_bitmap()
*   Input:
*               fragment_column:    The fragment array that will hold the assigned column
*               index_map:          A 2D array that holds the offsets of all key positions in the fragment_column
*               map_size:           The number of positions in the index map
*               curr_col:           The index of the current column in question
*               keys:               A reference to a vector to contain each unique key in the table
*               key_counts:         A reference to a vector that contains the number of occurrences of each key
*               byte_count:         A reference to a vector that holds the size in bytes of each fragment
*               bitmap_column:      A reference to a vector that contains the already encoded fragments that will
*                                   be assigned to fragment_column
*   Output:     None
*   Notes:      Byte-aligned bitmap (BB) fragments are assigned here
*/
template <typename TValue, typename TIndexMap>
void assign_data_bitmap(unsigned char * fragment_column, TIndexMap ** index_map, uint64_t map_size, int curr_col,
                        vector<TValue> & keys, vector<uint32_t> & key_counts, vector<uint32_t> & byte_count,
                        vector<unsigned char *> & bitmap_column)
{

    TIndexMap fragment_col_ptr = 0;
    uint32_t key_iterator = 0;

    for (uint64_t i=0; i<map_size; i++)
    {

        index_map[i][curr_col] = fragment_col_ptr;


        if (i == keys[key_iterator])
        {
            uint32_t curr_count = key_counts[key_iterator];
            // Assign values to the fragment column
            // Simple because encoding has already been achieved
            unsigned char * my_char_ptr = bitmap_column[key_iterator];

            for (int j=0; j<byte_count[key_iterator]; j++)
            {
                fragment_column[fragment_col_ptr] = my_char_ptr[j];
                fragment_col_ptr++;
            }

            key_iterator++;
        }

    }
    index_map[map_size][curr_col] = fragment_col_ptr;
    // Delete copied unsigned char arrays
    for (int i=0; i<bitmap_column.size(); i++)
    {
        delete[] bitmap_column[i];
    }

}


int getByteSize(uint64_t domain) {

    if (domain/0x100000000) {
        return INT_8BYTE;
    }
    else if (domain/0x10000) {
        return INT_4BYTE;
    }
    else if (domain/0x100) {
        return INT_2BYTE;
    }
    else {
        return CHAR_1BYTE;
    }
}

/*  Function:   buildIndex()
*   Input:
*               filename:           Name of file to load table from
*               encodings:          An array of Encodings that specify the encoding type for each encoded column
*               num_encodings:      The number of columns that are encoded by the loader
*   Output:     A pointer to the newly built fastr_index
*   Notes:      None
*/
template <typename TValue, typename TIndexMap>
fastr_index<TIndexMap> * buildIndex(string filename, Encodings* encodings[], int num_encodings, int index_id)
{

    //vector<int> & domains, int & max_frag_size) {

    // cerr << "\n...Begin loading file " << filename << "\n";


    vector<TValue> * input_file = new vector<TValue>[num_encodings+1];    // To store table in memory
    uint64_t* max_column_ids = new uint64_t[num_encodings+1]();           // To find table's domain sizes for each column
    uint64_t* min_column_ids = new uint64_t[num_encodings+1]();
    // Reads in file
    read_in_file(input_file, filename, max_column_ids, min_column_ids);

    auto t_start = std::chrono::high_resolution_clock::now();
    // dict[x] stores bits per encoding and decoding integer for column 'x' if 'x' is to be bit-aligned compressed
    dictionary** dict = new dictionary*[num_encodings];
    init_dictionaries(input_file, dict, encodings, num_encodings, max_column_ids, min_column_ids);

    // Create Huffman trees, decoding arrays, and encoding dictionaries for Huffman encodings
    vector<Node<TValue> *> huffman_tree;
    huffman_tree.resize(num_encodings);
    TValue ** huffman_tree_array = new TValue*[num_encodings];
    int* huffman_tree_sizes = new int[num_encodings]();
    bool ** huffman_terminator_array = new bool*[num_encodings];

    vector<encoding_dict<TValue> *> encoding_dictionary;
    encoding_dictionary.resize(num_encodings);
    init_huffman_structures(input_file, encodings, num_encodings, huffman_tree, huffman_tree_array,
                            huffman_terminator_array, encoding_dictionary, huffman_tree_sizes);

    // domain_size (of first column) will specify the size of the index map
    // +1 because we want to access the index at the last value
    uint64_t domain_size = max_column_ids[0] + 1;




    vector<TValue> keys;                         // Each unique key, in order of appearance
    vector<uint32_t> key_counts;            // For each key in table, # of times the key appears

    uint32_t key_counter = 0;               // A temporary holder of the number of times current key has appeared
    uint32_t total_row_count = 0;           // Total row count of the table

    vector<vector<uint32_t> > byte_count;   // The number of bytes needed for each key (compressed only)
    byte_count.resize(num_encodings);

    vector<vector<unsigned char*> > huffman_column;
    huffman_column.resize(num_encodings);

    vector<vector<TValue> > fragment_to_encode;                      // for Huffman or Byte-aligned bitmap
    fragment_to_encode.resize(num_encodings);

    vector< vector<unsigned char *> > bitmap_column;            // for Byte aligned bitmap
    bitmap_column.resize(num_encodings);

    uint32_t bit_count[num_encodings];       // The number of bits per encoding (bit-aligned compressed)
    for (int i=0; i<num_encodings; i++)
    {
        // get the bit encoding size
        if (encodings[i]->getEncoding() == ENCODING_BIT_ALIGNED_COMPRESSED)
        {
            bit_count[i] = dict[i]->bits_info[0];
        }
    }


    //
    // FIRST PASS: Begin processing according to encoding type
    //
    // cerr << "First pass\n\n";

    TValue currValue;                        // To keep track of the key currently being processed
    TValue oldValue = input_file[0][0];      // To keep track of when the current key has changed. Init to
    // first key in table.

    int next_val;

    for (uint32_t n=0; n<input_file[0].size(); n++)
    {
        total_row_count++;

        currValue = input_file[0][n];

        // When next key has been reached
        // call process_old_value() to update various related structures
        if (currValue != oldValue)
        {
            process_old_value(keys, key_counts, num_encodings, bit_count, byte_count, fragment_to_encode,
                              key_counter, encoding_dictionary, oldValue, encodings, huffman_column, bitmap_column, dict);
            oldValue = currValue;
            // Reset the key counter
            key_counter = 0;
        }

        key_counter++;

        // fragment_to_encode will store a copy of values for a given key when encoding is Huffman or
        // Byte-aligned Bitmap
        for (int i=0; i<num_encodings; i++)
        {
            next_val = input_file[i+1][n];
            int curr_encoding = encodings[i]->getEncoding();

            if (curr_encoding == ENCODING_HUFFMAN || curr_encoding == ENCODING_BYTE_ALIGNED_BITMAP)
            {
                fragment_to_encode[i].push_back(next_val);
            }

        }

    }
    // We process the last key, as it was missed by the loop
    process_old_value(keys, key_counts, num_encodings, bit_count, byte_count, fragment_to_encode, key_counter,
                      encoding_dictionary, oldValue, encodings, huffman_column, bitmap_column, dict);

    // Free some of the memory in the input_file that is no longer needed
    input_file[0].clear();
    for (int i=0; i<num_encodings; i++)
    {
        if  (!(encodings[i]->getEncoding() == ENCODING_BIT_ALIGNED_COMPRESSED) &&
                !(encodings[i]->getEncoding() == ENCODING_UNCOMPRESSED))
        {
            input_file[i+1].clear();
        }
    }

    // Statistical Measurements
    uint32_t max_size = 0;
    uint32_t max_id;
    for (int i=0; i<key_counts.size(); i++)
    {
        if (key_counts[i] > max_size)
        {
            max_size = key_counts[i];
            max_id = keys[i];
        }
    }

    uint32_t min_size = max_size;
    for (int i=0; i<key_counts.size(); i++)
    {
        if (key_counts[i] < min_size)
        {
            min_size = key_counts[i];
        }
    }

    double avg_size = total_row_count/(double)keys.size();

    double variance = 0;
    for (int i=0; i<key_counts.size(); i++)
    {
        double curr_temp = (double) key_counts[i];
        variance += ((curr_temp-avg_size)*(curr_temp-avg_size));
    }
    variance = variance / key_counts.size();

    double std_dev = sqrt(variance);

    //
    //  SECOND PASS: Allocate fragment arrays, now that sizes are known, and set-up the index map
    //
    // cerr << "Second pass\n\n";

    unsigned char ** fragment_data = new unsigned char *[num_encodings];
    TIndexMap ** index_map = new TIndexMap *[domain_size+1];

    // cerr << "Creating index map of size " << sizeof(TIndexMap) * (domain_size+1) * num_encodings << "\n";
    for (uint64_t i=0; i<domain_size+1; i++)
    {
        index_map[i] = new TIndexMap[num_encodings]();
    }

    uint32_t * size_of_current_array = new uint32_t[num_encodings]();

    // Each fragment (one per key) reserves space to store its size
    for (int i=0; i<num_encodings; i++)
    {

        switch(encodings[i]->getEncoding())
        {

        case ENCODING_UNCOMPRESSED:
        {

            int data_type;
            if (encodings[i]->getColumnName() == "Fre")
            {
                size_of_current_array[i] = total_row_count;
                data_type = CHAR_1BYTE;
                // cerr << "Setting Fre column to 1 byte\n";
            }
            else
            {
                size_of_current_array[i] = sizeof(TValue) * total_row_count;
                data_type = INT_4BYTE;
            }

            // Allocate and initialize
            // cerr << "Creating fragment data of size = " << size_of_current_array[i] << "\n";
            fragment_data[i] = new unsigned char[size_of_current_array[i]]();

            assign_data_uncompressed(fragment_data[i], index_map, domain_size, i, keys, key_counts,
                                     input_file[i+1], data_type);
            break;
        }
        case ENCODING_BIT_ALIGNED_COMPRESSED:
        {

            uint32_t sum = 0;
            for (uint32_t j=0; j<byte_count[i].size(); j++)
            {
                sum += byte_count[i][j];
            }
            // We need TERMINATING_BYTES for read-ahead encoding and decoding
            size_of_current_array[i] = sum + TERMINATING_BYTES;

            // Allocate and initialize
            // cerr << "Creating fragment data of size = " << size_of_current_array[i] << "\n";
            fragment_data[i] = new unsigned char[size_of_current_array[i]]();

            assign_data_dictionary(fragment_data[i], index_map, domain_size, i, byte_count[i], keys,
                                   key_counts, input_file[i+1], dict[i]);
            break;
        }
        case ENCODING_HUFFMAN:
        {

            uint32_t sum = 0;
            for (uint32_t j=0; j<byte_count[i].size(); j++)
            {
                sum += byte_count[i][j];
            }
            size_of_current_array[i] = sum;

            // Allocate and initialize
            // cerr << "creating fragment data of size = " << size_of_current_array[i] << "\n";
            fragment_data[i] = new unsigned char[size_of_current_array[i]]();

            assign_data_huffman(fragment_data[i], index_map, domain_size, i, keys, key_counts,
                                byte_count[i], huffman_column[i]);
            break;
        }
        case ENCODING_BYTE_ALIGNED_BITMAP:
        {
            uint32_t sum = 0;
            int max_bytes = 0;
            for (uint32_t j=0; j<byte_count[i].size(); j++)
            {
                sum += byte_count[i][j];
                if (max_bytes < byte_count[i][j])
                {
                    max_bytes = byte_count[i][j];
                }
            }
            // cerr << "\nMax size of BB fragment in bytes is " << max_bytes << "\n\n";
            size_of_current_array[i] = sum + 1;

            // Allocate and initialize
            // cerr << "Creating fragment data of size = " << size_of_current_array[i] << "\n";
            fragment_data[i] = new unsigned char[size_of_current_array[i]]();

            assign_data_bitmap(fragment_data[i], index_map, domain_size, i, keys, key_counts,
                               byte_count[i], bitmap_column[i]);
            break;
        }
        }
    }


    // Now the input file is definitely no longer necessary
    for (int i=1; i<num_encodings+1; i++)
    {
        input_file[i].clear();
    }
    delete[] input_file;


    // Sets the index to point to the new map
    int * encoding_types = new int[num_encodings];
    for (int i=0; i<num_encodings; i++)
    {
        encoding_types[i] = encodings[i]->getEncoding();
    }

    fastr_index<TIndexMap>* new_index = new fastr_index<TIndexMap>(domain_size, num_encodings, size_of_current_array, encoding_types);
    new_index->set_index_map(index_map);
    new_index->set_data(fragment_data);
    new_index->set_huffman(huffman_tree_array, huffman_terminator_array, huffman_tree_sizes);
    new_index->set_dictionary(dict);
    // cerr << "\nCompleted " << total_row_count << " row accesses\n";
    // cerr << "Number of fragments = " << keys.size() << "\n";
    // cerr << "Min fragment size = " << min_size << "\n";
    // cerr << "Max fragment size = " << max_size << "\n";
    // cerr << "ID of Max = " << max_id << "\n";
    // cerr << "Mean fragment size = " << avg_size << "\n";
    // cerr << "Standard deviation = " << std_dev << "\n\n";

    //Update metadata for encodings
    metadata.idx_max_fragment_sizes[index_id] = max_size;
    metadata.idx_num_encodings[index_id] = num_encodings;
    for (int i=1; i<num_encodings+1; i++)
    {
        metadata.idx_domains[index_id].push_back(max_column_ids[i]+1);
        int bytes_size = getByteSize(max_column_ids[i]+1);
        metadata.idx_cols_byte_sizes[index_id].push_back(bytes_size);
        // cerr << "encoding " << i << " has byte size in index " << index_id << " of " << bytes_size << "\n";
    }

    metadata.idx_map_byte_sizes[index_id] = getByteSize(max_column_ids[0]+1);
    // cerr << "index " << index_id << " has map byte size of " << metadata.idx_map_byte_sizes[index_id] << "\n";


    // Memory clean-up
    for (int i=0; i<num_encodings; i++)
    {
        huffman_column[i].clear();
        fragment_to_encode[i].clear();
        if (huffman_tree[i])
        {
            delete huffman_tree[i];
        }
        if (encoding_dictionary[i])
        {
            for (auto it=encoding_dictionary[i]->begin(); it != encoding_dictionary[i]->end(); ++it)
            {
                if (it->second.bits)
                {
                    delete[] it->second.bits;
                }
            }
            delete encoding_dictionary[i];
        }
    }

    delete[] max_column_ids;
    delete[] min_column_ids;

    huffman_tree.clear();
    encoding_dictionary.clear();

    keys.clear();
    key_counts.clear();

    auto t_cts = std::chrono::high_resolution_clock::now();
    cerr << "Total loading time: "
         << std::chrono::duration<double>(t_cts-t_start).count()
         << " sec\n\n";


    num_loaded_indices++;

    return new_index;
}

#endif
