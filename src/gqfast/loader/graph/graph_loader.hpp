#ifndef graph_loader_
#define graph_loader_

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
            if (lines_read_in == 1)
            {
                min_column_ids[counter] = current;
            }
            else if (min_column_ids[counter] > current)
            {
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
    cerr << "..." << lines_read_in << " lines read in.\n";

}

template <typename T>
void init_dictionary(vector<T> * input_file, dictionary* dict, Encodings* encoding, uint64_t max_column_ids[], uint64_t min_column_ids[])
{

    if (encoding->getEncoding() == ENCODING_BIT_ALIGNED_COMPRESSED)
    {

        uint64_t max = max_column_ids[1];
        uint64_t min = min_column_ids[1];
        uint64_t domain = max - min + 1;
        // Create the dictionary
        uint64_t offset = min-1;

        // Create the dictionary
        dict = new dictionary(domain, offset);

        cerr << "Dictionary created\n";
        cerr << dict->bits_info[0] << " bits needed to encode a value\n";

    }
    else
    {
        dict = new dictionary(1, 0);
    }

}

template <typename T>
void init_huffman_structures(vector<T> * input_file, Encodings* encoding, Node<T>* & huffman_tree, int* & huffman_tree_array, bool* & huffman_terminator_array,
                             encoding_dict<T>* & encoding_dictionary, int & huffman_tree_size)
{

    if (encoding->getEncoding() == ENCODING_HUFFMAN)
    {

        cerr << "Huffman: encoded column is of size " << input_file[1].size() << "\n";

        generate_array_tree_representation(input_file[1], input_file[1].size(), huffman_tree_array,
                                           huffman_tree_size, huffman_terminator_array, huffman_tree);

        encoding_dictionary = new encoding_dict<T>();
        build_inverse_mapping(huffman_tree, *encoding_dictionary);

        cerr << "...Huffman encoding complete\n";
    }
    else
    {
        // Encoding of column is not Huffman
        huffman_tree = nullptr;
        huffman_tree_array = new int[1]();
        huffman_terminator_array = new bool[1]();
        huffman_tree_size = 1;
        encoding_dictionary = nullptr;
    }
}



template <typename T>
void process_old_value(vector<T> & keys, vector<uint32_t> & key_counts, uint32_t bit_count,
                       vector<uint32_t> & byte_count, vector<T> & fragment_to_encode, uint32_t key_counter,
                       encoding_dict<T>* & encoding_dictionary, T oldValue, Encodings* encoding,
                       vector<unsigned char*> & huffman_column, vector<unsigned char *> & bitmap_column,
                       dictionary* dict)
{

    // Update with old key
    keys.push_back(oldValue);
    key_counts.push_back(key_counter);

    // Additional work for compression of columns

    // Calculate bytes for bit-aligned compression
    if (encoding->getEncoding() == ENCODING_BIT_ALIGNED_COMPRESSED)
    {
        uint32_t bytes = ceil((float)bit_count * key_counter / 8);
        byte_count.push_back(bytes);
    }
    // Huffman fragment encoding is done here to preserve byte-alignment between fragments
    // encode() is found in the file "huffman.hpp"
    else if (encoding->getEncoding() == ENCODING_HUFFMAN)
    {
        uint32_t bytes = 0;
        unsigned char* compressed_terms = encode(fragment_to_encode, key_counter, *encoding_dictionary,
                                          bytes);

        huffman_column.push_back(compressed_terms);
        byte_count.push_back(bytes);
        fragment_to_encode.clear();

    }
    // encode_bitmap() in byte_aligned_bitmap.hpp
    else if (encoding->getEncoding() == ENCODING_BYTE_ALIGNED_BITMAP)
    {
        uint32_t bytes = 0;
        encode_bitmap(fragment_to_encode, bitmap_column, bytes);
        byte_count.push_back(bytes);
        fragment_to_encode.clear();
    }

}



template <typename TValue, typename TIndexMap>
void assign_data_uncompressed(unordered_map<TIndexMap, unsigned char*> & index_map,
        vector<TValue> & keys, vector<uint32_t> & key_counts, vector<TValue> & column)
{

    uint64_t column_iterator = 0;

    for (uint32_t i=0; i<keys.size(); i++)
    {
        TValue curr_key = keys[i];
        uint32_t fragment_size = key_counts[i];

        unsigned char* fragment = new unsigned char[sizeof(uint32_t) + fragment_size];

        uint32_t* ptr_to_fragment_size = (uint32_t *) &(fragment[0]);
        *ptr_to_fragment_size = fragment_size;

        uint32_t fragment_index = sizeof(uint32_t);
        for (uint64_t j=column_iterator; j<column_iterator+fragment_size; j++)
        {
            TValue* frag_value = (TValue *) &(fragment[fragment_index]);
            *frag_value = column[j];
            fragment_index += sizeof(TValue);
        }

        column_iterator += fragment_size;

        index_map[curr_key] = fragment;

    }

}

template <typename TValue, typename TIndexMap>
void assign_data_dictionary(unordered_map<TIndexMap, unsigned char*> & index_map, vector<uint32_t> & byte_count,
        vector<TValue> & keys, vector<uint32_t> & key_counts, vector<TValue> & column,
                            dictionary* dict)
{

    uint64_t column_iterator = 0;
    int bits_size = dict->bits_info[0];
    uint64_t offset = dict->offset;

    for (uint32_t i=0; i<keys.size(); i++)
    {
        TValue curr_key = keys[i];
        uint32_t fragment_size = key_counts[i];
        uint32_t bytes_needed_for_key = byte_count[i];

        unsigned char* fragment = new unsigned char[sizeof(uint32_t) + bytes_needed_for_key];

        uint32_t* ptr_to_fragment_size = (uint32_t *) &(fragment[0]);
        *ptr_to_fragment_size = fragment_size;

        uint32_t byte_pos = sizeof(uint32_t);
        int bit_pos = 0;

        for (uint64_t j=column_iterator; j<column_iterator+fragment_size; j++)
        {
            uint32_t encoded_val = column[j]-offset;
            int bits_remaining = bits_size;
            while (bits_remaining)
            {
                int empty_bits_in_current_byte = 8 - bit_pos;

                unsigned char emplacer = (unsigned char)encoded_val;
                emplacer = emplacer << bit_pos;
                fragment[byte_pos] |= emplacer;

                if (bits_remaining < empty_bits_in_current_byte)
                {
                    bit_pos += bits_remaining;
                    bits_remaining = 0;
                }
                else if (bits_remaining == empty_bits_in_current_byte)
                {
                    bits_remaining = 0;
                    bit_pos = 0;
                    byte_pos++;
                }
                else
                {
                    bits_remaining -= empty_bits_in_current_byte;
                    bit_pos = 0;
                    byte_pos++;
                    encoded_val = encoded_val >> empty_bits_in_current_byte;
                }
            }

        }

        column_iterator += fragment_size;
        index_map[curr_key] = fragment;
    }

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


int getByteSize(uint64_t domain)
{

    if (domain/0x100000000)
    {
        return INT_8BYTE;
    }
    else if (domain/0x10000)
    {
        return INT_4BYTE;
    }
    else if (domain/0x100)
    {
        return INT_2BYTE;
    }
    else
    {
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
hash_index<TIndexMap>* buildIndex(string filename, Encodings* encoding, int index_id)
{

    vector<TValue> * input_file = new vector<TValue>[2];    // To store table in memory
    uint64_t* max_column_ids = new uint64_t[2]();           // To find table's domain sizes for each column
    uint64_t* min_column_ids = new uint64_t[2]();
    // Reads in file
    read_in_file(input_file, filename, max_column_ids, min_column_ids);

    auto t_start = std::chrono::high_resolution_clock::now();
    // dict[x] stores bits per encoding and decoding integer for column 'x' if 'x' is to be bit-aligned compressed
    dictionary* dict;
    init_dictionary(input_file, dict, encoding, max_column_ids, min_column_ids);

    // Create Huffman trees, decoding arrays, and encoding dictionaries for Huffman encodings
    Node<TValue>* huffman_tree;
    int* huffman_tree_array;
    int huffman_tree_size = 0;
    bool* huffman_terminator_array;
    encoding_dict<TValue>* encoding_dictionary;

    init_huffman_structures(input_file, encoding, huffman_tree, huffman_tree_array,
                            huffman_terminator_array, encoding_dictionary, huffman_tree_size);

    // domain_size (of first column) will specify the size of the index map
    // +1 because we want to access the index at the last value
    uint64_t domain_size = max_column_ids[0] + 1;

    vector<TValue> keys;                         // Each unique key, in order of appearance
    vector<uint32_t> key_counts;            // For each key in table, # of times the key appears

    uint32_t key_counter = 0;               // A temporary holder of the number of times current key has appeared
    uint32_t total_row_count = 0;           // Total row count of the table

    vector<uint32_t> byte_count;   // The number of bytes needed for each key (compressed only)

    vector<unsigned char*> huffman_column;

    vector<TValue> fragment_to_encode;                      // for Huffman or Byte-aligned bitmap

    vector<unsigned char *> bitmap_column;            // for Byte aligned bitmap

    uint32_t bit_count = 0;       // The number of bits per encoding (bit-aligned compressed)
    if (encoding->getEncoding() == ENCODING_BIT_ALIGNED_COMPRESSED)
    {
        bit_count = dict->bits_info[0];
    }


//
// FIRST PASS: Begin processing according to encoding type
//
    cerr << "First pass\n\n";

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
            process_old_value(keys, key_counts, bit_count, byte_count, fragment_to_encode,
                              key_counter, encoding_dictionary, oldValue, encoding, huffman_column, bitmap_column, dict);
            oldValue = currValue;
            // Reset the key counter
            key_counter = 0;
        }

        key_counter++;

        // fragment_to_encode will store a copy of values for a given key when encoding is Huffman or
        // Byte-aligned Bitmap
        next_val = input_file[1][n];
        int curr_encoding = encoding->getEncoding();
        if (curr_encoding == ENCODING_HUFFMAN || curr_encoding == ENCODING_BYTE_ALIGNED_BITMAP)
        {
            fragment_to_encode.push_back(next_val);
        }

    }
// We process the last key, as it was missed by the loop
    process_old_value(keys, key_counts, bit_count, byte_count, fragment_to_encode, key_counter,
                      encoding_dictionary, oldValue, encoding, huffman_column, bitmap_column, dict);

    // Free some of the memory in the input_file that is no longer needed
    input_file[0].clear();
    if  (!(encoding->getEncoding() == ENCODING_BIT_ALIGNED_COMPRESSED) &&
            !(encoding->getEncoding() == ENCODING_UNCOMPRESSED))
    {
        input_file[1].clear();
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
    cerr << "Second pass\n\n";

    unordered_map<TIndexMap, unsigned char*> index_map;

    switch(encoding->getEncoding())
    {

    case ENCODING_UNCOMPRESSED:
    {

        assign_data_uncompressed(index_map, keys, key_counts,
                                 input_file[1]);
        cerr << "UA: index map is of size " << index_map.size() << "\n";
        break;
    }
    case ENCODING_BIT_ALIGNED_COMPRESSED:
    {

        assign_data_dictionary(index_map, byte_count, keys,
                               key_counts, input_file[1], dict);
        cerr << "BCA: index map is of size " << index_map.size() << "\n";
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
        cerr << "creating fragment data of size = " << size_of_current_array[i] << "\n";
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
        cerr << "\nMax size of BB fragment in bytes is " << max_bytes << "\n\n";
        size_of_current_array[i] = sum + 1;

        // Allocate and initialize
        cerr << "Creating fragment data of size = " << size_of_current_array[i] << "\n";
        fragment_data[i] = new unsigned char[size_of_current_array[i]]();

        assign_data_bitmap(fragment_data[i], index_map, domain_size, i, keys, key_counts,
                           byte_count[i], bitmap_column[i]);
        break;
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
    cerr << "\nCompleted " << total_row_count << " row accesses\n";
    cerr << "Number of fragments = " << keys.size() << "\n";
    cerr << "Min fragment size = " << min_size << "\n";
    cerr << "Max fragment size = " << max_size << "\n";
    cerr << "ID of Max = " << max_id << "\n";
    cerr << "Mean fragment size = " << avg_size << "\n";
    cerr << "Standard deviation = " << std_dev << "\n\n";

//Update metadata for encodings
    metadata.idx_max_fragment_sizes[index_id] = max_size;
    metadata.idx_num_encodings[index_id] = num_encodings;
    for (int i=1; i<num_encodings+1; i++)
    {
        metadata.idx_domains[index_id].push_back(max_column_ids[i]+1);
        int bytes_size = getByteSize(max_column_ids[i]+1);
        metadata.idx_cols_byte_sizes[index_id].push_back(bytes_size);
        cerr << "encoding " << i << " has byte size in index " << index_id << " of " << bytes_size << "\n";
    }

    metadata.idx_map_byte_sizes[index_id] = getByteSize(max_column_ids[0]+1);
    cerr << "index " << index_id << " has map byte size of " << metadata.idx_map_byte_sizes[index_id] << "\n";


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
