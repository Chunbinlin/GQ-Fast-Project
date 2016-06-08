#ifndef serialization_
#define serialization_

#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>

#include "global_vars.hpp"
#include <fstream>

template <typename T>
void save_index(fastr_index<T>** s, const char * filename)
{

    chrono::steady_clock::time_point t1 = chrono::steady_clock::now();
    // make an archive
    ofstream ofs(filename, ios_base::binary);
    boost::archive::binary_oarchive oa(ofs);

    oa << num_loaded_indices;

    for (int i=0; i<num_loaded_indices; i++)
    {
        oa << s[i]->domain_size;
        oa << s[i]->num_fragment_data;
        //cerr << "num frag data = " << s[i]->num_fragment_data;
        for (int j=0; j<s[i]->num_fragment_data; j++)
        {
            oa << s[i]->fragment_data_length[j];
        }
        for (int j=0; j<s[i]->num_fragment_data; j++)
        {
            oa << s[i]->huffman_tree_array_size[j];
        }

        oa << s[i]->load_flag;
        oa << *(s[i]);

    }

    oa << metadata;

    chrono::steady_clock::time_point t2 = chrono::steady_clock::now();

    chrono::duration<double> time_span = chrono::duration_cast<chrono::duration<double>>(t2 - t1);

    cout << "Saving indices took " << time_span.count() << " seconds.";
    cout << endl;




}

template <typename T>
void load_index(fastr_index<T>** s, const char * filename)
{
    //cerr << "loading database\n";
    chrono::steady_clock::time_point t1 = chrono::steady_clock::now();
    ifstream ifs(filename, ios_base::binary);
    boost::archive::binary_iarchive ia(ifs);
    // read class state from archive
    ia >> num_loaded_indices;
    for (int i=0; i<num_loaded_indices; i++)
    {
        //cerr << "loading index " << i << "\n";
        uint64_t domain_size = 0;
        int num_fragment_data = 0;
        ia >> domain_size;
        ia >> num_fragment_data;
        //    cerr << "num fragment data = " << num_fragment_data << "\n";
        unsigned int* fragment_data_length = new unsigned int[num_fragment_data]();
        for (int j=0; j<num_fragment_data; j++)
        {
            //    cerr << "loading fragment_data_length " << j << "\n";
            ia >> fragment_data_length[j];
        }
        int* huffman_tree_array_size = new int[num_fragment_data]();
        for (int j=0; j<num_fragment_data; j++)
        {
            //    cerr << "loading huff tree array size " << j << "\n";
            ia >> huffman_tree_array_size[j];
        }

        int load_flag = 0;
        ia >> load_flag;
        //   cerr << "creating index\n";
        fastr_index<T>* temp_index = new fastr_index<T>(domain_size, num_fragment_data, fragment_data_length, huffman_tree_array_size, load_flag);
        //   cerr << "copying index\n";
        ia >> *temp_index;
        //   cerr << "setting index\n";
        idx[i] = temp_index;
    }

    ia >> metadata;

    chrono::steady_clock::time_point t2 = chrono::steady_clock::now();
    for (int i=0; i<num_loaded_indices; i++)
    {
        init_buffer(i);
    }

    chrono::duration<double> time_span = chrono::duration_cast<chrono::duration<double>>(t2 - t1);

    cout << "Loading indices took " << time_span.count() << " seconds.";
    cout << endl;


}
#endif
