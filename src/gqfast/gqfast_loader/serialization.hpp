#ifndef serialization_
#define serialization_

#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>

#include "gqfast_index.hpp"
#include <fstream>

template <typename T>
void save_index(GqFastIndex<T>* s, const char * filename)
{

    chrono::steady_clock::time_point t1 = chrono::steady_clock::now();
    // make an archive
    ofstream ofs(filename);
    boost::archive::text_oarchive oa(ofs);

    oa << s->domain_size;
    oa << s->num_fragment_data;
    //cerr << "num frag data = " << s[i]->num_fragment_data;
    for (int j=0; j<s->num_fragment_data; j++)
    {
        oa << s->fragment_data_length[j];
    }
    for (int j=0; j<s->num_fragment_data; j++)
    {
        oa << s->huffman_tree_array_size[j];
    }

    oa << s->load_flag;
    oa << *s;

    //oa << metadata;

    chrono::steady_clock::time_point t2 = chrono::steady_clock::now();

    chrono::duration<double> time_span = chrono::duration_cast<chrono::duration<double>>(t2 - t1);

    cout << "Saving indices took " << time_span.count() << " seconds.";
    cout << endl;




}

template <typename T>
void load_index(GqFastIndex<T>* & s, const char * filename)
{
    //cerr << "loading database\n";
    chrono::steady_clock::time_point t1 = chrono::steady_clock::now();
    ifstream ifs(filename);
    boost::archive::text_iarchive ia(ifs);

    uint64_t domain_size = 0;
    int num_fragment_data = 0;
    int load_flag = 0;
    unsigned int* fragment_data_length = new unsigned int[num_fragment_data]();
        //cerr << "loading index " << i << "\n";

    ia >> domain_size;
    ia >> num_fragment_data;

    for (int i=0; i<num_fragment_data; i++)
    {
        ia >> fragment_data_length[i];
    }
    int* huffman_tree_array_size = new int[num_fragment_data]();
    for (int i=0; i<num_fragment_data; i++)
    {
        ia >> huffman_tree_array_size[i];
    }

    ia >> load_flag;

    GqFastIndex<T>* temp_index = new GqFastIndex<T>(domain_size, num_fragment_data, fragment_data_length, huffman_tree_array_size, load_flag);
        //   cerr << "copying index\n";
    ia >> *temp_index;
        //   cerr << "setting index\n";
    s = temp_index;

    //ia >> metadata;

    chrono::steady_clock::time_point t2 = chrono::steady_clock::now();
    /*
    for (int i=0; i<num_loaded_indices; i++)
    {
        init_buffer(i);
    }
    */
    chrono::duration<double> time_span = chrono::duration_cast<chrono::duration<double>>(t2 - t1);

    cout << "Loading indices took " << time_span.count() << " seconds.";
    cout << endl;


}
#endif
