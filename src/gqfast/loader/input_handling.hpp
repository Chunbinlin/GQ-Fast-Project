#ifndef input_handling_
#define input_handling_

#include <iostream>
#include <fstream>
#include <dlfcn.h>             // dll functions
#include <utility>             // std::pair
#include "global_vars.hpp"

template <typename T>
class sort_comparator
{
public:
    inline bool operator() (const pair<int, T> & a, const pair<int, T> & b)
    {
        return a.second > b.second;
    }
};

template <typename T>
pair<int, T> * top_k(T* result, int k, int domain)
{

    vector<pair<int, T> > pairs;
    pairs.resize(domain);

    for (int i=0; i < domain; i++)
    {
        pairs[i].first = i;
        pairs[i].second = result[i];
    }

    sort(pairs.begin(), pairs.end(), sort_comparator<T>());

    pair<int, T> * result_pairs = new pair<int,T>[k];
    for (int i=0; i<k; i++)
    {
        result_pairs[i].first = 0;
        result_pairs[i].second = 0;
    }

    for (int i = 0; i < k;  i++)
    {
        if (pairs[i].second > 0)
        {
            result_pairs[i].first = pairs[i].first;
            result_pairs[i].second = pairs[i].second;
        }
        else
        {
            result_pairs[i].first = 0;
            result_pairs[i].second = 0;
        }
    }

    return result_pairs;
}



template <typename T>
void write_result_to_file(T* result, int* null_checks, int domain)
{
    vector<pair<int,T> > result_pairs;
    // Select result elements
    for (int i=0; i<domain; i++)
    {
        if (null_checks[i])
        {
            pair<int, T> temp;
            temp.first = i;
            temp.second = result[i];
            result_pairs.push_back(temp);
        }
    }

    // Sort the elements
    sort(result_pairs.begin(), result_pairs.end(), sort_comparator<T>());

    // Write to file
    ofstream myfile;
    myfile.open ("results.csv");
    myfile << result_pairs.size() << "\n";
    for (int i=0; i<result_pairs.size(); i++)
    {
        myfile << result_pairs[i].first << "," << result_pairs[i].second << "\n";
    }
    myfile.close();

}



template <typename T>
void handle_input(string func_name, int r_pos)
{

    int domain_temp = metadata.idx_domains[r_pos][0];
    string filename = "./test_cases/" + func_name + ".so";

    int* cold_checks;
    int* null_checks;
    int count = 0;

    // load the symbol
    cout << "Opening " << filename << "\n";

    void* handle = dlopen(filename.c_str(), RTLD_NOW);
    if (!handle)
    {
        cerr << "Cannot open library: " << dlerror() << '\n';
        return;
    }

    cout << "Loading symbol query_type...\n";
    typedef T* (*query_type)(int **);

    // reset errors
    dlerror();
    query_type query = (query_type) dlsym(handle, func_name.c_str());
    const char *dlsym_error = dlerror();
    if (dlsym_error)
    {
        cerr << "Cannot load symbol 'query_type': " << dlsym_error <<
             '\n';
        dlclose(handle);
        return;
    }

    T* cold_result = query(&cold_checks);
    T* result = query(&null_checks);

    for (int i=0; i<domain_temp; i++)
    {
        if (null_checks[i])
        {
            count++;
            if (count == 1)
            {
                benchmark_t2 = chrono::steady_clock::now();
            }
        }
    }

    // close the library
    cout << "Closing library...\n";
    dlclose(handle);

    chrono::duration<double> time_span = chrono::duration_cast<chrono::duration<double>>(benchmark_t2 - benchmark_t1);
    cout << "Query " << filename << " processed in " << time_span.count() << " seconds.\n\n";


    pair<int, T> * tops_result = top_k(result, 20, domain_temp);

    cout.precision(17);
    for (int i=0; i<20; i++)
    {
        cout << "Position " << tops_result[i].first << ": " << tops_result[i].second << "\n";
    }

    delete[] tops_result;

    write_result_to_file(result, null_checks, domain_temp);

    delete[] result;
    delete[] cold_result;

    delete[] cold_checks;
    delete[] null_checks;


}




#endif
