#include <iostream>
#include <fstream>
#include "load.hpp"
#include "input_handling.hpp"
#include "serialization.hpp"
// threads
int num_threads;

// Clock variables
struct timespec start;
struct timespec finish;

chrono::steady_clock::time_point benchmark_t1;
chrono::steady_clock::time_point benchmark_t2;

// Pre-declared index pointers
fastr_index<uint32_t>* idx[MAX_INDICES];

// Metadata wrapper
Metadata metadata;

// Thread defs
pthread_t threads[MAX_THREADS];
pthread_spinlock_t * spin_locks[MAX_INDICES];


// Buffers
int**** buffer_arrays[MAX_INDICES];


void init_globals()
{

    // Globals are initially null/0
    for (int i=0; i<MAX_INDICES; i++)
    {
        idx[i] = nullptr;
        metadata.idx_max_fragment_sizes[i] = 0;
        metadata.idx_num_encodings[i] = 0;
        buffer_arrays[i] = nullptr;
    }

}


void delete_globals()
{
    cerr << "Deleting globals\n";
    for (int i=0; i<MAX_INDICES; i++)
    {

        if (idx[i])
        {

            int num_encodings = idx[i]->num_fragment_data;
            // Free the associated buffer
            for (int j=0; j<num_encodings; j++)
            {
                delete[] buffer_arrays[i][j];
            }

            delete[] buffer_arrays[i];
            delete idx[i];
            delete[] spin_locks[i];
        }
    }
}


int main(int argc, char ** argv)
{

    init_globals();

    char c;
    char action = 0;
    int database = 1;
    int compression = 1;
    int num_files = 0;
    int save_to_file = 0;

    char* filename = "dummy.bin";

    num_loaded_indices = 0;
    char *help = "usage: %s [-h] [-s] [-l] [-d database] [-c compression] \n\n"
                 "-h\tShow list of parameters.\n\n"
                 "-s\tSave indices to file: filename\n\n"
                 "-l\tLoad indices from file: filename\n\n"
                 "-d\tChoose test database.\n\n"
                 "[1]\tSemmedDB\n"
                 "[2]\tPubmed (mesh only)\n"
                 "[3]\tPubmed (mesh+supp)\n\n"
                 "-c\tCompression type.\n\n"
                 "[1]\tUA\n"
                 "[2]\tBCA\n"
                 "[3]\tBB\n"
                 "[4]\tHuffman\n"
                 "[5]\tOptimal compression\n\n";
    while ((c = getopt(argc, argv, "hs:l:d:c:")) != -1)
    {
        switch (c)
        {
        case 'h':
            cout << "\n\n" << help << "\n\n";
            exit(0);
            break;
        case 's':
            action = 's';
            if (optarg)
            {
                filename = optarg;
            }
            else
            {
                cout << "error: filename required!\n";
                exit(0);
            }
            break;
        case 'l':
            action = 'l';
            if (optarg)
            {
                filename = optarg;
            }
            else
            {
                cout << "error: filename required!\n";
                exit(0);
            }
            break;
        case 'd':
            if (optarg)
            {
                database = atoi(optarg);
            }
            break;
        case 'c':
            if (optarg)
            {
                compression = atoi(optarg);
            }
            break;
        }
    }

    switch(action)
    {
    case 'l':
    {
        load_index<uint32_t>(idx, filename);
        cout << "\n...Indices have been loaded...";
        char testing = 'y';
        while (testing == 'y')
        {
            string library_file;
            cout << "Please enter the name of the library file you are loading (don't include the .so extension; path is assumed to be current dir): \n";
            cin >> library_file;
            string result_domain_string;
            cout << "Please specify the index ID for the result domain: \n";
            cin >> result_domain_string;
            int result_domain = atoi(result_domain_string.c_str());
            char result_data_type;
            cout << "Please specify one of these result data types: (d)ouble, (i)nt \n";
            cin >> result_data_type;
            if (result_data_type == 'd')
            {
                handle_input<double>(library_file, result_domain);
            }
            else if (result_data_type == 'i')
            {
                handle_input<int>(library_file, result_domain);
            }

            cout << "\nLoad another file(y/n)?\n";
            cin >> testing;
        }
        break;
    }
    case 's':
    {
        int result = load<int, uint32_t>(database, compression);
        if (result)
        {
            cerr << "Indices loaded properly\n";
            //save_index<uint32_t>(idx, filename);
        }
        else
        {
            cerr << "Error: load function did not terminate normally\n\n";
        }
        break;
    }
    default:
    {
        int result = load<int, uint32_t>(database, compression);
        if (result)
        {
            cout << "\n...Indices have been loaded...";
            char testing = 'y';
            while (testing == 'y')
            {
                string library_file;
                cout << "Please enter the name of the library file you are loading (don't include the .so extension; path is assumed to be current dir): \n";
                cin >> library_file;
                string result_domain_string;
                cout << "Please specify the index ID for the result domain: \n";
                cin >> result_domain_string;
                int result_domain = atoi(result_domain_string.c_str());
                char result_data_type;
                cout << "Please specify one of these result data types: (d)ouble, (i)nt \n";
                cin >> result_data_type;
                cin >> result_data_type;
                if (result_data_type == 'd')
                {
                    handle_input<double>(library_file, result_domain);
                }
                else if (result_data_type == 'i')
                {
                    handle_input<int>(library_file, result_domain);
                }
                cout << "\nLoad another file(y/n)?\n";
                cin >> testing;
            }

        }
        else
        {
            cerr << "Error: load function did not terminate normally\n\n";
        }
        break;
    }

    }

    delete_globals();
    return 0;
}
