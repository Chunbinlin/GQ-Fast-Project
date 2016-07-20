#ifndef test_db_maker_
#define test_db_maker_

#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <algorithm>
#include <utility>
#include <unordered_map>

#define INITIAL_AUTHOR_IDS 100
#define LIMIT_DOCS_PER_TERM 100
#define MAX_FAN_OUT 250
#define INITIAL_CONCEPT_IDS 50000

using namespace std;

vector<pair<uint32_t, uint32_t> > da1_table;
vector<pair<uint32_t, uint32_t> > dt1_table;

vector<uint32_t> author_ids;
vector<uint32_t> doc_ids;
vector<uint32_t> term_ids;

unordered_map <uint32_t, int> docs_per_term;

vector<uint32_t> concept_ids;
vector<uint32_t> concept_semtype_ids;
vector<uint32_t> predication_ids;
vector<uint32_t> sentence_ids;

void load_table(vector<pair<uint32_t, uint32_t> > & table, string filename)
{
    cerr << "Loading table from " << filename << "\n";
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

        pair<uint32_t, uint32_t> current_pair;
        getline(lineStream,cell,',');
        current_pair.first =  atoi(cell.c_str());
        getline(lineStream,cell,',');
        current_pair.second = atoi(cell.c_str());

        table.push_back(current_pair);
    }

    myfile.close();

    cerr << "Lines read-in: " << lines_read_in << "\n";
    cerr << "Table is of size " << table.size() << "\n";

}



void count_docs_per_term()
{

    cerr << "Reading DT2...\n";
    string line_dt2;
    ifstream file_dt2("../pubmed/dt2_mesh.csv");

    // Skip line 1
    getline(file_dt2, line_dt2);

    int lines_read_in = 0;
    int percent = 0;
    int term_count = 0;

    while (getline(file_dt2, line_dt2))
    {
        lines_read_in++;
        if (lines_read_in % 20709200 == 0)
        {
            percent += 10;
            cerr << percent << "% complete\n";
        }
        stringstream lineStream(line_dt2);
        string cell;

        getline(lineStream,cell,',');
        uint32_t current_term =  atoi(cell.c_str());

        if (!docs_per_term[current_term]) {
            docs_per_term[current_term] = 1;
            term_count++;
        }
        else{
            docs_per_term[current_term]++;
        }

    }
    file_dt2.close();
    cerr << "term_count = " << term_count << "\n";


}



void get_pubmed_ids()
{

    uint32_t author_count = 0;
    uint32_t doc_count = 0;
    uint32_t term_count = 0;

    unordered_map<uint32_t, int> author_map;
    unordered_map<uint32_t, int> doc_map;
    unordered_map<uint32_t, int> term_map;

    // First read in initial author ids and associated docs
    vector<pair<uint32_t, uint32_t> >::iterator da_it = da1_table.begin();

    while (author_count<INITIAL_AUTHOR_IDS)
    {
        pair<uint32_t, uint32_t> current_pair = *da_it++;

        uint32_t current_author = current_pair.first;

        if (!author_map[current_author])
        {
            author_map[current_author] = 1;
            author_ids.push_back(current_author);
            author_count++;
        }

        uint32_t current_doc = current_pair.second;
        if (!doc_map[current_doc])
        {
            doc_map[current_doc] = 1;
            doc_ids.push_back(current_doc);
            doc_count++;
        }
    }

    cerr << "author_count = " << author_count << "\n";
    cerr << "doc_count = " << doc_count << "\n";

    uint32_t old_author_count = 0;

    int passes = 0;
    // Repeat until no new author IDs are added
    while (old_author_count != author_count)
    {
        cerr << "\nPass "  << ++passes << "\n\n";

        old_author_count = author_count;
        cerr << "Scanning DA1...\n";
        vector<pair<uint32_t, uint32_t> >::iterator da1_it = da1_table.begin();
        int new_ids = 0;
        for (; da1_it != da1_table.end(); da1_it++)
        {
            pair<uint32_t, uint32_t> current_pair = *da1_it;
            uint32_t current_author = current_pair.first;

            if (author_map[current_author])
            {
                uint32_t current_doc = current_pair.second;
                if (!doc_map[current_doc] && new_ids < MAX_FAN_OUT)
                {
                    new_ids++;
                    doc_map[current_doc] = 1;
                    doc_ids.push_back(current_doc);
                    doc_count++;
                }
            }
        }

        cerr << "doc_count = " << doc_count << "\n";

        cerr << "Scanning DT1...\n";

        vector<pair<uint32_t, uint32_t> >::iterator dt1_it = dt1_table.begin();
        new_ids = 0;
        for (; dt1_it != dt1_table.end(); dt1_it++)
        {
            pair<uint32_t, uint32_t> current_pair = *dt1_it;
            uint32_t current_doc = current_pair.first;

            if (doc_map[current_doc])
            {
                uint32_t current_term = current_pair.second;
                if (!term_map[current_term] && docs_per_term[current_term]<=LIMIT_DOCS_PER_TERM && new_ids < MAX_FAN_OUT)
                {
                    new_ids++;
                    term_map[current_term] = 1;
                    term_ids.push_back(current_term);
                    term_count++;
                }
            }
        }

        cerr << "term_count = " << term_count << "\n";

        cerr << "Scanning DT2...\n";
        vector<pair<uint32_t, uint32_t> >::iterator dt2_it = dt1_table.begin();
        new_ids = 0;
        for (; dt2_it != dt1_table.end(); dt2_it++)
        {
            pair<uint32_t, uint32_t> current_pair = *dt2_it;
            uint32_t current_term =  current_pair.second;

            if (term_map[current_term])
            {
                uint32_t current_doc = current_pair.first;
                if (!doc_map[current_doc] && new_ids <MAX_FAN_OUT)
                {
                    new_ids++;
                    doc_map[current_doc] = 1;
                    doc_ids.push_back(current_doc);
                    doc_count++;
                }
            }
        }
        cerr << "doc_count = " << doc_count << "\n";

        cerr << "Scanning DA2...\n";
        vector<pair<uint32_t, uint32_t> >::iterator da2_it = da1_table.begin();
        new_ids = 0;
        for (; da2_it != da1_table.end(); da2_it++)
        {
           pair<uint32_t, uint32_t> current_pair = *da2_it;
           uint32_t current_doc =  current_pair.second;

           if (doc_map[current_doc])
           {
                uint32_t current_author = current_pair.first;
                if (!author_map[current_author] && new_ids < MAX_FAN_OUT)
                {
                    new_ids++;
                    author_map[current_author] = 1;
                    author_ids.push_back(current_author);
                    author_count++;
                }
            }
        }
        cerr << "New author_count = " << author_count << "\n";
    }

    cerr << "\nCompleted read in " << passes << " passes...\n";
    cerr << "Final author count = " << author_count << "\n";
    cerr << "Final doc count = " << doc_count << "\n";
    cerr << "Final term count = " << term_count << "\n";

}

int main (int argc, char** argv)
{
    load_table(da1_table, "../pubmed/da1.csv");
    load_table(dt1_table, "../pubmed/dt1_mesh.csv");
    count_docs_per_term();
    get_pubmed_ids();


    return 0;
}












#endif
