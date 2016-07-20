#ifndef test_db_maker_
#define test_db_maker_

#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <set>
#include <algorithm>
#include <utility>
#include <unordered_map>

#define INITIAL_AUTHOR_IDS 200000
#define LIMIT_DOCS_PER_TERM 100
#define MAX_FAN_OUT 250
#define INITIAL_CONCEPT_IDS 50000

using namespace std;

vector<pair<uint32_t, uint32_t> > da1_table;
vector<pair<pair<uint32_t, uint32_t>, int> > dt1_table;

set<uint32_t> author_ids;
set<uint32_t> doc_ids;
set<uint32_t> term_ids;

unordered_map<uint32_t, int> new_author_id_mapping;
unordered_map<uint32_t, int> new_doc_id_mapping;
unordered_map<uint32_t, int> new_term_id_mapping;

//unordered_map <uint32_t, int> docs_per_term;

set<uint32_t> concept_ids;
set<uint32_t> concept_semtype_ids;
set<uint32_t> predication_ids;
set<uint32_t> sentence_ids;

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

void load_table(vector<pair<pair<uint32_t, uint32_t>, int> > & table, string filename)
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
        getline(lineStream,cell,',');
        int third = atoi(cell.c_str());

        pair<pair<uint32_t, uint32_t>, int> current_triple;
        current_triple.first = current_pair;
        current_triple.second = third;
        table.push_back(current_triple);
    }

    myfile.close();

    cerr << "Lines read-in: " << lines_read_in << "\n";
    cerr << "Table is of size " << table.size() << "\n";

}

/*
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
*/

void get_pubmed_ids()
{
    // First read in initial author ids and associated docs
    vector<pair<uint32_t, uint32_t> >::iterator da_it = da1_table.begin();

    while (author_ids.size() <INITIAL_AUTHOR_IDS)
    {
        pair<uint32_t, uint32_t> current_pair = *da_it++;

        uint32_t current_author = current_pair.first;
        author_ids.emplace(current_author);
        uint32_t current_doc = current_pair.second;
        doc_ids.emplace(current_doc);


    }

    cerr << "author_count = " << author_ids.size() << "\n";
    cerr << "doc_count = " << doc_ids.size() << "\n";

    // Find associated terms
    cerr << "Scanning DT1...\n";

    for (auto dt1_it = dt1_table.begin(); dt1_it != dt1_table.end(); dt1_it++)
    {
        pair<uint32_t, uint32_t> current_pair = dt1_it->first;
        uint32_t current_doc = current_pair.first;

        if (doc_ids.find(current_doc) != doc_ids.end())
        {
            uint32_t current_term = current_pair.second;
            term_ids.emplace(current_term);
        }
    }

    cerr << "term_count = " << term_ids.size() << "\n";

    // Prune term set
    unordered_map<uint32_t, int> docs_per_term;

    cerr << "Scanning DT2...\n";

    for (auto dt2_it = dt1_table.begin(); dt2_it != dt1_table.end(); dt2_it++)
    {
        pair<uint32_t, uint32_t> current_pair = dt2_it->first;
        uint32_t current_term = current_pair.second;

        if (term_ids.find(current_term) != term_ids.end())
        {
            uint32_t current_doc = current_pair.first;
            if (doc_ids.find(current_doc) != doc_ids.end())
            {
                docs_per_term[current_term]++;
            }
        }
    }

    for (auto term_it = docs_per_term.begin(); term_it != docs_per_term.end(); term_it++)
    {
        uint32_t current_term = term_it->first;
        int current_doc_count = term_it->second;

        if (current_doc_count < 2)
        {
            term_ids.erase(current_term);
        }
    }

    cerr << "pruned term_count = " << term_ids.size() << "\n";

}


void map_new_ids(set<uint32_t> & ids, unordered_map<uint32_t, int> & new_id_map)
{
    int curr_id = 1;
    for (auto it = ids.begin(); it != ids.end(); it++)
    {
        new_id_map[*it] = curr_id++;
    }

}

bool tripleCompare(const pair<pair<uint32_t,uint32_t>,int>& firstElem, const pair<pair<uint32_t,uint32_t>,int> & secondElem) {
    if (firstElem.first.first == secondElem.first.first) {
        return firstElem.first.second < secondElem.first.second;
    }

    return firstElem.first.first < secondElem.first.first;

}


bool pairCompare(const pair<uint32_t,uint32_t>& firstElem, const pair<uint32_t,uint32_t>& secondElem) {
    if (firstElem.first == secondElem.first) {
        return firstElem.second < secondElem.second;
    }

    return firstElem.first < secondElem.first;

}

void generate_test_pubmed()
{

    cerr << "Creating test_da tables\n";

    vector<pair<uint32_t, uint32_t> > new_da1_table;
    vector<pair<uint32_t, uint32_t> > new_da2_table;

    for (auto da1_it = da1_table.begin(); da1_it != da1_table.end(); da1_it++)
    {
        uint32_t current_author = da1_it->first;
        uint32_t current_doc = da1_it->second;

        if (new_author_id_mapping[current_author] && new_doc_id_mapping[current_doc])
        {
            pair<uint32_t, uint32_t> current_da1_pair;
            current_da1_pair.first = new_author_id_mapping[current_author];
            current_da1_pair.second = new_doc_id_mapping[current_doc];

            new_da1_table.push_back(current_da1_pair);

            pair<uint32_t, uint32_t> current_da2_pair;
            current_da2_pair.first = new_doc_id_mapping[current_doc];
            current_da2_pair.second = new_author_id_mapping[current_author];
            new_da2_table.push_back(current_da2_pair);
        }

    }

    sort(new_da1_table.begin(), new_da1_table.end(), pairCompare);
    sort(new_da2_table.begin(), new_da2_table.end(), pairCompare);

    cerr << "Writing Test DA1 to file, size = " << new_da1_table.size() << "\n";
    ofstream my_da1_outfile("./test_da1.csv");
    my_da1_outfile << new_da1_table.size() << "\n";
    for (auto new_da1_it = new_da1_table.begin(); new_da1_it != new_da1_table.end(); new_da1_it++)
    {
        my_da1_outfile << new_da1_it->first << "," << new_da1_it->second << "\n";
    }

    my_da1_outfile.close();
    new_da1_table.clear();

    cerr << "Writing Test DA2 to file, size = " << new_da2_table.size() << "\n";
    ofstream my_da2_outfile("./test_da2.csv");
    my_da2_outfile << new_da2_table.size() << "\n";
    for (auto new_da2_it = new_da2_table.begin(); new_da2_it != new_da2_table.end(); new_da2_it++)
    {
        my_da2_outfile << new_da2_it->first << "," << new_da2_it->second << "\n";
    }

    my_da2_outfile.close();
    new_da2_table.clear();


    cerr << "Creating test_dt tables\n";

    vector<pair<pair<uint32_t, uint32_t>, int> > new_dt1_table;
    vector<pair<pair<uint32_t, uint32_t>, int> > new_dt2_table;

    for (auto dt1_it = dt1_table.begin(); dt1_it != dt1_table.end(); dt1_it++)
    {
        uint32_t current_doc = dt1_it->first.first;
        uint32_t current_term = dt1_it->first.second;
        uint32_t current_fre = dt1_it->second;

        if (new_doc_id_mapping[current_doc] && new_term_id_mapping[current_term])
        {
            pair<uint32_t, uint32_t> current_dt1_pair;
            current_dt1_pair.first = new_doc_id_mapping[current_doc];
            current_dt1_pair.second = new_term_id_mapping[current_term];

            pair<pair<uint32_t, uint32_t>, int> current_dt1_triple;
            current_dt1_triple.first = current_dt1_pair;
            current_dt1_triple.second = current_fre;

            new_dt1_table.push_back(current_dt1_triple);

            pair<uint32_t, uint32_t> current_dt2_pair;
            current_dt2_pair.second = new_doc_id_mapping[current_doc];
            current_dt2_pair.first = new_term_id_mapping[current_term];

            pair<pair<uint32_t, uint32_t>, int> current_dt2_triple;
            current_dt2_triple.first = current_dt2_pair;
            current_dt2_triple.second = current_fre;

            new_dt2_table.push_back(current_dt2_triple);
        }

    }

    sort(new_dt1_table.begin(), new_dt1_table.end(), tripleCompare);
    sort(new_dt2_table.begin(), new_dt2_table.end(), tripleCompare);

    cerr << "Writing Test DT1 to file, size = " << new_dt1_table.size() << "\n";
    ofstream my_dt1_outfile("./test_dt1.csv");
    my_dt1_outfile << new_dt1_table.size() << "\n";
    for (auto new_dt1_it = new_dt1_table.begin(); new_dt1_it != new_dt1_table.end(); new_dt1_it++)
    {
        my_dt1_outfile << new_dt1_it->first.first << "," << new_dt1_it->first.second << "," << new_dt1_it->second << "\n";
    }

    my_dt1_outfile.close();
    new_dt1_table.clear();

    cerr << "Writing Test DT2 to file, size = " << new_dt2_table.size() << "\n";
    ofstream my_dt2_outfile("./test_dt2.csv");
    my_dt2_outfile << new_dt2_table.size() << "\n";
    for (auto new_dt2_it = new_dt2_table.begin(); new_dt2_it != new_dt2_table.end(); new_dt2_it++)
    {
         my_dt2_outfile << new_dt2_it->first.first << "," << new_dt2_it->first.second << "," << new_dt2_it->second << "\n";
    }

    my_dt2_outfile.close();
    new_dt2_table.clear();

    cerr << "Creating test_dy table\n";
    vector<pair<int, int> > new_dy_table;

    cerr << "Loading original dy\n";
    string line;
    ifstream myfile("../pubmed/dy.csv");

    // skip line 1
    getline(myfile, line);
    while (getline(myfile,line))
    {
        stringstream lineStream(line);
        string cell;

        getline(lineStream,cell,',');
        uint32_t current_doc =  atoi(cell.c_str());
        if (new_doc_id_mapping[current_doc])
        {
            int new_doc_id = new_doc_id_mapping[current_doc];
            getline(lineStream,cell,',');
            int current_year = atoi(cell.c_str());

            pair<int, int> current_pair;
            current_pair.first = new_doc_id;
            current_pair.second = current_year;

            new_dy_table.push_back(current_pair);
        }

    }
    myfile.close();

    cerr << "Writing Test DY to file, size = " << new_dy_table.size() << "\n";
    ofstream my_dy_outfile("./test_dy.csv");
    my_dy_outfile << new_dy_table.size() << "\n";
    for (auto new_dy_it = new_dy_table.begin(); new_dy_it != new_dy_table.end(); new_dy_it++)
    {
        my_dy_outfile << new_dy_it->first << "," << new_dy_it->second << "\n";
    }

    my_dy_outfile.close();
    new_dy_table.clear();


}

/*
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
}*/

int main (int argc, char** argv)
{
    load_table(da1_table, "../pubmed/da1.csv");
    load_table(dt1_table, "../pubmed/dt1_tag.csv");

    get_pubmed_ids();
    map_new_ids(author_ids, new_author_id_mapping);
    map_new_ids(doc_ids, new_doc_id_mapping);
    map_new_ids(term_ids, new_term_id_mapping);

    generate_test_pubmed();


    return 0;
}












#endif
