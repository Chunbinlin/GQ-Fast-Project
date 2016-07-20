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
    cerr << "Table is of size " << table.size();

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

    string line_da1;
    ifstream file_da1("../pubmed/da1.csv");

    uint32_t author_count = 0;
    uint32_t doc_count = 0;
    uint32_t term_count = 0;

    // skip line 1
    getline(file_da1, line_da1);

    unordered_map<uint32_t, int> author_map;
    unordered_map<uint32_t, int> doc_map;
    unordered_map<uint32_t, int> term_map;

    // First read in initial author ids and associated docs
    while (author_count<INITIAL_AUTHOR_IDS)
    {

        getline(file_da1,line_da1);
        stringstream lineStream(line_da1);
        string cell;

        getline(lineStream,cell,',');
        uint32_t current_author =  atoi(cell.c_str());

        if (!author_map[current_author])
        {
            author_map[current_author] = 1;
            author_ids.push_back(current_author);
            author_count++;
        }

        getline(lineStream,cell,',');
        uint32_t current_doc = atoi(cell.c_str());
        if (!doc_map[current_doc])
        {
            doc_map[current_doc] = 1;
            doc_ids.push_back(current_doc);
            doc_count++;
        }
    }
    file_da1.close();

    cerr << "author_count = " << author_count << "\n";
    cerr << "doc_count = " << doc_count << "\n";

    uint32_t old_author_count = 0;

    int passes = 0;
    // Repeat until no new author IDs are added
    while (old_author_count != author_count)
    {
        cerr << "\nPass "  << ++passes << "\n\n";

        old_author_count = author_count;

        cerr << "Reading DT1...\n";
        string line_dt1;
        ifstream file_dt1("../pubmed/dt1_mesh.csv");

        // Skip line 1
        getline(file_dt1, line_dt1);

        uint32_t lines_read_in = 0;
        int percent = 0;


        while (getline(file_dt1, line_dt1))
        {
            lines_read_in++;
            if (lines_read_in % 20709200 == 0)
            {
                percent += 10;
                cerr << percent << "% complete\n";
            }
            stringstream lineStream(line_dt1);
            string cell;

            getline(lineStream,cell,',');
            uint32_t current_doc =  atoi(cell.c_str());

            if (doc_map[current_doc])
            {
                getline(lineStream,cell,',');
                uint32_t current_term = atoi(cell.c_str());
                if (!term_map[current_term] && docs_per_term[current_term]<=LIMIT_DOCS_PER_TERM)
                {
                    term_map[current_term] = 1;
                    term_ids.push_back(current_term);
                    term_count++;
                }
            }
        }
        file_dt1.close();
        cerr << "term_count = " << term_count << "\n";

        cerr << "Reading DT2...\n";
        string line_dt2;
        ifstream file_dt2("../pubmed/dt2_mesh.csv");

        // Skip line 1
        getline(file_dt2, line_dt2);

        lines_read_in = 0;
        percent = 0;

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

            if (term_map[current_term])
            {
                getline(lineStream,cell,',');
                uint32_t current_doc = atoi(cell.c_str());
                if (!doc_map[current_doc])
                {
                    doc_map[current_doc] = 1;
                    doc_ids.push_back(current_doc);
                    doc_count++;
                }
            }
        }
        file_dt2.close();
        cerr << "doc_count = " << doc_count << "\n";

        cerr << "Reading DA2...\n";
        string line_da2;
        ifstream file_da2("../pubmed/da2.csv");

        // Skip line 1
        getline(file_da2, line_da2);

        lines_read_in = 0;
        percent = 0;

        while (getline(file_da2, line_da2))
        {
            lines_read_in++;
            if (lines_read_in % 6131500 == 0)
            {
                percent += 10;
                cerr << percent << "% complete\n";
            }
            stringstream lineStream(line_da2);
            string cell;

            getline(lineStream,cell,',');
            uint32_t current_doc =  atoi(cell.c_str());

            if (doc_map[current_doc])
            {
                getline(lineStream,cell,',');
                uint32_t current_author = atoi(cell.c_str());
                if (!author_map[current_author])
                {
                    author_map[current_author] = 1;
                    author_ids.push_back(current_author);
                    author_count++;
                }
            }
        }
        file_da2.close();
        cerr << "New author_count = " << author_count << "\n";

    }

    cerr << "\nCompleted read-in " << passes << " passes...\n";
    cerr << "Final author count = " << author_count << "\n";
    cerr << "Final doc count = " << doc_count << "\n";
    cerr << "Final term count = " << term_count << "\n";

}

int main (int argc, char** argv)
{
    load_table(da1_table, "../pubmed/da1.csv");
    load_table(dt1_table, "../pubmed/dt1_mesh.csv");
    //count_docs_per_term();
    //get_pubmed_ids();


    return 0;
}












#endif
