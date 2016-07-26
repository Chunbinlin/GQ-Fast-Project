#ifndef make_graph_
#define make_graph_

#define MAX_DOC_ID 23326299
#define MAX_TERM_ID_MESH 223705
#define MAX_TERM_ID_TAG 247030

#define DOC_PROPERTY 1
#define TERM_PROPERTY 2
#define AUTHOR_PROPERTY 3

#include <iostream>
#include <utility>
#include <vector>
#include <set>
#include <sstream>
#include <fstream>

using namespace std;


vector<pair<uint32_t, uint32_t> > da1_table;
vector<pair<uint32_t, uint32_t> > dt1_table;

set<uint32_t> doc_ids;
set<uint32_t> term_ids;
set<uint32_t> author_ids;

void load_tables(bool tag_flag)
{
    cerr << "Loading dt1 table\n";
    string line;
    ifstream myfile;

    if (tag_flag)
    {
        myfile.open("../pubmed/dt1_tag.csv");
    }
    else
    {
        myfile.open("../pubmed/dt1_mesh.csv");
    }
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

        current_pair.second = atoi(cell.c_str()) + MAX_DOC_ID;
        dt1_table.push_back(current_pair);

        doc_ids.emplace(current_pair.first);
        term_ids.emplace(current_pair.second);
    }

    myfile.close();

    cerr << "Lines read-in dt1: " << lines_read_in << "\n";
    cerr << "DT Table is of size " << dt1_table.size() << "\n";

    myfile.open("../pubmed/da1.csv");
    lines_read_in = 0;


    // skip line 1
    getline(myfile, line);

    while (getline(myfile,line))
    {
        lines_read_in++;
        stringstream lineStream(line);
        string cell;

        pair<uint32_t, uint32_t> current_pair;
        getline(lineStream,cell,',');

        if (tag_flag) {
            current_pair.first = atoi(cell.c_str()) + MAX_DOC_ID + MAX_TERM_ID_TAG;

        }
        else {
            current_pair.first = atoi(cell.c_str()) + MAX_DOC_ID + MAX_TERM_ID_MESH;
        }
        getline(lineStream,cell,',');
        current_pair.second = atoi(cell.c_str());
        da1_table.push_back(current_pair);

        author_ids.emplace(current_pair.first);
        doc_ids.emplace(current_pair.second);
    }

    myfile.close();

    cerr << "Lines read-in da1: " << lines_read_in << "\n";
    cerr << "DA Table is of size " << da1_table.size() << "\n";

    cerr << "Num docs = " << doc_ids.size();
    cerr << "Num term = " << term_ids.size();
    cerr << "Num authors = " << author_ids.size();

}

int main (int argc, char** argv)
{
    load_tables(false);


    return 0;
}
#endif
