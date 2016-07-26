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
#include <algorithm>

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

    set<uint32_t>::iterator it_first;
    set<uint32_t>::iterator it_last;


    cerr << "Num docs = " << doc_ids.size() << "\n";
    it_first = doc_ids.begin();
    it_last = doc_ids.end();
    it_last--;
    cerr << "Docs range from " << *it_first << " to " << *it_last << "\n";

    cerr << "Num term = " << term_ids.size() << "\n";
    it_first = term_ids.begin();
    it_last = term_ids.end();
    it_last--;
    cerr << "Terms range from " << *it_first << " to " << *it_last << "\n";

    cerr << "Num authors = " << author_ids.size() << "\n";
    it_first = author_ids.begin();
    it_last = author_ids.end();
    it_last--;
    cerr << "Authors range from " << *it_first << " to " << *it_last << "\n";
}

bool pairCompare(const pair<uint32_t,uint32_t>& firstElem, const pair<uint32_t,uint32_t>& secondElem) {
    if (firstElem.first == secondElem.first) {
        return firstElem.second < secondElem.second;
    }

    return firstElem.first < secondElem.first;

}

void generate_edge_file()
{

    cerr << "Generating edge file: building pairs\n";

    vector<pair<uint32_t, uint32_t> > edge_table;

    vector<pair<uint32_t,uint32_t> >::iterator vit = dt1_table.begin();
    for (; vit != dt1_table.end(); ++vit)
    {
        pair<uint32_t, uint32_t> source_pair;
        source_pair.first = vit->first;
        source_pair.second = vit->second;

        edge_table.push_back(source_pair);

        pair<uint32_t, uint32_t> dest_pair;
        dest_pair.first = vit->second;
        dest_pair.second = vit->first;

        edge_table.push_back(dest_pair);



    }

    dt1_table.clear();
    vit = da1_table.begin();
    for (; vit != da1_table.end(); ++vit)
    {
        pair<uint32_t, uint32_t> source_pair;
        source_pair.first = vit->first;
        source_pair.second = vit->second;

        edge_table.push_back(source_pair);

        pair<uint32_t, uint32_t> dest_pair;
        dest_pair.first = vit->second;
        dest_pair.second = vit->first;

        edge_table.push_back(dest_pair);

    }
    da1_table.clear();

    cerr << "Pairs loaded, sorting...\n";
    sort(edge_table.begin(), edge_table.end(), pairCompare);

    cerr << "Sorted. Now writing to file 'edges.csv'\n";

    ofstream my_edges_file("./edges.csv");
    my_edges_file << edge_table.size() << "\n";
    for (auto new_it = edge_table.begin(); new_it != edge_table.end(); new_it++)
    {
        my_edges_file << new_it->first << "," << new_it->second << "\n";
    }

    my_edges_file.close();


    cerr << "Edge file written\n";
}

void generate_node_file()
{

    cerr << "Building node pairs\n";
    vector<pair<uint32_t, uint32_t> > node_table;

    set<uint32_t>::iterator vit = doc_ids.begin();
    for (; vit != doc_ids.end(); ++vit)
    {
        pair<uint32_t, uint32_t> temp;
        temp.first = *vit;
        temp.second = DOC_PROPERTY;

        node_table.push_back(temp);
    }

    vit = term_ids.begin();
    for (; vit != term_ids.end(); ++vit)
    {
        pair<uint32_t, uint32_t> temp;
        temp.first = *vit;
        temp.second = TERM_PROPERTY;

        node_table.push_back(temp);
    }

    vit = author_ids.begin();
    for (; vit != author_ids.end(); ++vit)
    {
        pair<uint32_t, uint32_t> temp;
        temp.first = *vit;
        temp.second = AUTHOR_PROPERTY;

        node_table.push_back(temp);
    }

    cerr << "Pairs loaded, sorting...\n";
    sort(node_table.begin(), node_table.end(), pairCompare);

    cerr << "Sorted. Now writing to file 'nodes.csv'\n";

    ofstream my_nodes_file("./nodes.csv");
    my_nodes_file << node_table.size() << "\n";
    for (auto new_it = node_table.begin(); new_it != node_table.end(); new_it++)
    {
        my_nodes_file << new_it->first << "," << new_it->second << "\n";
    }

    my_nodes_file.close();


    cerr << "Node file written\n";



}




int main (int argc, char** argv)
{
    load_tables(false);

    generate_edge_file();
    generate_node_file();

    return 0;
}
#endif
