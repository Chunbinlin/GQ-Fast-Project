#include <iostream>
#include <fstream>

#include "gqfast_loader.hpp"

int main(int argc, char ** argv)
{
    build_index("./pubmed/dt1_mesh.csv", "config.xml");

    GqFastIndex<uint32_t>* s;
    Metadata metadata;
    //load_index(s, "./pubmed/dt1_mesh.csv_08_03_16_13:25:22.txt");

    cerr << "Num encodings = " << metadata.idx_num_encodings << "\n";
    cerr << "Domain = " << metadata.idx_domain << "\n";
    for (int i=0; i<metadata.idx_num_encodings; i++)
    {
        cerr << "Domain for encoded " << i << " = " << metadata.idx_col_domains[i] << "\n";
        cerr << "Min id for col " << i << " = " << metadata.idx_min_col_ids[i] << "\n";
        cerr << "Byte size for encoded " << i << " = " << metadata.idx_cols_byte_sizes[i] << "\n";
    }
    cerr << "Map byte size = " << metadata.idx_map_byte_size << "\n";

    return 0;
}
