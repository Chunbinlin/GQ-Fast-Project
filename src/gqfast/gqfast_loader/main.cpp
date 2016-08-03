#include <iostream>
#include <fstream>

#include "gqfast_loader.hpp"

int main(int argc, char ** argv)
{
    build_index("./pubmed/dt1_mesh.csv", "config.xml");

    return 0;
}
