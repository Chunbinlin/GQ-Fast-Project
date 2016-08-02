#include <iostream>
#include <fstream>

#include "gqfast_loader.hpp"

int main(int argc, char ** argv)
{
    build_index("./pubmed/da1.csv", "config.xml");
    return 0;
}
