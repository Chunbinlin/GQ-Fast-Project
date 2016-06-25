#ifndef auto_tests_
#define auto_tests_

#include <iostream>
#include <fstream>
#include <sstream>
#include "input_handling.hpp"

void automatic_tests(char* input_file, char* output_file)
{

    string test_file(input_file);
    string output_file_string (output_file);

    cout << "test file = " << test_file << "\n";
    string line;
    ifstream myfile(test_file);
    ofstream outfile(output_file_string);
    uint64_t lines_read_in = 0;

    while (getline(myfile,line))
    {
        lines_read_in++;
        stringstream lineStream(line);
        string cell;
        string func_name;
        int r_pos;
        char output_type;
        int counter = 0;
        chrono::duration<double> time_span;
        chrono::duration<double> time_span2;
        chrono::duration<double> time_span3;
        bool valid = false;
        while(getline(lineStream,cell,','))
        {
            valid = true;
            if (counter == 0)
            {
                func_name = cell;
            }
            else if (counter == 1)
            {
                r_pos = atoi(cell.c_str());
            }
            else if (counter == 2)
            {
                output_type = cell[0];
            }
            counter++;
        }
        if (valid)
        {
            if (output_type == 'i')
            {
                cout << "calling int autohandle with func " << func_name << "and rpos " << r_pos << "\n";
                time_span = auto_handle_input<int>(func_name, r_pos);
            //    time_span2 = auto_handle_input<int>(func_name, r_pos);
            //    time_span3 = auto_handle_input<int>(func_name, r_pos);
            }
            else if (output_type == 'd')
            {
                cout << "calling double autohandle with func " << func_name << "and rpos " << r_pos << "\n";
                time_span = auto_handle_input<double>(func_name, r_pos);
            //    time_span2 = auto_handle_input<double>(func_name, r_pos);
            //    time_span3 = auto_handle_input<double>(func_name, r_pos);
            }

            outfile << func_name << ", " << time_span.count() << " sec ";// << time_span2.count() << " sec, " << time_span3.count() << " sec\n";
        }
    }
    myfile.close();
    outfile.close();
    // cerr << "..." << lines_read_in << " lines read in.\n";

}





#endif
