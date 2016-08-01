#ifndef auto_tests_
#define auto_tests_

#include <iostream>
#include <fstream>
#include <sstream>
#include "graph_input_handling.hpp"

void automatic_tests(char* input_file, char* output_file, bool two_ids_flag)
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
        int id_to_test;
        int id2_to_test;
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
            else if (counter == 3)
            {
                id_to_test = atoi(cell.c_str());
            }
            if (two_ids_flag && counter == 4)
            {
                id2_to_test = atoi(cell.c_str());
            }
            counter++;
        }
        if (valid && two_ids_flag)
        {
            if (output_type == 'i')
            {
                cout << "calling int autohandle with func " << func_name << " and rpos " << r_pos << " and ids " << id_to_test << "," << id2_to_test << "\n";
                time_span = auto_handle_input<int>(func_name, r_pos, id_to_test, id2_to_test);
            //    time_span2 = auto_handle_input<int>(func_name, r_pos);
            //    time_span3 = auto_handle_input<int>(func_name, r_pos);
            }
            else if (output_type == 'd')
            {
                cout << "calling double autohandle with func " << func_name << " and rpos " << r_pos << " and ids " << id_to_test << "," << id2_to_test << "\n";
                time_span = auto_handle_input<double>(func_name, r_pos, id_to_test, id2_to_test);
            //    time_span2 = auto_handle_input<double>(func_name, r_pos);
            //    time_span3 = auto_handle_input<double>(func_name, r_pos);
            }

            outfile << func_name << ", " << time_span.count() << " sec\n";// << time_span2.count() << " sec, " << time_span3.count() << " sec\n";
        }
        else
        {
             if (output_type == 'i')
            {
                cout << "calling int autohandle with func " << func_name << " and rpos " << r_pos << " and id " << id_to_test << "\n";
                time_span = auto_handle_input<int>(func_name, r_pos, id_to_test);
            //    time_span2 = auto_handle_input<int>(func_name, r_pos);
            //    time_span3 = auto_handle_input<int>(func_name, r_pos);
            }
            else if (output_type == 'd')
            {
                cout << "calling double autohandle with func " << func_name << " and rpos " << r_pos << " and id " << id_to_test << "\n";
                time_span = auto_handle_input<double>(func_name, r_pos, id_to_test);
            //    time_span2 = auto_handle_input<double>(func_name, r_pos);
            //    time_span3 = auto_handle_input<double>(func_name, r_pos);
            }

            outfile << func_name << ", " << time_span.count() << " sec\n";// << time_span2.count() << " sec, " << time_span3.count() << " sec\n";



        }
    }
    myfile.close();
    outfile.close();
    // cerr << "..." << lines_read_in << " lines read in.\n";

}





#endif
