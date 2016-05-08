#ifndef byte_aligned_bitmap_
#define byte_aligned_bitmap_

#include <iostream>
#include <vector>

using namespace std;

template <typename T>
void encode_bitmap(vector<T> & fragment_input, vector<unsigned char *> & rle_output, uint32_t & bytes)
{

    uint32_t last_position = 0;

    vector<unsigned char> temp_rle_array;

    for (int i=0; i<fragment_input.size(); i++)
    {
        uint32_t runlength = fragment_input[i] - last_position;

        // Convert runlength to byte-aligned RLE
        do
        {
            // process next 7 bits
            unsigned char curr_byte = runlength % 128;
            runlength = runlength / 128;
            if (runlength == 0)
            {
                // last byte of run has first bit set to 1, signifying end
                curr_byte = curr_byte | 128;
            }
            temp_rle_array.push_back(curr_byte);
        }
        while (runlength > 0);
        // Move position forward
        last_position = fragment_input[i] + 1;
    }

    // Move to array
    bytes = temp_rle_array.size();
    unsigned char * rle_array = new unsigned char[bytes];
    for (int i=0; i<bytes; i++)
    {
        rle_array[i] = temp_rle_array[i];
    }
    rle_output.push_back(rle_array);
}

template <typename T>
T next_run_length(unsigned char ** fragment_data, uint32_t & byte_pos, int col, uint32_t count, uint32_t size)
{

    if (count == size)
    {
        return 0;
    }

    T result = 0;
    int shiftbits = 0;
    bool endbyte = false;
    while (!endbyte)
    {
        uint32_t next_seven_bits = fragment_data[col][byte_pos] & 127;
        next_seven_bits = next_seven_bits << shiftbits;
        result |= next_seven_bits;

        if (fragment_data[col][byte_pos] & 128)
        {
            endbyte = true;
        }
        else
        {
            shiftbits += 7;
        }
        byte_pos++;
    }

    return result;
}

#endif
