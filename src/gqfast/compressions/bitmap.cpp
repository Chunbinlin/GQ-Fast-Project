#ifndef bitmap_cpp_
#define bitmap_cpp_

#include <cstdint>
#include "bitmap.hpp"


vector<unsigned char> compression(vector<int> T) {


    uint32_t last_position = 0;

    vector<unsigned char> compressed_data;

    for (int i=0; i<T.size(); i++)
    {
        uint32_t runlength = T[i] - last_position;

        // Convert runlength to byte-aligned bitmap
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
            compressed_data.push_back(curr_byte);
        }
        while (runlength > 0);
        // Move position forward
        last_position = T[i] + 1;
    }

    return compressed_data;

}


vector<int> decompression(vector<unsigned char> compressed_data) {

    vector<int> decompressed_data;
    decompressed_data.push_back(0);

    int64_t bytes_remaining = compressed_data.size();
	int64_t byte_pos = 0;
	int shiftbits = 0;
	do {
		bytes_remaining--;
		int32_t next_seven_bits = compressed_data[byte_pos] & 127;
		next_seven_bits = next_seven_bits << shiftbits;
		decompressed_data[0] |= next_seven_bits;
		shiftbits += 7;
	} while (!(compressed_data[byte_pos++] & 128));
    int i = 0;
	while (bytes_remaining > 0) {
		shiftbits = 0;
		int result = 0;

		do {

			bytes_remaining--;
			int32_t next_seven_bits = compressed_data[byte_pos] & 127;
			next_seven_bits = next_seven_bits << shiftbits;
			result |= next_seven_bits;
			shiftbits += 7;

		} while (!(compressed_data[byte_pos++] & 128));

		decompressed_data.push_back(decompressed_data[i++]+1+result);

	}

    return decompressed_data;

}





#endif
