#ifndef dictionary_cpp_
#define dictionary_cpp_

#include "dictionary.hpp"
#include <cmath>
#include <cstdint>

#define BYTE_SIZE 8

vector<unsigned char> compression(vector<int> T, int domain_size)
{


    int bits_per_element = (int)floor(log2((double)domain_size)) + 1;
    int64_t total_bits = T.size()* bits_per_element;
    int bits_to_next_byte = BYTE_SIZE - (total_bits % BYTE_SIZE);

    int64_t total_bytes = (total_bits + bits_to_next_byte)/BYTE_SIZE;

    unsigned char* temp_data = new unsigned char[total_bytes]();

    int bit_pos = 0;
    int64_t byte_pos = 0;

    for (int64_t i=0; i< T.size(); i++)
    {

        uint64_t encoded_val = T[i];
        // 8 byte 'encoded_val' guaranteed not to lose data when bit-shifting on 4 byte input
        encoded_val = encoded_val << bit_pos;

        // Access current + next 7 bytes
        uint64_t * column_address = (uint64_t *) &(temp_data[byte_pos]);

        // Emplaces the value
        *column_address |= encoded_val;

        // Move the bit and byte positions
        byte_pos += (bit_pos + bits_per_element)/8;
        bit_pos = (bit_pos + bits_per_element)%8;

    }

    // To vector
    vector<unsigned char> compressed_data;
    for (int64_t i=0; i<total_bytes; i++) {
        compressed_data.push_back(temp_data[i]);
    }

    delete[] temp_data;

    return compressed_data;
}

vector<int> decompression(vector<unsigned char> compressed_data, int domain_size)
{
    int bits_per_element = (int)floor(log2((double)domain_size)) + 1;
    uint64_t copy_bits = pow(2,bits_per_element)-1;
    int input_size = compressed_data.size();

    unsigned char* temp_data = new unsigned char[input_size];
    for (int64_t i=0; i<input_size; i++) {
        temp_data[i] = compressed_data[i];
    }

    int output_size = input_size * BYTE_SIZE / bits_per_element;
    unsigned char* temp_ptr = &(temp_data[0]);
	int bit_pos = 0;
	vector<int> decompressed_data;
	decompressed_data.resize(output_size);
	for (int64_t i=0; i<output_size; i++) {
		uint64_t encoded_value = copy_bits << bit_pos;
		uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(temp_ptr);
		encoded_value &= *next_8_ptr;
		encoded_value >>= bit_pos;

		temp_ptr += (bit_pos + bits_per_element) / 8;
		bit_pos = (bit_pos + bits_per_element) % 8;
        decompressed_data[i] = encoded_value;
	}

	return decompressed_data;
}

#endif
