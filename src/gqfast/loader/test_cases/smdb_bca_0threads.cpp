#ifndef smdb_bca_0threads_
#define smdb_bca_0threads_

#include "../fastr_index.hpp"
#include "../global_vars.hpp"


using namespace std;

static chrono::steady_clock::time_point t1;
static chrono::steady_clock::time_point t2;
static chrono::duration<double> tspan;

static int* R;
static int* RC;

static uint64_t* concept_semtype1_col0_buffer;
static uint64_t* predication1_col0_buffer;
static uint64_t* sentence1_col0_buffer;
static uint64_t* predication2_col0_buffer;
static uint64_t* concept_semtype2_col0_buffer;
static uint64_t* concept2_col0_buffer;

static bool* sentence1_bool_array;

static uint32_t* concept_semtype1_col0_bits_info;
static uint64_t concept_semtype1_col0_offset;

static uint32_t* predication1_col0_bits_info;
static uint64_t predication1_col0_offset;

static uint32_t* sentence1_col0_bits_info;
static uint64_t sentence1_col0_offset;

static uint32_t* predication2_col0_bits_info;
static uint64_t predication2_col0_offset;

static uint32_t* concept_semtype2_col0_bits_info;
static uint64_t concept_semtype2_col0_offset;

static uint32_t* concept2_col0_bits_info;
static uint64_t concept2_col0_offset;

extern inline void smdb_bca_0threads_concept_semtype1_col0_decode_BCA(unsigned char* concept_semtype1_col0_ptr, uint32_t concept_semtype1_col0_bytes, uint32_t & concept_semtype1_fragment_size) __attribute__((always_inline));

extern inline void smdb_bca_0threads_predication1_col0_decode_BCA(unsigned char* predication1_col0_ptr, uint32_t predication1_col0_bytes, uint32_t & predication1_fragment_size) __attribute__((always_inline));

extern inline void smdb_bca_0threads_sentence1_col0_decode_BCA(unsigned char* sentence1_col0_ptr, uint32_t sentence1_col0_bytes, uint32_t & sentence1_fragment_size) __attribute__((always_inline));

extern inline void smdb_bca_0threads_predication2_col0_decode_BCA(unsigned char* predication2_col0_ptr, uint32_t predication2_col0_bytes, uint32_t & predication2_fragment_size) __attribute__((always_inline));

extern inline void smdb_bca_0threads_concept_semtype2_col0_decode_BCA(unsigned char* concept_semtype2_col0_ptr, uint32_t concept_semtype2_col0_bytes, uint32_t & concept_semtype2_fragment_size) __attribute__((always_inline));

extern inline void smdb_bca_0threads_concept2_col0_decode_BCA(unsigned char* concept2_col0_ptr, uint32_t concept2_col0_bytes, uint32_t & concept2_fragment_size) __attribute__((always_inline));

void smdb_bca_0threads_concept_semtype1_col0_decode_BCA(unsigned char* concept_semtype1_col0_ptr, uint32_t concept_semtype1_col0_bytes, uint32_t & concept_semtype1_fragment_size) {

	concept_semtype1_fragment_size = concept_semtype1_col0_bytes* 8 / concept_semtype1_col0_bits_info[0];
	int bit_pos = 0;
	for (uint32_t i=0; i<concept_semtype1_fragment_size; i++) {
		uint32_t encoded_value = concept_semtype1_col0_bits_info[1] << bit_pos;
		uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(concept_semtype1_col0_ptr);
		encoded_value &= *next_8_ptr;
		encoded_value >>= bit_pos;

		concept_semtype1_col0_ptr += (bit_pos + concept_semtype1_col0_bits_info[0]) / 8;
		bit_pos = (bit_pos + concept_semtype1_col0_bits_info[0]) % 8;
		concept_semtype1_col0_buffer[i] = encoded_value + concept_semtype1_col0_offset;
	}
}

void smdb_bca_0threads_predication1_col0_decode_BCA(unsigned char* predication1_col0_ptr, uint32_t predication1_col0_bytes, uint32_t & predication1_fragment_size) {

	predication1_fragment_size = predication1_col0_bytes* 8 / predication1_col0_bits_info[0];
	int bit_pos = 0;
	for (uint32_t i=0; i<predication1_fragment_size; i++) {
		uint32_t encoded_value = predication1_col0_bits_info[1] << bit_pos;
		uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(predication1_col0_ptr);
		encoded_value &= *next_8_ptr;
		encoded_value >>= bit_pos;

		predication1_col0_ptr += (bit_pos + predication1_col0_bits_info[0]) / 8;
		bit_pos = (bit_pos + predication1_col0_bits_info[0]) % 8;
		predication1_col0_buffer[i] = encoded_value + predication1_col0_offset;
	}
}

void smdb_bca_0threads_sentence1_col0_decode_BCA(unsigned char* sentence1_col0_ptr, uint32_t sentence1_col0_bytes, uint32_t & sentence1_fragment_size) {

	sentence1_fragment_size = sentence1_col0_bytes* 8 / sentence1_col0_bits_info[0];
	int bit_pos = 0;
	for (uint32_t i=0; i<sentence1_fragment_size; i++) {
		uint32_t encoded_value = sentence1_col0_bits_info[1] << bit_pos;
		uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(sentence1_col0_ptr);
		encoded_value &= *next_8_ptr;
		encoded_value >>= bit_pos;

		sentence1_col0_ptr += (bit_pos + sentence1_col0_bits_info[0]) / 8;
		bit_pos = (bit_pos + sentence1_col0_bits_info[0]) % 8;
		sentence1_col0_buffer[i] = encoded_value + sentence1_col0_offset;
	}
}

void smdb_bca_0threads_predication2_col0_decode_BCA(unsigned char* predication2_col0_ptr, uint32_t predication2_col0_bytes, uint32_t & predication2_fragment_size) {

	predication2_fragment_size = predication2_col0_bytes* 8 / predication2_col0_bits_info[0];
	int bit_pos = 0;
	for (uint32_t i=0; i<predication2_fragment_size; i++) {
		uint32_t encoded_value = predication2_col0_bits_info[1] << bit_pos;
		uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(predication2_col0_ptr);
		encoded_value &= *next_8_ptr;
		encoded_value >>= bit_pos;

		predication2_col0_ptr += (bit_pos + predication2_col0_bits_info[0]) / 8;
		bit_pos = (bit_pos + predication2_col0_bits_info[0]) % 8;
		predication2_col0_buffer[i] = encoded_value + predication2_col0_offset;
	}
}

void smdb_bca_0threads_concept_semtype2_col0_decode_BCA(unsigned char* concept_semtype2_col0_ptr, uint32_t concept_semtype2_col0_bytes, uint32_t & concept_semtype2_fragment_size) {

	concept_semtype2_fragment_size = concept_semtype2_col0_bytes* 8 / concept_semtype2_col0_bits_info[0];
	int bit_pos = 0;
	for (uint32_t i=0; i<concept_semtype2_fragment_size; i++) {
		uint32_t encoded_value = concept_semtype2_col0_bits_info[1] << bit_pos;
		uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(concept_semtype2_col0_ptr);
		encoded_value &= *next_8_ptr;
		encoded_value >>= bit_pos;

		concept_semtype2_col0_ptr += (bit_pos + concept_semtype2_col0_bits_info[0]) / 8;
		bit_pos = (bit_pos + concept_semtype2_col0_bits_info[0]) % 8;
		concept_semtype2_col0_buffer[i] = encoded_value + concept_semtype2_col0_offset;
	}
}

void smdb_bca_0threads_concept2_col0_decode_BCA(unsigned char* concept2_col0_ptr, uint32_t concept2_col0_bytes, uint32_t & concept2_fragment_size) {

	concept2_fragment_size = concept2_col0_bytes* 8 / concept2_col0_bits_info[0];
	int bit_pos = 0;
	for (uint32_t i=0; i<concept2_fragment_size; i++) {
		uint32_t encoded_value = concept2_col0_bits_info[1] << bit_pos;
		uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(concept2_col0_ptr);
		encoded_value &= *next_8_ptr;
		encoded_value >>= bit_pos;

		concept2_col0_ptr += (bit_pos + concept2_col0_bits_info[0]) / 8;
		bit_pos = (bit_pos + concept2_col0_bits_info[0]) % 8;
		concept2_col0_buffer[i] = encoded_value + concept2_col0_offset;
	}
}

extern "C" int* smdb_bca_0threads(int** null_checks) {

	benchmark_t1 = chrono::steady_clock::now();

	tspan = chrono::steady_clock::duration::zero();

	int max_frag;

	max_frag = metadata.idx_max_fragment_sizes[0];
	concept_semtype1_col0_buffer = new uint64_t[max_frag];

	max_frag = metadata.idx_max_fragment_sizes[1];
	predication1_col0_buffer = new uint64_t[max_frag];

	max_frag = metadata.idx_max_fragment_sizes[2];
	sentence1_col0_buffer = new uint64_t[max_frag];

	max_frag = metadata.idx_max_fragment_sizes[3];
	predication2_col0_buffer = new uint64_t[max_frag];

	max_frag = metadata.idx_max_fragment_sizes[4];
	concept_semtype2_col0_buffer = new uint64_t[max_frag];

	max_frag = metadata.idx_max_fragment_sizes[5];
	concept2_col0_buffer = new uint64_t[max_frag];

	cerr << "domain2 = " << metadata.idx_domains[2][0];
	cerr << "domain5 = " << metadata.idx_domains[5][0];

	t1 = chrono::steady_clock::now();
	RC = new int[metadata.idx_domains[5][0]]();
	R = new int[metadata.idx_domains[5][0]]();


    t2 = chrono::steady_clock::now();
    tspan = chrono::duration_cast<chrono::duration<double>>(t2 - t1);
	cerr << "result arrays = " << tspan.count() << " sec\n";

	t1 = chrono::steady_clock::now();
	sentence1_bool_array = new bool[metadata.idx_domains[2][0]]();

    t2 = chrono::steady_clock::now();
    tspan = chrono::duration_cast<chrono::duration<double>>(t2 - t1);
	cerr << "bool array = " << tspan.count() << " sec\n";

	concept_semtype1_col0_bits_info = idx[0]->dict[0]->bits_info;
	concept_semtype1_col0_offset = idx[0]->dict[0]->offset;

	predication1_col0_bits_info = idx[1]->dict[0]->bits_info;
	predication1_col0_offset = idx[1]->dict[0]->offset;

	sentence1_col0_bits_info = idx[2]->dict[0]->bits_info;
	sentence1_col0_offset = idx[2]->dict[0]->offset;

	predication2_col0_bits_info = idx[3]->dict[0]->bits_info;
	predication2_col0_offset = idx[3]->dict[0]->offset;

	concept_semtype2_col0_bits_info = idx[4]->dict[0]->bits_info;
	concept_semtype2_col0_offset = idx[4]->dict[0]->offset;

	concept2_col0_bits_info = idx[5]->dict[0]->bits_info;
	concept2_col0_offset = idx[5]->dict[0]->offset;

	uint64_t concept1_list[1];
	concept1_list[0] = 2019;


	for (int concept1_it = 0; concept1_it<1; concept1_it++) {

		uint64_t concept1_col0_element = concept1_list[concept1_it];

		uint32_t* row_concept_semtype1 = idx[0]->index_map[concept1_col0_element];
		uint32_t concept_semtype1_col0_bytes = idx[0]->index_map[concept1_col0_element+1][0] - row_concept_semtype1[0];
		if(concept_semtype1_col0_bytes) {

			unsigned char* concept_semtype1_col0_ptr = &(idx[0]->fragment_data[0][row_concept_semtype1[0]]);
			uint32_t concept_semtype1_fragment_size = 0;
			smdb_bca_0threads_concept_semtype1_col0_decode_BCA(concept_semtype1_col0_ptr, concept_semtype1_col0_bytes, concept_semtype1_fragment_size);

			for (uint32_t concept_semtype1_it = 0; concept_semtype1_it < concept_semtype1_fragment_size; concept_semtype1_it++) {

				uint32_t concept_semtype1_col0_element = concept_semtype1_col0_buffer[concept_semtype1_it];

				uint32_t* row_predication1 = idx[1]->index_map[concept_semtype1_col0_element];
				uint32_t predication1_col0_bytes = idx[1]->index_map[concept_semtype1_col0_element+1][0] - row_predication1[0];
				if(predication1_col0_bytes) {

					unsigned char* predication1_col0_ptr = &(idx[1]->fragment_data[0][row_predication1[0]]);
					uint32_t predication1_fragment_size = 0;
					smdb_bca_0threads_predication1_col0_decode_BCA(predication1_col0_ptr, predication1_col0_bytes, predication1_fragment_size);

					for (uint32_t predication1_it = 0; predication1_it < predication1_fragment_size; predication1_it++) {

						uint32_t predication1_col0_element = predication1_col0_buffer[predication1_it];

						uint32_t* row_sentence1 = idx[2]->index_map[predication1_col0_element];
						uint32_t sentence1_col0_bytes = idx[2]->index_map[predication1_col0_element+1][0] - row_sentence1[0];
						if(sentence1_col0_bytes) {

							unsigned char* sentence1_col0_ptr = &(idx[2]->fragment_data[0][row_sentence1[0]]);
							uint32_t sentence1_fragment_size = 0;
							smdb_bca_0threads_sentence1_col0_decode_BCA(sentence1_col0_ptr, sentence1_col0_bytes, sentence1_fragment_size);

							for (uint32_t sentence1_it = 0; sentence1_it < sentence1_fragment_size; sentence1_it++) {


								if (!(sentence1_bool_array[sentence1_col0_buffer[sentence1_it]])) {
									sentence1_bool_array[sentence1_col0_buffer[sentence1_it]] = true;
									uint32_t sentence1_col0_element = sentence1_col0_buffer[sentence1_it];

									uint32_t* row_predication2 = idx[3]->index_map[sentence1_col0_element];
									uint32_t predication2_col0_bytes = idx[3]->index_map[sentence1_col0_element+1][0] - row_predication2[0];
									if(predication2_col0_bytes) {

										unsigned char* predication2_col0_ptr = &(idx[3]->fragment_data[0][row_predication2[0]]);
										uint32_t predication2_fragment_size = 0;
										smdb_bca_0threads_predication2_col0_decode_BCA(predication2_col0_ptr, predication2_col0_bytes, predication2_fragment_size);

										for (uint32_t predication2_it = 0; predication2_it < predication2_fragment_size; predication2_it++) {

											uint32_t predication2_col0_element = predication2_col0_buffer[predication2_it];

											uint32_t* row_concept_semtype2 = idx[4]->index_map[predication2_col0_element];
											uint32_t concept_semtype2_col0_bytes = idx[4]->index_map[predication2_col0_element+1][0] - row_concept_semtype2[0];
											if(concept_semtype2_col0_bytes) {

												unsigned char* concept_semtype2_col0_ptr = &(idx[4]->fragment_data[0][row_concept_semtype2[0]]);
												uint32_t concept_semtype2_fragment_size = 0;
												smdb_bca_0threads_concept_semtype2_col0_decode_BCA(concept_semtype2_col0_ptr, concept_semtype2_col0_bytes, concept_semtype2_fragment_size);

												for (uint32_t concept_semtype2_it = 0; concept_semtype2_it < concept_semtype2_fragment_size; concept_semtype2_it++) {

													uint32_t concept_semtype2_col0_element = concept_semtype2_col0_buffer[concept_semtype2_it];

													uint32_t* row_concept2 = idx[5]->index_map[concept_semtype2_col0_element];
													uint32_t concept2_col0_bytes = idx[5]->index_map[concept_semtype2_col0_element+1][0] - row_concept2[0];
													if(concept2_col0_bytes) {

														unsigned char* concept2_col0_ptr = &(idx[5]->fragment_data[0][row_concept2[0]]);
														uint32_t concept2_fragment_size = 0;

														smdb_bca_0threads_concept2_col0_decode_BCA(concept2_col0_ptr, concept2_col0_bytes, concept2_fragment_size);



														for (uint32_t concept2_it = 0; concept2_it < concept2_fragment_size; concept2_it++) {
															uint32_t concept2_col0_element = concept2_col0_buffer[concept2_it];

															RC[concept2_col0_element] = 1;
															R[concept2_col0_element] += 1;
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}


	delete[] concept_semtype1_col0_buffer;
	delete[] predication1_col0_buffer;
	delete[] sentence1_col0_buffer;
	delete[] predication2_col0_buffer;
	delete[] concept_semtype2_col0_buffer;
	delete[] concept2_col0_buffer;

	delete[] sentence1_bool_array;


	*null_checks = RC;
	return R;

}

#endif

