#ifndef test_smdb_bca_1threads_
#define test_smdb_bca_1threads_

#include "../fastr_index.hpp"
#include "../global_vars.hpp"

#define NUM_THREADS 1
#define NUM_BUFFERS 6
#define BUFFER_POOL_SIZE 1

using namespace std;

static int* R;
static int* RC;

static bool* sentence1_bool_array;

static uint32_t* concept_semtype1_col0_bits_info;

static uint32_t* predication1_col0_bits_info;

static uint32_t* sentence1_col0_bits_info;

static uint32_t* predication2_col0_bits_info;

static uint32_t* concept_semtype2_col0_bits_info;

static uint32_t* concept2_col0_bits_info;

extern inline void test_smdb_bca_1threads_concept_semtype1_col0_decode_BCA(unsigned char* concept_semtype1_col0_ptr, uint32_t concept_semtype1_col0_bytes, uint32_t & concept_semtype1_fragment_size) __attribute__((always_inline));

extern inline void test_smdb_bca_1threads_predication1_col0_decode_BCA(unsigned char* predication1_col0_ptr, uint32_t predication1_col0_bytes, uint32_t & predication1_fragment_size) __attribute__((always_inline));

extern inline void test_smdb_bca_1threads_sentence1_col0_decode_BCA(unsigned char* sentence1_col0_ptr, uint32_t sentence1_col0_bytes, uint32_t & sentence1_fragment_size) __attribute__((always_inline));

extern inline void test_smdb_bca_1threads_predication2_col0_decode_BCA(unsigned char* predication2_col0_ptr, uint32_t predication2_col0_bytes, uint32_t & predication2_fragment_size) __attribute__((always_inline));

extern inline void test_smdb_bca_1threads_concept_semtype2_col0_decode_BCA(unsigned char* concept_semtype2_col0_ptr, uint32_t concept_semtype2_col0_bytes, uint32_t & concept_semtype2_fragment_size) __attribute__((always_inline));

extern inline void test_smdb_bca_1threads_concept2_col0_decode_BCA(unsigned char* concept2_col0_ptr, uint32_t concept2_col0_bytes, uint32_t & concept2_fragment_size) __attribute__((always_inline));

void test_smdb_bca_1threads_concept_semtype1_col0_decode_BCA(unsigned char* concept_semtype1_col0_ptr, uint32_t concept_semtype1_col0_bytes, uint32_t & concept_semtype1_fragment_size) {

	concept_semtype1_fragment_size = concept_semtype1_col0_bytes* 8 / concept_semtype1_col0_bits_info[0];
	int bit_pos = 0;
	for (uint32_t i=0; i<concept_semtype1_fragment_size; i++) {
		uint32_t encoded_value = concept_semtype1_col0_bits_info[1] << bit_pos;
		uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(concept_semtype1_col0_ptr);
		encoded_value &= *next_8_ptr;
		encoded_value >>= bit_pos;

		concept_semtype1_col0_ptr += (bit_pos + concept_semtype1_col0_bits_info[0]) / 8;
		bit_pos = (bit_pos + concept_semtype1_col0_bits_info[0]) % 8;
		buffer_arrays[0][0][0][0][i] = encoded_value;
	}
}

void test_smdb_bca_1threads_predication1_col0_decode_BCA(unsigned char* predication1_col0_ptr, uint32_t predication1_col0_bytes, uint32_t & predication1_fragment_size) {

	predication1_fragment_size = predication1_col0_bytes* 8 / predication1_col0_bits_info[0];
	int bit_pos = 0;
	for (uint32_t i=0; i<predication1_fragment_size; i++) {
		uint32_t encoded_value = predication1_col0_bits_info[1] << bit_pos;
		uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(predication1_col0_ptr);
		encoded_value &= *next_8_ptr;
		encoded_value >>= bit_pos;

		predication1_col0_ptr += (bit_pos + predication1_col0_bits_info[0]) / 8;
		bit_pos = (bit_pos + predication1_col0_bits_info[0]) % 8;
		buffer_arrays[1][0][0][0][i] = encoded_value;
	}
}

void test_smdb_bca_1threads_sentence1_col0_decode_BCA(unsigned char* sentence1_col0_ptr, uint32_t sentence1_col0_bytes, uint32_t & sentence1_fragment_size) {

	sentence1_fragment_size = sentence1_col0_bytes* 8 / sentence1_col0_bits_info[0];
	int bit_pos = 0;
	for (uint32_t i=0; i<sentence1_fragment_size; i++) {
		uint32_t encoded_value = sentence1_col0_bits_info[1] << bit_pos;
		uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(sentence1_col0_ptr);
		encoded_value &= *next_8_ptr;
		encoded_value >>= bit_pos;

		sentence1_col0_ptr += (bit_pos + sentence1_col0_bits_info[0]) / 8;
		bit_pos = (bit_pos + sentence1_col0_bits_info[0]) % 8;
		buffer_arrays[2][0][0][0][i] = encoded_value;
	}
}

void test_smdb_bca_1threads_predication2_col0_decode_BCA(unsigned char* predication2_col0_ptr, uint32_t predication2_col0_bytes, uint32_t & predication2_fragment_size) {

	predication2_fragment_size = predication2_col0_bytes* 8 / predication2_col0_bits_info[0];
	int bit_pos = 0;
	for (uint32_t i=0; i<predication2_fragment_size; i++) {
		uint32_t encoded_value = predication2_col0_bits_info[1] << bit_pos;
		uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(predication2_col0_ptr);
		encoded_value &= *next_8_ptr;
		encoded_value >>= bit_pos;

		predication2_col0_ptr += (bit_pos + predication2_col0_bits_info[0]) / 8;
		bit_pos = (bit_pos + predication2_col0_bits_info[0]) % 8;
		buffer_arrays[3][0][0][0][i] = encoded_value;
	}
}

void test_smdb_bca_1threads_concept_semtype2_col0_decode_BCA(unsigned char* concept_semtype2_col0_ptr, uint32_t concept_semtype2_col0_bytes, uint32_t & concept_semtype2_fragment_size) {

	concept_semtype2_fragment_size = concept_semtype2_col0_bytes* 8 / concept_semtype2_col0_bits_info[0];
	int bit_pos = 0;
	for (uint32_t i=0; i<concept_semtype2_fragment_size; i++) {
		uint32_t encoded_value = concept_semtype2_col0_bits_info[1] << bit_pos;
		uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(concept_semtype2_col0_ptr);
		encoded_value &= *next_8_ptr;
		encoded_value >>= bit_pos;

		concept_semtype2_col0_ptr += (bit_pos + concept_semtype2_col0_bits_info[0]) / 8;
		bit_pos = (bit_pos + concept_semtype2_col0_bits_info[0]) % 8;
		buffer_arrays[4][0][0][0][i] = encoded_value;
	}
}

void test_smdb_bca_1threads_concept2_col0_decode_BCA(unsigned char* concept2_col0_ptr, uint32_t concept2_col0_bytes, uint32_t & concept2_fragment_size) {

	concept2_fragment_size = concept2_col0_bytes* 8 / concept2_col0_bits_info[0];
	int bit_pos = 0;
	for (uint32_t i=0; i<concept2_fragment_size; i++) {
		uint32_t encoded_value = concept2_col0_bits_info[1] << bit_pos;
		uint64_t * next_8_ptr = reinterpret_cast<uint64_t *>(concept2_col0_ptr);
		encoded_value &= *next_8_ptr;
		encoded_value >>= bit_pos;

		concept2_col0_ptr += (bit_pos + concept2_col0_bits_info[0]) / 8;
		bit_pos = (bit_pos + concept2_col0_bits_info[0]) % 8;
		buffer_arrays[5][0][0][0][i] = encoded_value;
	}
}

extern "C" int* test_smdb_bca_1threads(int** null_checks) {

	benchmark_t1 = chrono::steady_clock::now();

	int max_frag;

	max_frag = metadata.idx_max_fragment_sizes[0];
	for(int i=0; i<metadata.idx_num_encodings[0]; i++) {
		for (int j=0; j<NUM_THREADS; j++) {
			buffer_arrays[0][i][j] = new int*[BUFFER_POOL_SIZE];
			for (int k=0; k<BUFFER_POOL_SIZE; k++) {
				buffer_arrays[0][i][j][k] = new int[max_frag];
			}
		}
	}

	max_frag = metadata.idx_max_fragment_sizes[1];
	for(int i=0; i<metadata.idx_num_encodings[1]; i++) {
		for (int j=0; j<NUM_THREADS; j++) {
			buffer_arrays[1][i][j] = new int*[BUFFER_POOL_SIZE];
			for (int k=0; k<BUFFER_POOL_SIZE; k++) {
				buffer_arrays[1][i][j][k] = new int[max_frag];
			}
		}
	}

	max_frag = metadata.idx_max_fragment_sizes[2];
	for(int i=0; i<metadata.idx_num_encodings[2]; i++) {
		for (int j=0; j<NUM_THREADS; j++) {
			buffer_arrays[2][i][j] = new int*[BUFFER_POOL_SIZE];
			for (int k=0; k<BUFFER_POOL_SIZE; k++) {
				buffer_arrays[2][i][j][k] = new int[max_frag];
			}
		}
	}

	max_frag = metadata.idx_max_fragment_sizes[3];
	for(int i=0; i<metadata.idx_num_encodings[3]; i++) {
		for (int j=0; j<NUM_THREADS; j++) {
			buffer_arrays[3][i][j] = new int*[BUFFER_POOL_SIZE];
			for (int k=0; k<BUFFER_POOL_SIZE; k++) {
				buffer_arrays[3][i][j][k] = new int[max_frag];
			}
		}
	}

	max_frag = metadata.idx_max_fragment_sizes[4];
	for(int i=0; i<metadata.idx_num_encodings[4]; i++) {
		for (int j=0; j<NUM_THREADS; j++) {
			buffer_arrays[4][i][j] = new int*[BUFFER_POOL_SIZE];
			for (int k=0; k<BUFFER_POOL_SIZE; k++) {
				buffer_arrays[4][i][j][k] = new int[max_frag];
			}
		}
	}

	max_frag = metadata.idx_max_fragment_sizes[5];
	for(int i=0; i<metadata.idx_num_encodings[5]; i++) {
		for (int j=0; j<NUM_THREADS; j++) {
			buffer_arrays[5][i][j] = new int*[BUFFER_POOL_SIZE];
			for (int k=0; k<BUFFER_POOL_SIZE; k++) {
				buffer_arrays[5][i][j][k] = new int[max_frag];
			}
		}
	}

	RC = new int[metadata.idx_domains[5][0]]();
	R = new int[metadata.idx_domains[5][0]]();

	sentence1_bool_array = new bool[metadata.idx_domains[2][0]]();

	concept_semtype1_col0_bits_info = idx[0]->dict[0]->bits_info;

	predication1_col0_bits_info = idx[1]->dict[0]->bits_info;

	sentence1_col0_bits_info = idx[2]->dict[0]->bits_info;

	predication2_col0_bits_info = idx[3]->dict[0]->bits_info;

	concept_semtype2_col0_bits_info = idx[4]->dict[0]->bits_info;

	concept2_col0_bits_info = idx[5]->dict[0]->bits_info;

	uint64_t* concept1_list = new uint64_t[1];
	concept1_list[0] = 2019;

	for (int concept1_it = 0; concept1_it<1; concept1_it++) {

		uint64_t concept1_col0_element = concept1_list[concept1_it];

		uint32_t* row_op1 = idx[0]->index_map[concept1_col0_element];
		uint32_t concept_semtype1_col0_bytes = idx[0]->index_map[concept1_col0_element+1][0] - row_op1[0];
		if(concept_semtype1_col0_bytes) {

			unsigned char* concept_semtype1_col0_ptr = &(idx[0]->fragment_data[0][row_op1[0]]);
			uint32_t concept_semtype1_fragment_size = 0;
			test_smdb_bca_1threads_concept_semtype1_col0_decode_BCA(concept_semtype1_col0_ptr, concept_semtype1_col0_bytes, concept_semtype1_fragment_size);

			for (uint32_t concept_semtype1_it = 0; concept_semtype1_it < concept_semtype1_fragment_size; concept_semtype1_it++) {

				uint32_t concept_semtype1_col0_element = buffer_arrays[0][0][0][0][concept_semtype1_it];

				uint32_t* row_op2 = idx[1]->index_map[concept_semtype1_col0_element];
				uint32_t predication1_col0_bytes = idx[1]->index_map[concept_semtype1_col0_element+1][0] - row_op2[0];
				if(predication1_col0_bytes) {

					unsigned char* predication1_col0_ptr = &(idx[1]->fragment_data[0][row_op2[0]]);
					uint32_t predication1_fragment_size = 0;
					test_smdb_bca_1threads_predication1_col0_decode_BCA(predication1_col0_ptr, predication1_col0_bytes, predication1_fragment_size);

					for (uint32_t predication1_it = 0; predication1_it < predication1_fragment_size; predication1_it++) {

						uint32_t predication1_col0_element = buffer_arrays[1][0][0][0][predication1_it];

						uint32_t* row_op3 = idx[2]->index_map[predication1_col0_element];
						uint32_t sentence1_col0_bytes = idx[2]->index_map[predication1_col0_element+1][0] - row_op3[0];
						if(sentence1_col0_bytes) {

							unsigned char* sentence1_col0_ptr = &(idx[2]->fragment_data[0][row_op3[0]]);
							uint32_t sentence1_fragment_size = 0;
							test_smdb_bca_1threads_sentence1_col0_decode_BCA(sentence1_col0_ptr, sentence1_col0_bytes, sentence1_fragment_size);

							for (uint32_t sentence1_it = 0; sentence1_it < sentence1_fragment_size; sentence1_it++) {


								if (!(sentence1_bool_array[buffer_arrays[2][0][0][0][sentence1_it]])) {
									sentence1_bool_array[buffer_arrays[2][0][0][0][sentence1_it]] = true;
									uint32_t sentence1_col0_element = buffer_arrays[2][0][0][0][sentence1_it];

									uint32_t* row_op4 = idx[3]->index_map[sentence1_col0_element];
									uint32_t predication2_col0_bytes = idx[3]->index_map[sentence1_col0_element+1][0] - row_op4[0];
									if(predication2_col0_bytes) {

										unsigned char* predication2_col0_ptr = &(idx[3]->fragment_data[0][row_op4[0]]);
										uint32_t predication2_fragment_size = 0;
										test_smdb_bca_1threads_predication2_col0_decode_BCA(predication2_col0_ptr, predication2_col0_bytes, predication2_fragment_size);

										for (uint32_t predication2_it = 0; predication2_it < predication2_fragment_size; predication2_it++) {

											uint32_t predication2_col0_element = buffer_arrays[3][0][0][0][predication2_it];

											uint32_t* row_op5 = idx[4]->index_map[predication2_col0_element];
											uint32_t concept_semtype2_col0_bytes = idx[4]->index_map[predication2_col0_element+1][0] - row_op5[0];
											if(concept_semtype2_col0_bytes) {

												unsigned char* concept_semtype2_col0_ptr = &(idx[4]->fragment_data[0][row_op5[0]]);
												uint32_t concept_semtype2_fragment_size = 0;
												test_smdb_bca_1threads_concept_semtype2_col0_decode_BCA(concept_semtype2_col0_ptr, concept_semtype2_col0_bytes, concept_semtype2_fragment_size);

												for (uint32_t concept_semtype2_it = 0; concept_semtype2_it < concept_semtype2_fragment_size; concept_semtype2_it++) {

													uint32_t concept_semtype2_col0_element = buffer_arrays[4][0][0][0][concept_semtype2_it];

													uint32_t* row_op6 = idx[5]->index_map[concept_semtype2_col0_element];
													uint32_t concept2_col0_bytes = idx[5]->index_map[concept_semtype2_col0_element+1][0] - row_op6[0];
													if(concept2_col0_bytes) {

														unsigned char* concept2_col0_ptr = &(idx[5]->fragment_data[0][row_op6[0]]);
														uint32_t concept2_fragment_size = 0;
														test_smdb_bca_1threads_concept2_col0_decode_BCA(concept2_col0_ptr, concept2_col0_bytes, concept2_fragment_size);

														for (uint32_t concept2_it = 0; concept2_it < concept2_fragment_size; concept2_it++) {
															uint32_t concept2_col0_element = buffer_arrays[5][0][0][0][concept2_it];

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


	for (int j=0; j<metadata.idx_num_encodings[0]; j++) {
		for (int k=0; k<NUM_THREADS; k++) {
			for (int l=0; l<BUFFER_POOL_SIZE; l++) {
				delete[] buffer_arrays[0][j][k][l];
			}
			delete[] buffer_arrays[0][j][k];
		}
	}
	for (int j=0; j<metadata.idx_num_encodings[1]; j++) {
		for (int k=0; k<NUM_THREADS; k++) {
			for (int l=0; l<BUFFER_POOL_SIZE; l++) {
				delete[] buffer_arrays[1][j][k][l];
			}
			delete[] buffer_arrays[1][j][k];
		}
	}
	for (int j=0; j<metadata.idx_num_encodings[2]; j++) {
		for (int k=0; k<NUM_THREADS; k++) {
			for (int l=0; l<BUFFER_POOL_SIZE; l++) {
				delete[] buffer_arrays[2][j][k][l];
			}
			delete[] buffer_arrays[2][j][k];
		}
	}
	for (int j=0; j<metadata.idx_num_encodings[3]; j++) {
		for (int k=0; k<NUM_THREADS; k++) {
			for (int l=0; l<BUFFER_POOL_SIZE; l++) {
				delete[] buffer_arrays[3][j][k][l];
			}
			delete[] buffer_arrays[3][j][k];
		}
	}
	for (int j=0; j<metadata.idx_num_encodings[4]; j++) {
		for (int k=0; k<NUM_THREADS; k++) {
			for (int l=0; l<BUFFER_POOL_SIZE; l++) {
				delete[] buffer_arrays[4][j][k][l];
			}
			delete[] buffer_arrays[4][j][k];
		}
	}
	for (int j=0; j<metadata.idx_num_encodings[5]; j++) {
		for (int k=0; k<NUM_THREADS; k++) {
			for (int l=0; l<BUFFER_POOL_SIZE; l++) {
				delete[] buffer_arrays[5][j][k][l];
			}
			delete[] buffer_arrays[5][j][k];
		}
	}

	delete[] sentence1_bool_array;


	*null_checks = RC;
	return R;

}

#endif

