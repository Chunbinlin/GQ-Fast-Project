#ifndef test_smdb_array_1threads_
#define test_smdb_array_1threads_

#include "../fastr_index.hpp"
#include "../global_vars.hpp"

#define NUM_THREADS 1
#define BUFFER_POOL_SIZE 1

using namespace std;

static int* R;
static int* RC;

static bool* sentence1_bool_array;

extern inline void test_smdb_array_1threads_concept_semtype1_col0_decode_UA(int32_t* concept_semtype1_col0_ptr, int32_t concept_semtype1_col0_bytes, int32_t & concept_semtype1_fragment_size) __attribute__((always_inline));

extern inline void test_smdb_array_1threads_predication1_col0_decode_UA(int32_t* predication1_col0_ptr, int32_t predication1_col0_bytes, int32_t & predication1_fragment_size) __attribute__((always_inline));

extern inline void test_smdb_array_1threads_sentence1_col0_decode_UA(int32_t* sentence1_col0_ptr, int32_t sentence1_col0_bytes, int32_t & sentence1_fragment_size) __attribute__((always_inline));

extern inline void test_smdb_array_1threads_predication2_col0_decode_UA(int32_t* predication2_col0_ptr, int32_t predication2_col0_bytes, int32_t & predication2_fragment_size) __attribute__((always_inline));

extern inline void test_smdb_array_1threads_concept_semtype2_col0_decode_UA(int32_t* concept_semtype2_col0_ptr, int32_t concept_semtype2_col0_bytes, int32_t & concept_semtype2_fragment_size) __attribute__((always_inline));

extern inline void test_smdb_array_1threads_concept2_col0_decode_UA(int32_t* concept2_col0_ptr, int32_t concept2_col0_bytes, int32_t & concept2_fragment_size) __attribute__((always_inline));

void test_smdb_array_1threads_concept_semtype1_col0_decode_UA(int32_t* concept_semtype1_col0_ptr, int32_t concept_semtype1_col0_bytes, int32_t & concept_semtype1_fragment_size) {

	concept_semtype1_fragment_size = concept_semtype1_col0_bytes/4;

	for (int32_t i=0; i<concept_semtype1_fragment_size; i++) {
		buffer_arrays[0][0][0][0][i] = *concept_semtype1_col0_ptr++;
	}
}

void test_smdb_array_1threads_predication1_col0_decode_UA(int32_t* predication1_col0_ptr, int32_t predication1_col0_bytes, int32_t & predication1_fragment_size) {

	predication1_fragment_size = predication1_col0_bytes/4;

	for (int32_t i=0; i<predication1_fragment_size; i++) {
		buffer_arrays[1][0][0][0][i] = *predication1_col0_ptr++;
	}
}

void test_smdb_array_1threads_sentence1_col0_decode_UA(int32_t* sentence1_col0_ptr, int32_t sentence1_col0_bytes, int32_t & sentence1_fragment_size) {

	sentence1_fragment_size = sentence1_col0_bytes/4;

	for (int32_t i=0; i<sentence1_fragment_size; i++) {
		buffer_arrays[2][0][0][0][i] = *sentence1_col0_ptr++;
	}
}

void test_smdb_array_1threads_predication2_col0_decode_UA(int32_t* predication2_col0_ptr, int32_t predication2_col0_bytes, int32_t & predication2_fragment_size) {

	predication2_fragment_size = predication2_col0_bytes/4;

	for (int32_t i=0; i<predication2_fragment_size; i++) {
		buffer_arrays[3][0][0][0][i] = *predication2_col0_ptr++;
	}
}

void test_smdb_array_1threads_concept_semtype2_col0_decode_UA(int32_t* concept_semtype2_col0_ptr, int32_t concept_semtype2_col0_bytes, int32_t & concept_semtype2_fragment_size) {

	concept_semtype2_fragment_size = concept_semtype2_col0_bytes/4;

	for (int32_t i=0; i<concept_semtype2_fragment_size; i++) {
		buffer_arrays[4][0][0][0][i] = *concept_semtype2_col0_ptr++;
	}
}

void test_smdb_array_1threads_concept2_col0_decode_UA(int32_t* concept2_col0_ptr, int32_t concept2_col0_bytes, int32_t & concept2_fragment_size) {

	concept2_fragment_size = concept2_col0_bytes/4;

	for (int32_t i=0; i<concept2_fragment_size; i++) {
		buffer_arrays[5][0][0][0][i] = *concept2_col0_ptr++;
	}
}

extern "C" int* test_smdb_array_1threads(int** null_checks) {

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

	int64_t* concept1_list = new int64_t[1];
	concept1_list[0] = 2019;

	for (int concept1_it = 0; concept1_it<1; concept1_it++) {

		int64_t concept1_col0_element = concept1_list[concept1_it];

		uint32_t* row_concept_semtype1 = idx[0]->index_map[concept1_col0_element];
		int32_t concept_semtype1_col0_bytes = idx[0]->index_map[concept1_col0_element+1][0] - row_concept_semtype1[0];
		if(concept_semtype1_col0_bytes) {

			int32_t* concept_semtype1_col0_ptr = reinterpret_cast<int32_t *>(&(idx[0]->fragment_data[0][row_concept_semtype1[0]]));
			int32_t concept_semtype1_fragment_size = 0;
			test_smdb_array_1threads_concept_semtype1_col0_decode_UA(concept_semtype1_col0_ptr, concept_semtype1_col0_bytes, concept_semtype1_fragment_size);

			for (int32_t concept_semtype1_it = 0; concept_semtype1_it < concept_semtype1_fragment_size; concept_semtype1_it++) {

				int32_t concept_semtype1_col0_element = buffer_arrays[0][0][0][0][concept_semtype1_it];

				uint32_t* row_predication1 = idx[1]->index_map[concept_semtype1_col0_element];
				int32_t predication1_col0_bytes = idx[1]->index_map[concept_semtype1_col0_element+1][0] - row_predication1[0];
				if(predication1_col0_bytes) {

					int32_t* predication1_col0_ptr = reinterpret_cast<int32_t *>(&(idx[1]->fragment_data[0][row_predication1[0]]));
					int32_t predication1_fragment_size = 0;
					test_smdb_array_1threads_predication1_col0_decode_UA(predication1_col0_ptr, predication1_col0_bytes, predication1_fragment_size);

					for (int32_t predication1_it = 0; predication1_it < predication1_fragment_size; predication1_it++) {

						int32_t predication1_col0_element = buffer_arrays[1][0][0][0][predication1_it];

						uint32_t* row_sentence1 = idx[2]->index_map[predication1_col0_element];
						int32_t sentence1_col0_bytes = idx[2]->index_map[predication1_col0_element+1][0] - row_sentence1[0];
						if(sentence1_col0_bytes) {

							int32_t* sentence1_col0_ptr = reinterpret_cast<int32_t *>(&(idx[2]->fragment_data[0][row_sentence1[0]]));
							int32_t sentence1_fragment_size = 0;
							test_smdb_array_1threads_sentence1_col0_decode_UA(sentence1_col0_ptr, sentence1_col0_bytes, sentence1_fragment_size);

							for (int32_t sentence1_it = 0; sentence1_it < sentence1_fragment_size; sentence1_it++) {


								if (!(sentence1_bool_array[buffer_arrays[2][0][0][0][sentence1_it]])) {
									sentence1_bool_array[buffer_arrays[2][0][0][0][sentence1_it]] = true;
									int32_t sentence1_col0_element = buffer_arrays[2][0][0][0][sentence1_it];

									uint32_t* row_predication2 = idx[3]->index_map[sentence1_col0_element];
									int32_t predication2_col0_bytes = idx[3]->index_map[sentence1_col0_element+1][0] - row_predication2[0];
									if(predication2_col0_bytes) {

										int32_t* predication2_col0_ptr = reinterpret_cast<int32_t *>(&(idx[3]->fragment_data[0][row_predication2[0]]));
										int32_t predication2_fragment_size = 0;
										test_smdb_array_1threads_predication2_col0_decode_UA(predication2_col0_ptr, predication2_col0_bytes, predication2_fragment_size);

										for (int32_t predication2_it = 0; predication2_it < predication2_fragment_size; predication2_it++) {

											int32_t predication2_col0_element = buffer_arrays[3][0][0][0][predication2_it];

											uint32_t* row_concept_semtype2 = idx[4]->index_map[predication2_col0_element];
											int32_t concept_semtype2_col0_bytes = idx[4]->index_map[predication2_col0_element+1][0] - row_concept_semtype2[0];
											if(concept_semtype2_col0_bytes) {

												int32_t* concept_semtype2_col0_ptr = reinterpret_cast<int32_t *>(&(idx[4]->fragment_data[0][row_concept_semtype2[0]]));
												int32_t concept_semtype2_fragment_size = 0;
												test_smdb_array_1threads_concept_semtype2_col0_decode_UA(concept_semtype2_col0_ptr, concept_semtype2_col0_bytes, concept_semtype2_fragment_size);

												for (int32_t concept_semtype2_it = 0; concept_semtype2_it < concept_semtype2_fragment_size; concept_semtype2_it++) {

													int32_t concept_semtype2_col0_element = buffer_arrays[4][0][0][0][concept_semtype2_it];

													uint32_t* row_concept2 = idx[5]->index_map[concept_semtype2_col0_element];
													int32_t concept2_col0_bytes = idx[5]->index_map[concept_semtype2_col0_element+1][0] - row_concept2[0];
													if(concept2_col0_bytes) {

														int32_t* concept2_col0_ptr = reinterpret_cast<int32_t *>(&(idx[5]->fragment_data[0][row_concept2[0]]));
														int32_t concept2_fragment_size = 0;
														test_smdb_array_1threads_concept2_col0_decode_UA(concept2_col0_ptr, concept2_col0_bytes, concept2_fragment_size);

														for (int32_t concept2_it = 0; concept2_it < concept2_fragment_size; concept2_it++) {
															int32_t concept2_col0_element = buffer_arrays[5][0][0][0][concept2_it];

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

