#ifndef q5_array_2threads_
#define q5_array_2threads_

#include "../graph_index.hpp"
#include "../graph_global_vars.hpp"

#include <atomic>
#define NUM_THREADS 2

using namespace std;

static args_threading arguments[NUM_THREADS];

static uint32_t doc1_col0_element;

static int* R;
static int* RC;

static pthread_spinlock_t* r_spin_locks;

static uint64_t** doc1_col0_buffer;
static uint64_t** term_col0_buffer;
static uint64_t** doc2_col0_buffer;
static uint64_t** author2_col0_buffer;

extern inline void q5_array_2threads_doc1_col0_decode_UA(uint32_t* doc1_col0_ptr, uint32_t & doc1_fragment_size) __attribute__((always_inline));

extern inline void q5_array_2threads_term_col0_decode_UA(uint32_t* term_col0_ptr, uint32_t & term_fragment_size) __attribute__((always_inline));

void* pthread_q5_array_2threads_worker(void* arguments);

extern inline void q5_array_2threads_doc2_col0_decode_UA_threaded(int thread_id, uint32_t* doc2_col0_ptr, uint32_t & doc2_fragment_size) __attribute__((always_inline));

extern inline void q5_array_2threads_author2_col0_decode_UA_threaded(int thread_id, uint32_t* author2_col0_ptr, uint32_t & author2_fragment_size) __attribute__((always_inline));

void q5_array_2threads_doc1_col0_decode_UA(uint32_t* doc1_col0_ptr, uint32_t & doc1_fragment_size) {

	uint32_t buffer_it = 0;
	for (uint32_t i=0; i<doc1_fragment_size; i++) {
		uint32_t doc_candidate = *doc1_col0_ptr++;

        unsigned char* nodes_frag = idx[0]->index_map[doc_candidate];
        uint32_t* nodes_frag_ptr = reinterpret_cast<uint32_t *>(&(nodes_frag[sizeof(uint32_t)]));
        uint32_t type = *nodes_frag_ptr;
        if (type == DOC_PROPERTY)
        {
            doc1_col0_buffer[0][buffer_it++] = doc_candidate;
		}
	}
    doc1_fragment_size = buffer_it;
}

void q5_array_2threads_term_col0_decode_UA(uint32_t* term_col0_ptr, uint32_t & term_fragment_size) {

	uint32_t buffer_it = 0;
	for (uint32_t i=0; i<term_fragment_size; i++) {
		uint32_t term_candidate = *term_col0_ptr++;

        unsigned char* nodes_frag = idx[0]->index_map[term_candidate];
        uint32_t* nodes_frag_ptr = reinterpret_cast<uint32_t *>(&(nodes_frag[sizeof(uint32_t)]));
        uint32_t type = *nodes_frag_ptr;
        if (type == TERM_PROPERTY)
        {
            term_col0_buffer[0][buffer_it++] = term_candidate;
		}
	}
	term_fragment_size = buffer_it;
}

void* pthread_q5_array_2threads_worker(void* arguments) {

	args_threading* args = (args_threading *) arguments;

	uint32_t term_it = args->start;
	uint32_t term_fragment_size = args->end;
	int thread_id = args->thread_id;

	for (; term_it < term_fragment_size; term_it++) {

		uint32_t term_col0_element = term_col0_buffer[0][term_it];

        unsigned char* frag_doc2 = idx[1]->index_map[term_col0_element];
		if(frag_doc2) {

			uint32_t* doc2_col0_ptr = reinterpret_cast<uint32_t *>(&(frag_doc2[0]));
			uint32_t doc2_fragment_size = *doc2_col0_ptr++;
			q5_array_2threads_doc2_col0_decode_UA_threaded(thread_id, doc2_col0_ptr, doc2_fragment_size);

			for (uint32_t doc2_it = 0; doc2_it < doc2_fragment_size; doc2_it++) {

				uint32_t doc2_col0_element = doc2_col0_buffer[thread_id][doc2_it];

				unsigned char* frag_author2 = idx[1]->index_map[doc2_col0_element];
				if (frag_author2) {

					uint32_t* author2_col0_ptr = reinterpret_cast<uint32_t *>(&(frag_author2[0]));
					uint32_t author2_fragment_size = *author2_col0_ptr++;
					q5_array_2threads_author2_col0_decode_UA_threaded(thread_id, author2_col0_ptr, author2_fragment_size);

					for (uint32_t author2_it = 0; author2_it < author2_fragment_size; author2_it++) {
						uint32_t author2_col0_element = author2_col0_buffer[thread_id][author2_it];

						RC[author2_col0_element] = 1;

						pthread_spin_lock(&r_spin_locks[author2_col0_element]);
						R[author2_col0_element] += 1;
						pthread_spin_unlock(&r_spin_locks[author2_col0_element]);

					}
				}
			}
		}
	}
	return nullptr;
}

void q5_array_2threads_doc2_col0_decode_UA_threaded(int thread_id, uint32_t* doc2_col0_ptr, uint32_t & doc2_fragment_size) {

	uint32_t buffer_it = 0;
	for (uint32_t i=0; i<doc2_fragment_size; i++) {
		uint32_t doc_candidate = *doc2_col0_ptr++;

        unsigned char* nodes_frag = idx[0]->index_map[doc_candidate];
        uint32_t* nodes_frag_ptr = reinterpret_cast<uint32_t *>(&(nodes_frag[sizeof(uint32_t)]));
        uint32_t type = *nodes_frag_ptr;
        if (type == DOC_PROPERTY)
        {
            doc2_col0_buffer[thread_id][buffer_it++] = doc_candidate;
		}
	}
    doc2_fragment_size = buffer_it;
}

void q5_array_2threads_author2_col0_decode_UA_threaded(int thread_id, uint32_t* author2_col0_ptr, uint32_t & author2_fragment_size) {

	uint32_t buffer_it = 0;
	for (uint32_t i=0; i<author2_fragment_size; i++) {
		uint32_t author_candidate = *author2_col0_ptr++;

        unsigned char* nodes_frag = idx[0]->index_map[author_candidate];
        uint32_t* nodes_frag_ptr = reinterpret_cast<uint32_t *>(&(nodes_frag[sizeof(uint32_t)]));
        uint32_t type = *nodes_frag_ptr;
        if (type == AUTHOR_PROPERTY)
        {
            author2_col0_buffer[thread_id][buffer_it++] = author_candidate;
		}
	}

	author2_fragment_size = buffer_it;
}

extern "C" int* q5_array_2threads(int** null_checks, int author1) {

	benchmark_t1 = chrono::steady_clock::now();

	int max_frag;

	max_frag = metadata.idx_max_fragment_sizes[1];
	doc1_col0_buffer = new uint64_t*[NUM_THREADS];
	for (int i=0; i<NUM_THREADS; i++) {
		doc1_col0_buffer[i] = new uint64_t[max_frag];
	}

	max_frag = metadata.idx_max_fragment_sizes[1];
	term_col0_buffer = new uint64_t*[NUM_THREADS];
	for (int i=0; i<NUM_THREADS; i++) {
		term_col0_buffer[i] = new uint64_t[max_frag];
	}

	max_frag = metadata.idx_max_fragment_sizes[1];
	doc2_col0_buffer = new uint64_t*[NUM_THREADS];
	for (int i=0; i<NUM_THREADS; i++) {
		doc2_col0_buffer[i] = new uint64_t[max_frag];
	}

	max_frag = metadata.idx_max_fragment_sizes[1];
	author2_col0_buffer = new uint64_t*[NUM_THREADS];
	for (int i=0; i<NUM_THREADS; i++) {
		author2_col0_buffer[i] = new uint64_t[max_frag];
	}

	RC = new int[metadata.idx_domains[1][0]]();
	R = new int[metadata.idx_domains[1][0]]();

	r_spin_locks = spin_locks[1];


	uint64_t author1_list[1];
	author1_list[0] = author1+MAX_DOC_ID+MAX_TERM_ID_MESH;

	for (int author1_it = 0; author1_it<1; author1_it++) {

		uint64_t author1_col0_element = author1_list[author1_it];

		unsigned char* frag_doc1 = idx[1]->index_map[author1_col0_element];
		if(frag_doc1) {

			uint32_t* doc1_col0_ptr = reinterpret_cast<uint32_t *>(&(frag_doc1[0]));
			uint32_t doc1_fragment_size = *doc1_col0_ptr++;
			q5_array_2threads_doc1_col0_decode_UA(doc1_col0_ptr, doc1_fragment_size);

			for (uint32_t doc1_it = 0; doc1_it < doc1_fragment_size; doc1_it++) {

				doc1_col0_element = doc1_col0_buffer[0][doc1_it];

				unsigned char* frag_term = idx[1]->index_map[doc1_col0_element];
				if(frag_term) {

					uint32_t* term_col0_ptr = reinterpret_cast<uint32_t *>(&(frag_term[0]));
					uint32_t term_fragment_size = *term_col0_ptr++;
					q5_array_2threads_term_col0_decode_UA(term_col0_ptr, term_fragment_size);

					uint32_t thread_size = term_fragment_size/NUM_THREADS;
					uint32_t position = 0;

					for (int i=0; i<NUM_THREADS; i++) {
						arguments[i].start = position;
						position += thread_size;
						arguments[i].end = position;
						arguments[i].thread_id = i;
					}
					arguments[NUM_THREADS-1].end = term_fragment_size;

					for (int i=0; i<NUM_THREADS; i++) {
						pthread_create(&threads[i], NULL, &pthread_q5_array_2threads_worker, (void *) &arguments[i]);
					}

					for (int i=0; i<NUM_THREADS; i++) {
						pthread_join(threads[i], NULL);
					}
				}
			}
		}
	}


	for (int i=0; i<NUM_THREADS; i++) {
		delete[] doc1_col0_buffer[i];
	}
	delete[] doc1_col0_buffer;

	for (int i=0; i<NUM_THREADS; i++) {
		delete[] term_col0_buffer[i];
	}
	delete[] term_col0_buffer;

	for (int i=0; i<NUM_THREADS; i++) {
		delete[] doc2_col0_buffer[i];
	}
	delete[] doc2_col0_buffer;


	for (int i=0; i<NUM_THREADS; i++) {
		delete[] author2_col0_buffer[i];
	}
	delete[] author2_col0_buffer;


	*null_checks = RC;
	return R;

}

#endif

