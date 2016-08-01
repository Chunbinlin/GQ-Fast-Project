#ifndef q1_array_1threads_
#define q1_array_1threads_

#include "../graph_index.hpp"
#include "../graph_global_vars.hpp"

#include <atomic>
#define NUM_THREADS 1

using namespace std;

static args_threading arguments[NUM_THREADS];


static int* R;
static int* RC;

static pthread_spinlock_t* r_spin_locks;

static uint64_t** term1_col0_buffer;
static uint64_t** doc2_col0_buffer;

extern inline void q1_array_1threads_term1_col0_decode_UA(uint32_t* term1_col0_ptr, uint32_t & term1_fragment_size) __attribute__((always_inline));

void* pthread_q1_array_1threads_worker(void* arguments);

extern inline void q1_array_1threads_doc2_col0_decode_UA_threaded(int thread_id, uint32_t* doc2_col0_ptr, uint32_t & doc2_fragment_size) __attribute__((always_inline));

void q1_array_1threads_term1_col0_decode_UA(uint32_t* term1_col0_ptr, uint32_t & term1_fragment_size) {


    uint32_t buffer_it = 0;
	for (uint32_t i=0; i<term1_fragment_size; i++) {
		uint32_t term_candidate = *term1_col0_ptr++;

        unsigned char* nodes_frag = idx[0]->index_map[term_candidate];
        uint32_t* nodes_frag_ptr = reinterpret_cast<uint32_t *>(&(nodes_frag[sizeof(uint32_t)]));
        uint32_t type = *nodes_frag_ptr;
        if (type == TERM_PROPERTY)
        {
            term1_col0_buffer[0][buffer_it++] = term_candidate;
		}
	}
	term1_fragment_size = buffer_it;
}

void* pthread_q1_array_1threads_worker(void* arguments) {

	args_threading* args = (args_threading *) arguments;

	uint32_t term1_it = args->start;
	uint32_t term1_fragment_size = args->end;
	int thread_id = args->thread_id;

	for (; term1_it < term1_fragment_size; term1_it++) {

		uint32_t term1_col0_element = term1_col0_buffer[0][term1_it];

		unsigned char* frag_doc2 = idx[1]->index_map[term1_col0_element];
		if(frag_doc2) {

			uint32_t* doc2_frag_ptr = reinterpret_cast<uint32_t *>(&(frag_doc2[0]));
			uint32_t doc2_frag_size = *doc2_frag_ptr;
			doc2_frag_ptr++;
			q1_array_1threads_doc2_col0_decode_UA_threaded(thread_id, doc2_frag_ptr, doc2_frag_size);

			for (uint32_t doc2_it = 0; doc2_it < doc2_frag_size; doc2_it++) {
				uint32_t doc2_col0_element = doc2_col0_buffer[thread_id][doc2_it];

				RC[doc2_col0_element] = 1;

				pthread_spin_lock(&r_spin_locks[doc2_col0_element]);
				R[doc2_col0_element] += 1;
				pthread_spin_unlock(&r_spin_locks[doc2_col0_element]);

			}
		}
	}
	return nullptr;
}

void q1_array_1threads_doc2_col0_decode_UA_threaded(int thread_id, uint32_t* doc2_col0_ptr, uint32_t & doc2_fragment_size) {

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

extern "C" int* q1_array_1threads_10296795(int** null_checks, int doc1) {

	benchmark_t1 = chrono::steady_clock::now();

	int max_frag;

	max_frag = metadata.idx_max_fragment_sizes[1];
	term1_col0_buffer = new uint64_t*[NUM_THREADS];
	for (int i=0; i<NUM_THREADS; i++) {
		term1_col0_buffer[i] = new uint64_t[max_frag];
	}

	max_frag = metadata.idx_max_fragment_sizes[1];
	doc2_col0_buffer = new uint64_t*[NUM_THREADS];
	for (int i=0; i<NUM_THREADS; i++) {
		doc2_col0_buffer[i] = new uint64_t[max_frag];
	}

	RC = new int[metadata.idx_domains[1][0]]();
	R = new int[metadata.idx_domains[1][0]]();

	r_spin_locks = spin_locks[1];


	uint64_t doc1_list[1];
	doc1_list[0] = doc1;

	for (int doc1_it = 0; doc1_it<1; doc1_it++) {

		uint64_t doc1_col0_element = doc1_list[doc1_it];

		unsigned char* frag_term1 = idx[1]->index_map[doc1_col0_element];
		if(frag_term1) {

			uint32_t* frag_term1_ptr = reinterpret_cast<uint32_t *>(&(frag_term1[0]));
			uint32_t term1_frag_size = *frag_term1_ptr;
			frag_term1_ptr++;

			q1_array_1threads_term1_col0_decode_UA(frag_term1_ptr, term1_frag_size);

			uint32_t thread_size = term1_frag_size/NUM_THREADS;
			uint32_t position = 0;

			for (int i=0; i<NUM_THREADS; i++) {
				arguments[i].start = position;
				position += thread_size;
				arguments[i].end = position;
				arguments[i].thread_id = i;
			}
			arguments[NUM_THREADS-1].end = term1_frag_size;

			for (int i=0; i<NUM_THREADS; i++) {
				pthread_create(&threads[i], NULL, &pthread_q1_array_1threads_worker, (void *) &arguments[i]);
			}

			for (int i=0; i<NUM_THREADS; i++) {
				pthread_join(threads[i], NULL);
			}
		}
	}


	for (int i=0; i<NUM_THREADS; i++) {
		delete[] term1_col0_buffer[i];
	}
	delete[] term1_col0_buffer;
	for (int i=0; i<NUM_THREADS; i++) {
		delete[] doc2_col0_buffer[i];
	}
	delete[] doc2_col0_buffer;


	*null_checks = RC;
	return R;

}

#endif

