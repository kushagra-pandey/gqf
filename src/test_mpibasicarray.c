#include <mpi.h>
#include <stdio.h>
#include "include/gqf.h"
#include "include/gqf_int.h"
#include "include/gqf_file.h"

#define MAX_VALUE(nbits) ((1ULL << (nbits)) - 1)
#define BITMASK(nbits)                                    \
  ((nbits) == 64 ? 0xffffffffffffffff : MAX_VALUE(nbits))
int insertarr(int* arr, int val, int index, int size, int freq) {
    if (index + freq - 1 >= size) {
        return -1;
    }
    for (int i = index; i < index + freq; i++) {
        arr[index] = val;
    }
    
    return index + freq;
}
int main(int argc, char** argv) {
    MPI_Init(NULL, NULL);
    int rank;
    int size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    //printf("Hello: rank %d, size %d\n",rank, size);
    int processorBits = ceil_log2(size);
    
	
    /*QF qf;
    uint64_t qbits = atoi(argv[1]);
    uint64_t freq = 4;
    uint64_t nhashbits = qbits + 8;
    uint64_t nslots = (1ULL << qbits);
    uint64_t nvals = 750*nslots/1000;
    nvals = nvals/freq;
    */
    uint64_t qbits = atoi(argv[1]);
    uint64_t freq = 4;
    uint64_t nslots = (1ULL << qbits);
    uint64_t nvals = 750*nslots/1000;
    nvals = nvals/freq;
    int* qf = malloc(sizeof(int) * (nslots));
    int curIndex = 0;

    /*if (!qf_malloc(&qf, nslots, nhashbits, 0, QF_HASH_INVERTIBLE, 0)) {
            fprintf(stderr, "Can't allocate CQF.\n");
            abort();
    }*/


    
    uint64_t *vals;
    nvals = (uint64_t) ((nvals / size) * 0.9);
    vals = (uint64_t*)malloc(nvals*sizeof(vals[0]));
        //RAND_bytes((unsigned char *)vals, sizeof(*vals) * nvals);
    srand(rank);
    for (uint64_t i = 0; i < nvals; i++) {
        vals[i] = (1 * rand());
        /*vals[i] = rand() % qf.metadata->range;*/
        /*fprintf(stdout, "%lx\n", vals[i]);*/
    }

    /* Insert keys in the CQF */
    for (uint64_t i = 0; i < nvals; i++) {
        //int ret = qf_insert(&qf, vals[i], 0, freq, QF_NO_LOCK);
        int ret = insertarr(qf, vals[i], curIndex, nslots, freq);
        if (ret < 0) {
            fprintf(stderr, "failed insertion for key: %lx %d.\n", vals[i], 50);
            abort();
        }
        curIndex = ret;
    }

    /* Lookup inserted keys and counts. 
    for (uint64_t i = 0; i < nvals; i++) {
        uint64_t count = qf_count_key_value(&qf, vals[i], 0, 0);
        if (count < freq) {
            fprintf(stderr, "failed lookup after insertion for %lx %ld.\n", vals[i],
                            count);
            abort();
        }
    }*/
    printf("Finished querying cqf in rank %d\n", rank);




    MPI_Finalize();
}