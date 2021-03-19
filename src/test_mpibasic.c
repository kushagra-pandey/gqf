#include <mpi.h>
#include <stdio.h>
#include "include/gqf.h"
#include "include/gqf_int.h"
#include "include/gqf_file.h"

#define MAX_VALUE(nbits) ((1ULL << (nbits)) - 1)
#define BITMASK(nbits)                                    \
  ((nbits) == 64 ? 0xffffffffffffffff : MAX_VALUE(nbits))

int main(int argc, char** argv) {
    MPI_Init(NULL, NULL);
    int rank;
    int size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    //printf("Hello: rank %d, size %d\n",rank, size);
    int processorBits = ceil_log2(size);
    
	
    /*QF qf; //every process gets its own quotient filter

    
    uint64_t qbits = 10; //arbitrary value
    uint64_t freq = 4; //freq of each object
    uint64_t nhashbits = qbits + 8; //remainder bits also arbitrarily set at 8
    uint64_t nslots = (1ULL << qbits);
    uint64_t nvals = 750*nslots/1000;
    nvals = nvals/freq;
	


    uint64_t localqbits = qbits - processorBits;
    uint64_t localhashbits = localqbits + 8;
    uint64_t localslots = (1ULL << localqbits);
    
    if (!qf_malloc(&qf, localslots, localhashbits, 0, QF_HASH_INVERTIBLE, 0)) {
		fprintf(stderr, "Can't allocate CQF.\n");
		abort();
	}

    
    //qf_set_auto_resize(&qf, true);
    
    //nvals = (uint64_t) ((nvals / size) * 0.9);

    uint64_t* arr = malloc(sizeof(arr[0]) * nvals);
    
    srand(rank); //different seed for each process
    for (int i = 0; i < nvals; i++) {
    	arr[i] = rand() % qf.metadata->range; 
    }
    printf("Process %d, first value is %d\n", rank, arr[0]);


    int i = 0;

    while (i < nvals) {
    	//uint64_t hashbucket = qf_gethashbucket(&qf, arr[i]);
	
	    int ret = qf_insert(&qf, arr[i], 0, freq, QF_NO_LOCK);

	    i++;
    }
    printf("all insertions before alltoallv successful for process %d\n\n", rank);

    for(int i = 0; i < nvals; i++) {

    }
    */




    QF qf;
    uint64_t qbits = 10;
    uint64_t freq = 4;
    uint64_t nhashbits = qbits + 8;
    uint64_t nslots = (1ULL << qbits);
    uint64_t nvals = 750*nslots/1000;
    nvals = nvals/freq;

    if (!qf_malloc(&qf, nslots, nhashbits, 0, QF_HASH_INVERTIBLE, 0)) {
            fprintf(stderr, "Can't allocate CQF.\n");
            abort();
    }


    /* First, a sanity test to make sure a gqf works */
    uint64_t *vals;
    vals = (uint64_t*)malloc(nvals*sizeof(vals[0]));
        //RAND_bytes((unsigned char *)vals, sizeof(*vals) * nvals);
    srand(1);
    for (uint64_t i = 0; i < nvals; i++) {
        vals[i] = (1 * rand()) % qf.metadata->range;
        /*vals[i] = rand() % qf.metadata->range;*/
        /*fprintf(stdout, "%lx\n", vals[i]);*/
    }

    /* Insert keys in the CQF */
    for (uint64_t i = 0; i < nvals; i++) {
        int ret = qf_insert(&qf, vals[i], 0, freq, QF_NO_LOCK);
        if (ret < 0) {
            fprintf(stderr, "failed insertion for key: %lx %d.\n", vals[i], 50);
            if (ret == QF_NO_SPACE)
                fprintf(stderr, "CQF is full.\n");
            else if (ret == QF_COULDNT_LOCK)
                fprintf(stderr, "TRY_ONCE_LOCK failed.\n");
            else
                fprintf(stderr, "Does not recognise return value.\n");
            abort();
        }
    }

    /* Lookup inserted keys and counts. */
    for (uint64_t i = 0; i < nvals; i++) {
        uint64_t count = qf_count_key_value(&qf, vals[i], 0, 0);
        if (count < freq) {
            fprintf(stderr, "failed lookup after insertion for %lx %ld.\n", vals[i],
                            count);
            abort();
        }
    }
    printf("Finished querying cqf in rank %d\n", rank);




    MPI_Finalize();
}