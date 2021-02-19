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
    /*if (rank == 0) {
    	printf("Hello world!\n");
	printf("The number of bits of the processors is %d", processorBits);
    }*/
	
    QF qf; //every process gets its own quotient filter

    
    uint64_t qbits = 10; //arbitrary value
    uint64_t freq = 4; //freq of each object
    uint64_t nhashbits = qbits + 8; //remainder bits also arbitrarily set at 8
    uint64_t nslots = (1ULL << qbits);
    uint64_t nvals = 750*nslots/1000;
    nvals = nvals/freq;
	


    uint64_t localqbits = qbits - processorBits;
    uint64_t localhashbits = localqbits + 8;
    uint64_t localslots = (1ULL << localqbits);
    if (!qf_initfile(&qf, localslots, localhashbits /* since we are using 4 processes, and the qf will be partitioned into 4 8 bit qfs*/, 0, QF_HASH_INVERTIBLE, 0,
                                     "mycqf.file")) {
        fprintf(stderr, "Can't allocate CQF.\n");
        abort();
    }

    
    qf_set_auto_resize(&qf, true);
    
    uint64_t* arr = malloc(sizeof(arr[0]) * (nvals / size));
    
    srand(rank); //different seed for each process

    for (int i = 0; i < nvals / size; i++) {
    	arr[i] = rand() % nslots; //different seedfor each process, so the numbers are different
    }
    printf("Process %d, first value is %d\n", rank, arr[0]);

    int endofarr = (nvals / size);
    int i = 0;
    while (i < endofarr) {
    	//uint64_t hashbucket = qf_gethashbucket(&qf, arr[i]);
	
	uint64_t hash = hash_64(arr[i], BITMASK(nhashbits));
	
	uint32_t processName = hash >> (nhashbits - processorBits);
	uint64_t localhash = hash & BITMASK(nhashbits - processorBits);
	if (processName == rank) {
		qf_inserthash(&qf, localhash, arr[i],0, freq, QF_NO_LOCK);
	} else {
		
	}
	i++;
    }



    MPI_Finalize();
}
