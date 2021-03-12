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
    /*if (!qf_initfile(&qf, localslots, localhashbits, 0, QF_HASH_INVERTIBLE, 0,
                                     "mycqf.file")) {
        fprintf(stderr, "Can't allocate CQF.\n");
        abort();
    }*/
    if (!qf_malloc(&qf, localslots, localhashbits, 0, QF_HASH_INVERTIBLE, 0)) {
		fprintf(stderr, "Can't allocate CQF.\n");
		abort();
	}

    
    //qf_set_auto_resize(&qf, true);
    nvals = (uint64_t) ((nvals / size) * 0.9);
    uint64_t* arr = malloc(sizeof(arr[0]) * nvals);
    
    srand(rank); //different seed for each process
    for (int i = 0; i < nvals; i++) {
    	arr[i] = rand() % nslots; //different seed for each process, so the numbers are different
    }
    printf("Process %d, first value is %d\n", rank, arr[0]);

    int buffer_send_length = nvals; //20 vals in each process bucket
    int* buffer_send = (int*) malloc(sizeof(int) * (buffer_send_length+1) * size);
    for (int i = 0; i < (buffer_send_length+1)*size;i++) {
    	buffer_send[i] = 0;
    }
    int counts_send[size];
    for (int i = 0; i < size; i++) {
    	counts_send[i] = buffer_send_length + 1;
    }
    int displacements_send[size];
    for (int i = 0; i < size; i++) {
    	displacements_send[i] = (buffer_send_length + 1) * i;
    }

    int* buffer_recv = (int*) malloc(sizeof(int) * (buffer_send_length+1) * size);
    for (int i = 0; i < (buffer_send_length+1)*size;i++) {
        buffer_recv[i] = 0;
    }
    int counts_recv[size];
    for (int i = 0; i < size; i++) {
        counts_recv[i] = buffer_send_length + 1;
    }
    int displacements_recv[size];
    for (int i = 0; i < size; i++) {
        displacements_recv[i] = (buffer_send_length + 1) * i;
    }



    int i = 0;

    while (i < nvals) {
    	//uint64_t hashbucket = qf_gethashbucket(&qf, arr[i]);
	
	uint64_t hash = hash_64(arr[i], BITMASK(nhashbits));
	
	uint32_t processName = hash >> (nhashbits - processorBits);
	uint64_t localhash = hash & BITMASK(nhashbits - processorBits);
	if (processName == rank) {
		//int ret = qf_inserthash(&qf, localhash, arr[i],0, freq, QF_NO_LOCK);
		//int ret = qf_insert(&qf, arr[i], 0, freq, QF_NO_LOCK);
		int ret = qf_insert(&qf, localhash, 0, freq, QF_NO_LOCK | QF_KEY_IS_HASH);
        if (ret < 0) {
			printf("Num successful: %d\n", i);
			if (ret == QF_NO_SPACE)
				printf("CQF is full.\n");
			else if (ret == QF_COULDNT_LOCK)
				printf("TRY_ONCE_LOCK failed.\n");
			else
				printf("Does not recognise return value.\n");
			break;
		}
		/*
		 ????
		 Why does insert keep failing?
		 
		 */
	} else {
		//add to bucket
		buffer_send[(buffer_send_length + 1) * processName]++;
		int offset = buffer_send[(buffer_send_length + 1) * processName];
		int index = (buffer_send_length + 1) * processName + offset;
		buffer_send[index] = arr[i];		
	}
	i++;
    }
    MPI_Alltoallv(buffer_send, counts_send, displacements_send, MPI_INT, buffer_recv, counts_recv, displacements_recv, MPI_INT, MPI_COMM_WORLD);

    /*int numElements = buffer_recv[(buffer_send_length + 1) * rank];
    int start = (buffer_send_length + 1) * rank + 1;
    for (int i = start; i < start + numElements;i++) {
    	//int ret = qf_inserthash(&qf, localhash, arr[i],0, freq, QF_NO_LOCK);
                uint64_t hash = hash_64(buffer_recv[i], BITMASK(nhashbits));
                uint32_t processName = hash >> (nhashbits - processorBits);
                uint64_t localhash = hash & BITMASK(nhashbits - processorBits);
                int ret = qf_insert(&qf, localhash, 0, freq, QF_NO_LOCK | QF_KEY_IS_HASH);
                if (ret < 0) {
                        printf("Num successful: %d\n", i);
                        if (ret == QF_NO_SPACE)
                                printf("CQF is full.\n");
                        else if (ret == QF_COULDNT_LOCK)
                                printf("TRY_ONCE_LOCK failed.\n");
                        else
                                printf("Does not recognise return value.\n");
                        break;
                }
    } */

    //now, insert all the buffer_recv elements
    for(int i = 0; i < size; i++) {
        if (i == rank) {
            continue;
        }
        int start = (buffer_send_length + 1) * i + 1;
        int numElements = buffer_recv[(buffer_send_length + 1) * i];
        for (int i = start; i < start + numElements;i++) {
    	//int ret = qf_inserthash(&qf, localhash, arr[i],0, freq, QF_NO_LOCK);
                uint64_t hash = hash_64(buffer_recv[i], BITMASK(nhashbits));
                uint32_t processName = hash >> (nhashbits - processorBits);
                uint64_t localhash = hash & BITMASK(nhashbits - processorBits);
                int ret = qf_insert(&qf, localhash, 0, freq, QF_NO_LOCK | QF_KEY_IS_HASH);
                if (ret < 0) {
                        printf("Num successful: %d\n", i);
                        if (ret == QF_NO_SPACE)
                                printf("CQF is full.\n");
                        else if (ret == QF_COULDNT_LOCK)
                                printf("TRY_ONCE_LOCK failed.\n");
                        else
                                printf("Does not recognise return value.\n");
                        break;
                }
        }

    }
	




    /* Accuracy test */

    for (int i = 0; i < nvals; i++) {
	    uint64_t hash = hash_64(arr[i], BITMASK(nhashbits));
        uint32_t processName = hash >> (nhashbits - processorBits);
        uint64_t localhash = hash & BITMASK(nhashbits - processorBits);
        if (processName == rank) {
		
    		uint64_t count = qf_count_key_value(&qf, localhash, 0, QF_KEY_IS_HASH);
            if (count < freq) {
                printf("Failed insertion in normal array iteration for key %d, frequency %d: got count %d\n", arr[i], freq, count);
            }
	    }
    }
    /*
    //int numElements = buffer_recv[(buffer_send_length + 1) * rank];
    //int start = (buffer_send_length + 1) * rank + 1;
    for (int i = start; i < start + numElements;i++) {
        //int ret = qf_inserthash(&qf, localhash, arr[i],0, freq, QF_NO_LOCK);
        uint64_t hash = hash_64(buffer_recv[i], BITMASK(nhashbits));
        uint32_t processName = hash >> (nhashbits - processorBits);
        uint64_t localhash = hash & BITMASK(nhashbits - processorBits);

    	uint64_t count = qf_count_key_value(&qf, localhash, 0, QF_KEY_IS_HASH);
        if (count < freq) {
        	printf("Failed insertion for key %d, frequency %d: got count %d\n", buffer_recv[i], freq, count);
        }
    }
    */
     for(int i = 0; i < size; i++) {
        if (i == rank) {
            continue;
        }
        int start = (buffer_send_length + 1) * i + 1;
        int numElements = buffer_recv[(buffer_send_length + 1) * i];
        for (int i = start; i < start + numElements;i++) {
    	//int ret = qf_inserthash(&qf, localhash, arr[i],0, freq, QF_NO_LOCK);
                uint64_t hash = hash_64(buffer_recv[i], BITMASK(nhashbits));
                uint32_t processName = hash >> (nhashbits - processorBits);
                uint64_t localhash = hash & BITMASK(nhashbits - processorBits);
                uint64_t count = qf_count_key_value(&qf, localhash, 0, QF_KEY_IS_HASH);
                if (count < freq) {
        	        printf("Failed insertion for key %d, frequency %d: got count %d\n", buffer_recv[i], freq, count);
                }
        }

    }



    MPI_Finalize();
}
