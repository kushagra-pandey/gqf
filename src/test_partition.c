#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <pthread.h>
//include <openssl/rand.h>

#include "include/gqf.h"
#include "include/gqf_int.h"
#include "include/gqf_file.h"

#define MAX_VALUE(nbits) ((1ULL << (nbits)) - 1)
#define BITMASK(nbits)                                    \
  ((nbits) == 64 ? 0xffffffffffffffff : MAX_VALUE(nbits))

int main(int argc, char **argv)
{


    time_t start_t, end_t;
    double diff_t;

    printf("Starting of the program...\n");
    time(&start_t);

    QF qf;
    uint64_t qbits = 24;
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
    printf("Finished querying cqf\n");
    time(&end_t);
    diff_t = difftime(end_t, start_t);

    printf("Execution time = %f\n", diff_t);
    printf("Exiting of the program...\n");


    /* Now, we create 4 quotient filters each with 8 quotient bits, and for each query we use the top 2 bits
        of the hash to identify the qf and the rest of the bits as the hash */
    
    QF qf1;
    QF qf2; 
    QF qf3;
    QF qf4;

    qbits = qbits - 2;
    nhashbits = qbits + 8;
    nslots = (1ULL << qbits);

    if (!qf_malloc(&qf1, nslots, nhashbits, 0, QF_HASH_INVERTIBLE, 0)) {
            fprintf(stderr, "Can't allocate CQF.\n");
            abort();
    }
    if (!qf_malloc(&qf2, nslots, nhashbits, 0, QF_HASH_INVERTIBLE, 0)) {
            fprintf(stderr, "Can't allocate CQF.\n");
            abort();
    }
    if (!qf_malloc(&qf3, nslots, nhashbits, 0, QF_HASH_INVERTIBLE, 0)) {
            fprintf(stderr, "Can't allocate CQF.\n");
            abort();
    }
    if (!qf_malloc(&qf4, nslots, nhashbits, 0, QF_HASH_INVERTIBLE, 0)) {
            fprintf(stderr, "Can't allocate CQF.\n");
            abort();
    }
    QF qfarr[4];
    qfarr[0] = qf1;
    qfarr[1] = qf2;
    qfarr[2] = qf3;
    qfarr[3] = qf4;

    nhashbits = nhashbits + 2;
    uint64_t processorBits = 2;

    /* Insert keys in the CQF */
    for (uint64_t i = 0; i < nvals; i++) {

        uint64_t hash = hash_64(vals[i], BITMASK(nhashbits));
	
	    uint32_t processName = hash >> (nhashbits - processorBits);
	    uint64_t localhash = hash & BITMASK(nhashbits - processorBits);


        int ret = qf_insert(&(qfarr[processName]), localhash, 0, freq, QF_NO_LOCK | QF_KEY_IS_HASH);

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
        uint64_t hash = hash_64(vals[i], BITMASK(nhashbits));
	
	    uint32_t processName = hash >> (nhashbits - processorBits);
	    uint64_t localhash = hash & BITMASK(nhashbits - processorBits);

        uint64_t count = qf_count_key_value(&(qfarr[processName]), localhash, 0, QF_KEY_IS_HASH);
        if (count < freq) {
            fprintf(stderr, "failed lookup after insertion for %lx %ld.\n", vals[i],
                            count);
            abort();
        }
    }
    printf("Finished querying partitioned cqf\n");


}
