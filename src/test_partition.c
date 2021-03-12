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

int main(int argc, char **argv)
{



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
    srand(0);
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
}
