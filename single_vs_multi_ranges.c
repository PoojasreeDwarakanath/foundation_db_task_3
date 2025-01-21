#define FDB_API_VERSION 630
#include <foundationdb/fdb_c.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

void* run_network(void* arg) {
    fdb_error_t err = fdb_run_network();
    if (err) {
        fprintf(stderr, "Error running network: %s\n", fdb_get_error(err));
    }
    return NULL;
}

typedef struct {
    const FDBKeyValue* kv;
    int count;
    fdb_bool_t more;
} RangeResult;

int main() {
    // Initialize FDB
    fdb_error_t err = fdb_select_api_version(630);
    if (err) {
        fprintf(stderr, "Error selecting API version: %s\n", fdb_get_error(err));
        return 1;
    }

    err = fdb_setup_network();
    if (err) {
        fprintf(stderr, "Error setting up network: %s\n", fdb_get_error(err));
        return 1;
    }

    // Start network thread
    pthread_t network_thread;
    if (pthread_create(&network_thread, NULL, run_network, NULL)) {
        fprintf(stderr, "Error creating network thread\n");
        return 1;
    }

    // Create database handle
    FDBDatabase *db;
    err = fdb_create_database(NULL, &db);
    if (err) {
        fprintf(stderr, "Error creating database: %s\n", fdb_get_error(err));
        return 1;
    }

    // Define ranges
    #define NUM_RANGES 10
    char ranges[][2][20] = {
        {"0", "1000"},
        {"1000", "2000"},
        {"2000", "3000"},
        {"3000", "4000"},
        {"4000", "5000"},
        {"5000", "6000"},
        {"6000", "7000"},
        {"7000", "8000"},
        {"8000", "9000"},
        {"9000", "10000"}
    };

    // Create arrays to store futures, transactions, and results
    FDBFuture* futures[NUM_RANGES];
    FDBTransaction* transactions[NUM_RANGES];
    RangeResult results[NUM_RANGES];

    // Start timing
    clock_t start = clock();

    // Start all range queries asynchronously with separate transactions
    for (int i = 0; i < NUM_RANGES; i++) {
        // Create a new transaction for each range
        err = fdb_database_create_transaction(db, &transactions[i]);
        if (err) {
            fprintf(stderr, "Error creating transaction %d: %s\n", i, fdb_get_error(err));
            continue;
        }
//		Trying the 3 modes for getRange
        futures[i] = fdb_transaction_get_range(transactions[i],
            (const uint8_t*)ranges[i][0], strlen(ranges[i][0]), 0, 1,
            (const uint8_t*)ranges[i][1], strlen(ranges[i][1]), 0, 1,
            0, 0, FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0);

//        futures[i] = fdb_transaction_get_range(transactions[i],
//            (const uint8_t*)ranges[i][0], strlen(ranges[i][0]), 0, 1,
//            (const uint8_t*)ranges[i][1], strlen(ranges[i][1]), 0, 1,
//            0, 0, FDB_STREAMING_MODE_ITERATOR, 1, 0, 0);
//
//        futures[i] = fdb_transaction_get_range(transactions[i],
//            (const uint8_t*)ranges[i][0], strlen(ranges[i][0]), 0, 1,
//            (const uint8_t*)ranges[i][1], strlen(ranges[i][1]), 0, 1,
//            10, 0, FDB_STREAMING_MODE_EXACT, 0, 0, 0);


    }

    // Process results as they become available
    for (int i = 0; i < NUM_RANGES; i++) {
        err = fdb_future_block_until_ready(futures[i]);
        if (err) {
            fprintf(stderr, "Error waiting for future %d: %s\n", i, fdb_get_error(err));
            continue;
        }

        err = fdb_future_get_keyvalue_array(futures[i],
                                          &results[i].kv,
                                          &results[i].count,
                                          &results[i].more);
        if (err) {
            fprintf(stderr, "Error getting key-value array %d: %s\n", i, fdb_get_error(err));
            continue;
        }

        // Uncomment to print results
        /*
        printf("Range %d results:\n", i);
        for (int j = 0; j < results[i].count; j++) {
            printf("Key: %.*s, Value: %.*s\n",
                   (int)results[i].kv[j].key_length,
                   (char*)results[i].kv[j].key,
                   (int)results[i].kv[j].value_length,
                   (char*)results[i].kv[j].value);
        }
        */
    }

    clock_t end = clock();
    double cpu_time_used = ((double) (end - start)) * 1000 / CLOCKS_PER_SEC;
    printf("Response time for async get range: %f milliseconds\n", cpu_time_used);

    for (int i = 0; i < NUM_RANGES; i++) {
        fdb_future_destroy(futures[i]);
        fdb_transaction_destroy(transactions[i]);
    }

    fdb_database_destroy(db);

    err = fdb_stop_network();
    if (err) {
        fprintf(stderr, "Error stopping network: %s\n", fdb_get_error(err));
        return 1;
    }

    pthread_join(network_thread, NULL);

    return 0;
}
