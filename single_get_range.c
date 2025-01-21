//
// Created by pooja on 1/17/25.
//

#include "single_get_range.h"
#define FDB_API_VERSION 630
#include <foundationdb/fdb_c.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>

void* run_network(void* arg) {
    fdb_error_t err = fdb_run_network();
    if (err) {
        fprintf(stderr, "Error running network: %s\n", fdb_get_error(err));
    }
    return NULL;
}

int main() {
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

    pthread_t network_thread;
    if (pthread_create(&network_thread, NULL, run_network, NULL)) {
        fprintf(stderr, "Error creating network thread\n");
        return 1;
    }

    FDBDatabase *db;
    err = fdb_create_database(NULL, &db);
    if (err) {
        fprintf(stderr, "Error creating database: %s\n", fdb_get_error(err));
        return 1;
    }

    // New transaction for getting the range
    FDBTransaction *tr_get_range;
    err = fdb_database_create_transaction(db, &tr_get_range);
    if (err) {
        fprintf(stderr, "Error creating transaction for get: %s\n", fdb_get_error(err));
        return 1;
    }

    const uint8_t begin_key[] = {0x00};
    const uint8_t end_key[] = {0xff};

    clock_t start, end;
    double cpu_time_used;

    FDBFuture *f_range;
    const FDBKeyValue *kv;
    int count;
    fdb_bool_t more;

    // WANT_ALL mode
    start = clock();
    f_range = fdb_transaction_get_range(tr_get_range,
                                                   begin_key, 1, 0, 1,
                                                   end_key, 1, 0, 1,
                                                   0, 0, FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0);


    err = fdb_future_block_until_ready(f_range);
    if (err) {
        fprintf(stderr, "Error waiting for getRange: %s\n", fdb_get_error(err));
        return 1;
    }

    err = fdb_future_get_keyvalue_array(f_range, &kv, &count, &more);
    if (err) {
        fprintf(stderr, "Error getting key-value array: %s\n", fdb_get_error(err));
        return 1;
    }

    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    printf("Response time for get range WANTALL mode: %f seconds\n", cpu_time_used);

    printf("Range results:\n");
    for (int i = 0; i < count; i++) {
        printf("Key: %.*s, Value: %.*s\n",
               (int)kv[i].key_length, kv[i].key,
               (int)kv[i].value_length, kv[i].value);
    }

    // ITERATOR mode
    start = clock();
    f_range = fdb_transaction_get_range(tr_get_range,
                                                   begin_key, 1, 0, 1,
                                                   end_key, 1, 0, 1,
                                                   0, 0, FDB_STREAMING_MODE_ITERATOR, 1, 0, 0);


    err = fdb_future_block_until_ready(f_range);
    if (err) {
        fprintf(stderr, "Error waiting for getRange: %s\n", fdb_get_error(err));
        return 1;
    }

    err = fdb_future_get_keyvalue_array(f_range, &kv, &count, &more);
    if (err) {
        fprintf(stderr, "Error getting key-value array: %s\n", fdb_get_error(err));
        return 1;
    }

    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    printf("Response time for get range ITERATOR mode: %f seconds\n", cpu_time_used);


    // EXACT mode
    start = clock();
    f_range = fdb_transaction_get_range(tr_get_range,
                                                   begin_key, 1, 0, 1,
                                                   end_key, 1, 0, 1,
                                                   10, 0, FDB_STREAMING_MODE_EXACT, 0, 0, 0);


    err = fdb_future_block_until_ready(f_range);
    if (err) {
        fprintf(stderr, "Error waiting for getRange: %s\n", fdb_get_error(err));
        return 1;
    }

    err = fdb_future_get_keyvalue_array(f_range, &kv, &count, &more);
    if (err) {
        fprintf(stderr, "Error getting key-value array: %s\n", fdb_get_error(err));
        return 1;
    }

    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    printf("Response time for get range EXACT mode: %f seconds\n", cpu_time_used);


    fdb_future_destroy(f_range);
    fdb_transaction_destroy(tr_get_range);
    fdb_database_destroy(db);

    err = fdb_stop_network();
    if (err) {
        fprintf(stderr, "Error stopping network: %s\n", fdb_get_error(err));
        return 1;
    }

    pthread_join(network_thread, NULL);

    return 0;
}