//
// Created by pooja on 1/18/25.
//
#define FDB_API_VERSION 630
#include "single_vs_multi_ranges.h"
#include <foundationdb/fdb_c.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "single_get_range.h"



void* run_network(void* arg) {
    fdb_error_t err = fdb_run_network();
    if (err) {
        fprintf(stderr, "Error running network: %s\n", fdb_get_error(err));
    }
    return NULL;
}

typedef struct {
    FDBDatabase *db;
    int thread_id;
    uint8_t begin_key;
    uint8_t end_key;
} ThreadArgs;


void* run_get_range(void* arg) {
    ThreadArgs* args = (ThreadArgs*)arg;
    FDBDatabase *db = args->db;
    int thread_id = args->thread_id;
    const uint8_t begin_key = args->begin_key;
    const uint8_t end_key = args->end_key;

    FDBTransaction *tr_get_range;
    fdb_error_t err = fdb_database_create_transaction(db, &tr_get_range);
    if (err) {
        fprintf(stderr, "Thread %d: Error creating transaction for get: %s\n", thread_id, fdb_get_error(err));
        return NULL;
    }

//    const uint8_t begin_key[] = {0x00};
//    const uint8_t end_key[] = {0xff};

    printf("Begin key %d ", begin_key);
    printf("End key %d\n", end_key);

    FDBFuture *f_range = fdb_transaction_get_range(tr_get_range,
                                                   &begin_key, sizeof(uint8_t), 0, 1,
                                                   &end_key, sizeof(uint8_t), 0, 1,
                                                   0, 0, FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0);

    err = fdb_future_block_until_ready(f_range);
    if (err) {
        fprintf(stderr, "Thread %d: Error waiting for getRange: %s\n", thread_id, fdb_get_error(err));
        return NULL;
    }

    const FDBKeyValue *kv;
    int count = 1000;
    fdb_bool_t more;
    err = fdb_future_get_keyvalue_array(f_range, &kv, &count, &more);
    if (err) {
        fprintf(stderr, "Thread %d: Error getting key-value array: %s\n", thread_id, fdb_get_error(err));
        return NULL;
    }

    printf("Thread %d: Range results:\n", thread_id);

//    count=2;
//    if (count == 0) {
//        printf("Thread %d: No results found in the specified range\n", thread_id);
//    } else {
//        for (int i = 0; i < count; i++) {
//            printf("Thread %d: Key: %.*s, Value: %.*s\n",
//                   thread_id,
//                   (int)kv[i].key_length, (char*)kv[i].key,
//                   (int)kv[i].value_length, (char*)kv[i].value);
//        }
//    }

    fdb_future_destroy(f_range);
    fdb_transaction_destroy(tr_get_range);

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

    pthread_t get_range_threads[10];
    ThreadArgs thread_args[10];

    for (int i = 0; i < 10; i++) {
        thread_args[i].db = db;
        thread_args[i].thread_id = i;
//        thread_args[i].begin_key = (const uint8_t[]){(uint8_t)(i*1000)};
//        thread_args[i].end_key = (const uint8_t[]){(uint8_t) (i*1000+1000)};

        thread_args[i].begin_key = i*1000;
        thread_args[i].end_key = (i*1000)+1000;
        printf("Setting Begin key %d, end key %d\n", thread_args[i].begin_key, thread_args[i].end_key);
        if (pthread_create(&get_range_threads[i], NULL, run_get_range, (&thread_args[i]))) {
            fprintf(stderr, "Error creating get_range thread %d\n", i);
            return 1;
        }
    }

    for (int i = 0; i < 10; i++) {
        pthread_join(get_range_threads[i], NULL);
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
