//
// Created by pooja on 1/18/25.
//
#define FDB_API_VERSION 630
#include <foundationdb/fdb_c.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "tx_conflict.h"

void* run_network(void* arg) {
    fdb_error_t err = fdb_run_network();
    if (err) {
        fprintf(stderr, "Error running network: %s\n", fdb_get_error(err));
    }
    return NULL;
}

void check_error(fdb_error_t err, const char* message) {
    if (err) {
        fprintf(stderr, "%s: %s\n", message, fdb_get_error(err));
        exit(1);
    }
}


void* T1(void* arg) {
    FDBDatabase* db = (FDBDatabase*)arg;
    FDBTransaction* tr;
    fdb_error_t err;

    err = fdb_database_create_transaction(db, &tr);
    check_error(err, "Creating transaction in T1");

    // Get k1
    const char* key1;
    FDBFuture* f;
    fdb_bool_t present;
    const uint8_t* value1;
    int value_length;

    key1 = "k1";
    f = fdb_transaction_get(tr, (uint8_t*)key1, strlen(key1), 0);
    err = fdb_future_block_until_ready(f);
    check_error(err, "Getting value in T1");
    err = fdb_future_get_value(f, &present, &value1, &value_length);
    check_error(err, "Retrieving value in T1");

    if (present) {
        printf("T1: Got value: %.*s\n", value_length, value1);
    } else {
        printf("T1: Key not found\n");
    }

    // Set k2
    const char* key2;
    const char* value2;
    key2 = "k2";
    value2 = "k2_T1";
    fdb_transaction_set(tr, (uint8_t*)key2, strlen(key2), (uint8_t*)value2, strlen(value2));

    // Commit transaction
    f = fdb_transaction_commit(tr);
    err = fdb_future_block_until_ready(f);
    check_error(err, "Committing transaction in T1");


    fdb_future_destroy(f);
    fdb_transaction_destroy(tr);

    return NULL;
}

void* T2(void* arg) {
    FDBDatabase* db = (FDBDatabase*)arg;
    FDBTransaction* tr;
    fdb_error_t err;

    err = fdb_database_create_transaction(db, &tr);
    check_error(err, "Creating transaction in T1");

    // Get k2
    const char* key1;
    FDBFuture* f;
    fdb_bool_t present;
    const uint8_t* value1;
    int value_length;

    key1 = "k2";
    f = fdb_transaction_get(tr, (uint8_t*)key1, strlen(key1), 0);
    err = fdb_future_block_until_ready(f);
    check_error(err, "Getting value in T2");
    err = fdb_future_get_value(f, &present, &value1, &value_length);
    check_error(err, "Retrieving value in T2");

    if (present) {
        printf("T2: Got value: %.*s\n", value_length, value1);
    } else {
        printf("T2: Key not found\n");
    }

    // Set k1
    const char* key2;
    const char* value2;
    key2 = "k1";
    value2 = "k1_T2";
    fdb_transaction_set(tr, (uint8_t*)key2, strlen(key2), (uint8_t*)value2, strlen(value2));

    // Commit transaction
    f = fdb_transaction_commit(tr);
    err = fdb_future_block_until_ready(f);
    check_error(err, "Committing transaction in T1");


    fdb_future_destroy(f);
    fdb_transaction_destroy(tr);

    return NULL;
}

int main() {
    fdb_error_t err = fdb_select_api_version(630);
    check_error(err, "Selecting API version");

    err = fdb_setup_network();
    check_error(err, "Setting up network");

    pthread_t network_thread;
    if (pthread_create(&network_thread, NULL, run_network, NULL)) {
        fprintf(stderr, "Error creating network thread\n");
        return 1;
    }

    FDBDatabase* db;
    err = fdb_create_database(NULL, &db);
    check_error(err, "Creating database");

    pthread_t t1_thread, t2_thread;
    if (pthread_create(&t1_thread, NULL, T1, db)) {
        fprintf(stderr, "Error creating T1 thread\n");
        return 1;
    }
    if (pthread_create(&t2_thread, NULL, T2, db)) {
        fprintf(stderr, "Error creating T2 thread\n");
        return 1;
    }

    pthread_join(t1_thread, NULL);
    pthread_join(t2_thread, NULL);

    fdb_database_destroy(db);

    err = fdb_stop_network();
    check_error(err, "Stopping network");

    pthread_join(network_thread, NULL);

    return 0;
}
