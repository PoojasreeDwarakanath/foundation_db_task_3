//
// Created by pooja on 1/18/25.
//
#define FDB_API_VERSION 630
#include <foundationdb/fdb_c.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "read_snapshot.h"

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

void* T2(void* arg) {
    FDBDatabase* db = (FDBDatabase*)arg;
    FDBTransaction* tr;
    fdb_error_t err;

    err = fdb_database_create_transaction(db, &tr);
    check_error(err, "Creating transaction in T2");

    // Set operation
    const char* key;
    const char* value;

    key = "k4";
    value = "v4_t2";
    fdb_transaction_set(tr, (uint8_t*)key, strlen(key), (uint8_t*)value, strlen(value));

    key = "k2";
    value = "v2_t2";
    fdb_transaction_set(tr, (uint8_t*)key, strlen(key), (uint8_t*)value, strlen(value));

    // Commit transaction
    FDBFuture* f = fdb_transaction_commit(tr);
    err = fdb_future_block_until_ready(f);
    check_error(err, "Committing transaction in T2");

    fdb_future_destroy(f);
    fdb_transaction_destroy(tr);

    printf("T2: Set operation completed\n");

    return NULL;
}

void* T1(void* arg) {
    FDBDatabase* db = (FDBDatabase*)arg;
    FDBTransaction* tr;
    fdb_error_t err;

    err = fdb_database_create_transaction(db, &tr);
    check_error(err, "Creating transaction in T1");

    // Get operation
    const char* key;
    FDBFuture* f;
    fdb_bool_t present;
    const uint8_t* value;
    int value_length;

    key = "k1";
    f = fdb_transaction_get(tr, (uint8_t*)key, strlen(key), 0);
    err = fdb_future_block_until_ready(f);
    check_error(err, "Getting value in T1");
    err = fdb_future_get_value(f, &present, &value, &value_length);
    check_error(err, "Retrieving value in T1");

    if (present) {
        printf("T1: Got value: %.*s\n", value_length, value);
    } else {
        printf("T1: Key not found\n");
    }

    key = "k2";
    f = fdb_transaction_get(tr, (uint8_t*)key, strlen(key), 0);
    err = fdb_future_block_until_ready(f);
    check_error(err, "Getting value in T1");
    err = fdb_future_get_value(f, &present, &value, &value_length);
    check_error(err, "Retrieving value in T1");

    if (present) {
        printf("T1: Got value: %.*s\n", value_length, value);
    } else {
        printf("T1: Key not found\n");
    }

    key = "k3";
    f = fdb_transaction_get(tr, (uint8_t*)key, strlen(key), 0);
    err = fdb_future_block_until_ready(f);
    check_error(err, "Getting value in T1");
    err = fdb_future_get_value(f, &present, &value, &value_length);
    check_error(err, "Retrieving value in T1");

    if (present) {
        printf("T1: Got value: %.*s\n", value_length, value);
    } else {
        printf("T1: Key not found\n");
    }

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
