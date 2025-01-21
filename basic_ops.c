//
// Created by pooja on 1/17/25.
//


#define FDB_API_VERSION 630
#include "basic_ops.h"
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

    FDBTransaction *tr_set;
    err = fdb_database_create_transaction(db, &tr_set);
    if (err) {
        fprintf(stderr, "Error creating transaction: %s\n", fdb_get_error(err));
        return 1;
    }

    const char *key = "mk";
    const char *value = "world";
    fdb_transaction_set(tr_set, (uint8_t*)key, strlen(key), (uint8_t*)value, strlen(value));

    FDBFuture *f = fdb_transaction_commit(tr_set);
    err = fdb_future_block_until_ready(f);
    if (err) {
        fprintf(stderr, "Error waiting for commit: %s\n", fdb_get_error(err));
        return 1;
    }

    err = fdb_future_get_error(f);
    if (err) {
        fprintf(stderr, "Error committing transaction: %s\n", fdb_get_error(err));
        return 1;
    }

    printf("Value set successfully\n");

    // New transaction for getting the value
    FDBTransaction *tr_get;
    err = fdb_database_create_transaction(db, &tr_get);
    if (err) {
        fprintf(stderr, "Error creating transaction for get: %s\n", fdb_get_error(err));
        return 1;
    }

    FDBFuture *f_read = fdb_transaction_get(tr_get, (uint8_t*)key, strlen(key), 0);
    err = fdb_future_block_until_ready(f_read);
    if (err) {
        fprintf(stderr, "Error waiting for get: %s\n", fdb_get_error(err));
        return 1;
    }

    fdb_bool_t present;
    uint8_t const *read_value;
    int value_length;
    err = fdb_future_get_value(f_read, &present, &read_value, &value_length);
    if (err) {
        fprintf(stderr, "Error getting value: %s\n", fdb_get_error(err));
        return 1;
    }

    if (present) {
        printf("Value for key '%s': %.*s\n", key, value_length, read_value);
    } else {
        printf("Key '%s' not found\n", key);
    }

    // New transaction for getting the range
    FDBTransaction *tr_get_range;
    err = fdb_database_create_transaction(db, &tr_get_range);
    if (err) {
        fprintf(stderr, "Error creating transaction for get: %s\n", fdb_get_error(err));
        return 1;
    }

    // Perform getRange operation
    const char *begin_key = "a";
    const char *end_key = "n";  // exclusive
    FDBFuture *f_range = fdb_transaction_get_range(tr_get_range,
                                                   (uint8_t*)begin_key, strlen(begin_key), 0, 1,
                                                   (uint8_t*)end_key, strlen(end_key), 0, 1,
                                                   0, 0, FDB_STREAMING_MODE_WANT_ALL, 0, 0, 0);

    err = fdb_future_block_until_ready(f_range);
    if (err) {
        fprintf(stderr, "Error waiting for getRange: %s\n", fdb_get_error(err));
        return 1;
    }

    const FDBKeyValue *kv;
    int count;
    fdb_bool_t more;
    err = fdb_future_get_keyvalue_array(f_range, &kv, &count, &more);
    if (err) {
        fprintf(stderr, "Error getting key-value array: %s\n", fdb_get_error(err));
        return 1;
    }

    printf("Range results:\n");
    for (int i = 0; i < count; i++) {
        printf("Key: %.*s, Value: %.*s\n",
               (int)kv[i].key_length, kv[i].key,
               (int)kv[i].value_length, kv[i].value);
    }

    // Transaction for deleting a value
    FDBTransaction *tr_delete;
    err = fdb_database_create_transaction(db, &tr_delete);
    if (err) {
        fprintf(stderr, "Error creating transaction for delete: %s\n", fdb_get_error(err));
        return 1;
    }

    // Delete the key "mk"
    const char *key_to_delete = "mk";
    fdb_transaction_clear(tr_delete, (uint8_t*)key_to_delete, strlen(key_to_delete));

    FDBFuture *f_delete = fdb_transaction_commit(tr_delete);
    err = fdb_future_block_until_ready(f_delete);
    if (err) {
        fprintf(stderr, "Error waiting for delete commit: %s\n", fdb_get_error(err));
        return 1;
    }

    err = fdb_future_get_error(f_delete);
    if (err) {
        fprintf(stderr, "Error committing delete transaction: %s\n", fdb_get_error(err));
        return 1;
    }

    fdb_future_destroy(f);
    fdb_future_destroy(f_read);
    fdb_transaction_destroy(tr_set);
    fdb_transaction_destroy(tr_get);
    fdb_transaction_destroy(tr_delete);
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