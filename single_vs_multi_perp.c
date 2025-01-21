#define FDB_API_VERSION 630
#include <foundationdb/fdb_c.h>
#include <stdio.h>
#include <string.h>

int main() {
    // Initialize the FoundationDB API
    fdb_select_api_version(630);
    fdb_setup_network();
    fdb_run_network();

    FDBDatabase *db;
    fdb_create_database(NULL, &db);

    FDBTransaction *tr;
    fdb_database_create_transaction(db, &tr);

    // Convert the range bounds to strings
    const char *begin_key = "2000";
    const char *end_key = "3001"; // Use 3001 to include 3000

    // Set up the range query
    int limit = 0; // 0 means no limit
    int target_bytes = 0; // 0 means no target bytes limit
    FDBFuture *f = fdb_transaction_get_range(tr,
                                             (const uint8_t *)begin_key, strlen(begin_key), 0, 1,
                                             (const uint8_t *)end_key, strlen(end_key), 0, 1,
                                             limit,
                                             target_bytes,
                                             FDB_STREAMING_MODE_WANT_ALL,
                                             0, 0, 0);

    // Wait for the result
    fdb_future_block_until_ready(f);

    // Get the key-value pairs
    const FDBKeyValue *kv;
    int count;
    int more;
    fdb_error_t err = fdb_future_get_keyvalue_array(f, &kv, &count, &more);
    if (err) {
        printf("Error: %s\n", fdb_get_error(err));
    } else {
        // Process the results
        for (int i = 0; i < count; i++) {
            printf("Key: %.*s, Value: %.*s\n",
                   (int)kv[i].key_length, kv[i].key,
                   (int)kv[i].value_length, kv[i].value);
        }
    }

    // Clean up
    fdb_future_destroy(f);
    fdb_transaction_destroy(tr);
    fdb_database_destroy(db);
    fdb_stop_network();

    return 0;
}
