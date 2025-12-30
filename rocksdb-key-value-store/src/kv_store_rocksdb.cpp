// Standard libraries
#include <iostream>
#include <string>
#include <vector>
#include <memory>

// CSV parsing
#include "csv.hpp"

// RocksDB
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/write_batch.h>

using std::cerr;
using std::cout;
using std::endl;
using std::string;
using std::vector;

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Slice;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

/**
 * RocksDB-backed KV Store
 *
 * Key schema:
 *   <id>_<column_name>  ->  <cell_value>
 *
 * Example:
 *   42_display_name -> "datascience"
 */

// Create/open a RocksDB instance and bulk-load CSV rows into it.
DB* build_kv_store_from_csv(const string& csv_file_path, const string& db_path) {
    csv::CSVReader reader(csv_file_path);

    // Header names (column names)
    const vector<string> header = reader.get_col_names();
    if (header.empty()) {
        cerr << "CSV header is empty. Cannot build KV store." << endl;
        return nullptr;
    }

    DB* db = nullptr;

    Options options;
    options.create_if_missing = true;
    options.error_if_exists = false;

    Status openStatus = DB::Open(options, db_path, &db);
    if (!openStatus.ok()) {
        cerr << "Error opening RocksDB at " << db_path << ": "
             << openStatus.ToString() << endl;
        return nullptr;
    }

    WriteBatch batch;

    // Bulk load: for each row, create keys "<id>_<colName>"
    for (csv::CSVRow& row : reader) {
        // Assumes there is an "id" column in the CSV
        string id_value;
        try {
            id_value = row["id"].get<string>();
        } catch (...) {
            // Skip malformed rows without id
            continue;
        }

        if (id_value.empty()) continue;

        for (size_t col = 0; col < header.size(); ++col) {
            const string& col_name = header[col];

            string key = id_value + "_" + col_name;

            string value;
            try {
                value = row[col].get<string>();
            } catch (...) {
                value.clear();
            }

            batch.Put(key, value);
        }
    }

    Status writeStatus = db->Write(WriteOptions(), &batch);
    if (!writeStatus.ok()) {
        cerr << "Error writing batch to RocksDB: " << writeStatus.ToString() << endl;
        delete db;
        return nullptr;
    }

    return db;
}

// MultiGet wrapper: returns values aligned with input keys.
// Missing keys produce an empty string in the corresponding position.
vector<string> kv_multiget(DB* db, const vector<string>& keys) {
    vector<string> values;
    if (!db || keys.empty()) return values;

    vector<Slice> key_slices;
    key_slices.reserve(keys.size());
    for (const auto& k : keys) key_slices.emplace_back(k);

    values.resize(keys.size());

    vector<Status> statuses = db->MultiGet(ReadOptions(), key_slices, &values);
    for (size_t i = 0; i < statuses.size(); ++i) {
        if (!statuses[i].ok()) values[i].clear();
    }
    return values;
}

// Scan a key range and return only values for keys ending in "_display_name".
// start_id and end_id represent the ID portion (prefix) boundaries.
vector<string> scan_display_names_in_id_range(DB* db, const string& start_id, const string& end_id) {
    vector<string> result;
    if (!db) return result;

    std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(ReadOptions()));

    // Seek to the first key >= "<start_id>"
    it->Seek(start_id);

    while (it->Valid()) {
        const string key = it->key().ToString();

        // Parse "<id>_<column>"
        const size_t pos = key.find('_');
        if (pos == string::npos) {
            it->Next();
            continue;
        }

        const string id_part = key.substr(0, pos);

        // Stop when out of range (lexicographic: assumes id values are comparable as strings)
        if (id_part > end_id) break;
        if (id_part < start_id) {
            it->Next();
            continue;
        }

        // Only include display_name values
        if (key.find("_display_name", pos) != string::npos) {
            result.push_back(it->value().ToString());
        }

        it->Next();
    }

    return result;
}

// Delete a specific key from the KV store.
Status kv_delete(DB* db, const string& key) {
    if (!db) return Status::InvalidArgument("DB pointer is null");
    return db->Delete(WriteOptions(), key);
}
