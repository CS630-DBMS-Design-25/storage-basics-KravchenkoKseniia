#pragma once
#include <string>
#include <vector>
#include <functional>
#include <optional>
#include <filesystem>
#include <fstream>
#include <iostream>
#include "storage_layer.h"
#include "table_schema.h"

static const int PAGE_SIZE = 4096; // Size of a page in bytes
static const uint16_t DELETE_SLOT = 0xFFFF; // Special value to indicate a deleted slot
static const int INDEX_BUCKET_SIZE = 1024; // size of each index bucket in bytes

struct PageHeader {
	uint16_t slot_count; // Number of slots in the page
	uint16_t free_space_offset; // Offset to the next free space in the page
};


class FileStorageLayer : public StorageLayer {
public:
    FileStorageLayer();
    ~FileStorageLayer() override;

    void open(const std::string& path) override;
    void close() override;
    int insert(const std::string& table, const std::vector<uint8_t>& record) override;
    std::vector<uint8_t> get(const std::string& table, int record_id) override;
    bool update(const std::string& table, int record_id, const std::vector<uint8_t>& updated_record) override;
    bool delete_record(const std::string& table, int record_id) override;
    std::vector<std::vector<uint8_t>> scan(
        const std::string& table,
        const std::optional<std::function<bool(int, const std::vector<uint8_t>&)>>& callback = std::nullopt,
        const std::optional<std::vector<int>>& projection = std::nullopt,
        const std::optional<std::function<bool(const std::vector<uint8_t>&)>>& filter_func = std::nullopt) override;

    bool create_table(const std::string& table_name, const TableSchema& schema);
    bool drop_table(const std::string& table_name);
    std::vector<std::string> list_tables();

    TableSchema get_table_schema(const std::string& table_name) const;

    std::vector<int> find(const std::string& table_name, const std::string& key);
private:
    bool is_open;
	bool is_vacuum;
    std::string storage_path;

	std::unordered_map<std::string, TableSchema> table_schemas;
	std::unordered_map<std::string, std::vector<std::vector<int>>> index_buckets;

    bool vacuum(const std::string& table_name);

	void ensure_directory_exists(const std::string& path);
	bool is_table_exists(const std::string& table_name) const;

	int make_record_id(uint16_t page, uint16_t slot) const;
	void split_record_id(int record_id, uint16_t& page, uint16_t& slot);

	void load_table_schemas();

	void load_index_buckets(const std::string& table_name);
    void save_index_buckets(const std::string& table_name);
    std::string get_key(const std::string& table_name, const std::vector<uint8_t>& record);
};

