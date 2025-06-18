#pragma once
#include <string>
#include <vector>
#include <functional>
#include <optional>
#include <filesystem>
#include <fstream>
#include <iostream>
#include "storage_layer.h"

static const int PAGE_SIZE = 4096; // Size of a page in bytes
static const uint16_t DELETE_SLOT = 0xFFFF; // Special value to indicate a deleted slot

struct PageHeader {
	uint16_t slot_count; // Number of slots in the page
	uint16_t free_space_offset; // Offset to the next free space in the page
};

//enum class DataType {
//    INT,
//    VARCHAR,
//	UNKNOWN
//};
//
//struct Column {
//    std::string name;
//    DataType type;
//    size_t size; // Size for VARCHAR, ignored for INT
//};
//
//struct TableSchema {
//    std::string name;
//    std::vector<Column> columns;
//};

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

    bool create_table(const std::string& table_name/*, const std::vector<Column>& table_schema*/);
    bool drop_table(const std::string& table_name);
    std::vector<std::string> list_tables();
	bool vacuum(const std::string& table_name); // willbe implemented later
private:
	void ensure_directory_exists(const std::string& path);
	bool is_table_exists(const std::string& table_name) const;
	int make_record_id(uint16_t page, uint16_t slot) const;
	void split_record_id(int record_id, uint16_t& page, uint16_t& slot);
    bool is_open;
    std::string storage_path;

	//std::unordered_map<std::string, TableSchema> table_schemas;
	//bool save_table_schema(const std::string& table_name);
	//bool load_table_schema(const std::string& table_name);
    //std::vector<uint8_t> encode_record(const std::string& table, const std::vector<std::string>& values);
};

