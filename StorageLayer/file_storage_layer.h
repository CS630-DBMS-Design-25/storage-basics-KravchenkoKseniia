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

struct PageHeader {
	uint16_t slot_count; // Number of slots in the page
	uint16_t free_space_offset; // Offset to the next free space in the page
	uint32_t reserved; // Reserved for future use
};

class FileStorageLayer : public StorageLayer {
public:
    FileStorageLayer();
    ~FileStorageLayer() override;

    void open(const std::string& path) override;
    void close() override;
    int insert(const std::string& table, const std::vector<uint8_t>& record) override;
    std::vector<uint8_t> get(const std::string& table, int record_id) override;
    void update(const std::string& table, int record_id, const std::vector<uint8_t>& updated_record) override;
    void delete_record(const std::string& table, int record_id) override;
    std::vector<std::vector<uint8_t>> scan(
        const std::string& table,
        const std::optional<std::function<bool(int, const std::vector<uint8_t>&)>>& callback = std::nullopt,
        const std::optional<std::vector<int>>& projection = std::nullopt,
        const std::optional<std::function<bool(const std::vector<uint8_t>&)>>& filter_func = std::nullopt) override;
    void flush() override;

    bool create_table(const std::string& table_name);
    bool drop_table(const std::string& table_name);
    std::vector<std::string> list_tables();
private:
	void ensure_directory_exists(const std::string& path);
    bool is_open;
    std::string storage_path;
};

