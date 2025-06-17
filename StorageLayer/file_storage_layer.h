#pragma once
#include <string>
#include <vector>
#include "storage_layer.h"


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

    void create_table(const std::string& table_name) override;
    void drop_table(const std::string& table_name) override;
    bool table_exists(const std::string& table_name) override;
    std::vector<std::string> list_tables() override;
private:
    bool is_open;
    std::string storage_path;
    // Add any other necessary instance variables here
};

