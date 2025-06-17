#pragma once

#include <string>
#include <vector>
#include <functional>
#include <optional>

/**
 * Abstract base class that defines the interface for a simple storage system.
 */
class StorageLayer {
public:
    virtual ~StorageLayer() = default;

    /**
     * Initialize or open existing storage at the given path.
     */
    virtual void open(const std::string& path) = 0;

    /**
     * Close storage safely and ensure all data is persisted.
     */
    virtual void close() = 0;

    /**
     * Insert a new record into the specified table, returning a unique record ID.
     */
    virtual int insert(const std::string& table, const std::vector<uint8_t>& record) = 0;

    /**
     * Retrieve a record by its unique ID from the specified table.
     */
    virtual std::vector<uint8_t> get(const std::string& table, int record_id) = 0;

    /**
     * Update an existing record identified by record ID.
     */
    virtual void update(const std::string& table, int record_id, const std::vector<uint8_t>& updated_record) = 0;

    /**
     * Delete a record identified by its unique ID.
     */
    virtual void delete_record(const std::string& table, int record_id) = 0;

    /**
     * Scan records in a table optionally using projection and filter. Callback is optional.
     */
    virtual std::vector<std::vector<uint8_t>> scan(
        const std::string& table,
        const std::optional<std::function<bool(int, const std::vector<uint8_t>&)>>& callback = std::nullopt,
        const std::optional<std::vector<int>>& projection = std::nullopt,
        const std::optional<std::function<bool(const std::vector<uint8_t>&)>>& filter_func = std::nullopt) = 0;

    /**
     * Persist all buffered data immediately to disk.
     */
    virtual void flush() = 0;

	virtual void create_table(const std::string& table_name) = 0;

    virtual void drop_table(const std::string& table_name) = 0;
    virtual bool table_exists(const std::string& table_name) = 0;
	virtual std::vector<std::string> list_tables() = 0;
};

/**
 * Example implementation of the StorageLayer interface.
 * Students should fill in the method implementations.
 */
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