#include "file_storage_layer.h"

FileStorageLayer::FileStorageLayer()
    : is_open(false), storage_path("") {
    // Initialize any other instance variables here
}

FileStorageLayer::~FileStorageLayer() {
    if (is_open) {
        close();
    }
}

void FileStorageLayer::open(const std::string& path) {
    // TODO: Implement this method to open storage at the specified path
    storage_path = path;
	ensure_directory_exists(path);
    is_open = true;
    // Implement storage initialization/opening logic
}

void FileStorageLayer::close() {
    // TODO: Implement this method to close the storage safely
    is_open = false;
    // Implement closing logic
}

int FileStorageLayer::insert(const std::string& table, const std::vector<uint8_t>& record) {
    // TODO: Implement this method to insert a record and return its ID
    // Implement insert logic
    return 0; // Replace with actual implementation
}

std::vector<uint8_t> FileStorageLayer::get(const std::string& table, int record_id) {
    // TODO: Implement this method to retrieve a record by ID
    // Implement retrieval logic
    return std::vector<uint8_t>(); // Replace with actual implementation
}

void FileStorageLayer::update(const std::string& table, int record_id, const std::vector<uint8_t>& updated_record) {
    // TODO: Implement this method to update a record
    // Implement update logic
}

void FileStorageLayer::delete_record(const std::string& table, int record_id) {
    // TODO: Implement this method to delete a record
    // Implement delete logic
}

std::vector<std::vector<uint8_t>> FileStorageLayer::scan(
    const std::string& table,
    const std::optional<std::function<bool(int, const std::vector<uint8_t>&)>>& callback,
    const std::optional<std::vector<int>>& projection,
    const std::optional<std::function<bool(const std::vector<uint8_t>&)>>& filter_func) {

    // TODO: Implement this method to scan records in a table
    // Implement scan logic
    return std::vector<std::vector<uint8_t>>();
}

void FileStorageLayer::flush() {
    // TODO: Implement this method to flush data to disk
    // Implement flush logic
}

bool FileStorageLayer::create_table(const std::string& table_name) {
    if (!is_open) {
		std::cout << "Storage is not open. Cannot create table." << std::endl;
		return false;
    }

    auto tableFile = std::filesystem::path(storage_path) / (table_name + ".db");
    
    if (std::filesystem::exists(tableFile)) {
        std::cout << "Table with such name is already exists!" << std::endl;
        return false;
    }

    std::ofstream page(tableFile, std::ios::binary);

    PageHeader header{ 0, (uint16_t)PAGE_SIZE, 0 };
	page.write(reinterpret_cast<const char*>(&header), sizeof(header));

    if (!page) {
        std::cout << "Failed to create table file." << std::endl;
        return false;
    }
    page.close();
	return true;
}

bool FileStorageLayer::drop_table(const std::string& table_name) {
    if (!is_open) {
        std::cout << "Storage is not open. Cannot drop table." << std::endl;
        return false;
    }
    auto tableFile = std::filesystem::path(storage_path) / (table_name + ".db");
    
    if (!std::filesystem::exists(tableFile)) {
        std::cout << "Table with such name does not exist!" << std::endl;
        return false;
    }
    std::filesystem::remove(tableFile);

	return true;
}

std::vector<std::string> FileStorageLayer::list_tables() {
    if (!is_open) {
        std::cout << "Storage is not open. Cannot list tables." << std::endl;
        return {};
    }
    std::vector<std::string> tables;
    for (const auto& entry : std::filesystem::directory_iterator(storage_path)) {
        if (entry.is_regular_file() && entry.path().extension() == ".db") {
            tables.push_back(entry.path().stem().string());
        }
    }
    return tables;
}

void FileStorageLayer::ensure_directory_exists(const std::string& path) {
	std::filesystem::create_directory(path);
}