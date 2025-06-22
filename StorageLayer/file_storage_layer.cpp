#include "file_storage_layer.h"


// MAIN CLASS IMPLEMENTATION

FileStorageLayer::FileStorageLayer()
    : is_open(false), is_vacuum(false), storage_path("") {
}

FileStorageLayer::~FileStorageLayer() {
    if (is_open) {
        close();
    }
}

void FileStorageLayer::open(const std::string& path) {
    storage_path = path;
	ensure_directory_exists(path);
	load_table_schemas(); // Load existing table schemas if any
    is_open = true;
}

void FileStorageLayer::close() {
    is_open = false;
}

int FileStorageLayer::insert(const std::string& table, const std::vector<uint8_t>& record) {
    if (!is_open) {
        std::cout << "Storage is not open. Cannot insert record." << std::endl;
        return -1;
	}

    if (!is_table_exists(table)) {
        std::cout << "Table does not exist." << std::endl;
        return -1;
	}

	auto tableFile = std::filesystem::path(storage_path) / (table + ".db");

    int recordId;

    {
        std::fstream page(tableFile, std::ios::binary | std::ios::in | std::ios::out);

        if (!page.is_open()) {
            std::cout << "Failed to open table file." << std::endl;
            return -1;
        }

        // Prepare a buffer for the record
        uint32_t record_size = record.size();
        std::vector<uint8_t> buffer(sizeof(record_size) + record_size); // Buffer to hold record size and data
        std::memcpy(buffer.data(), &record_size, sizeof(record_size)); // Copy record size
        std::memcpy(buffer.data() + sizeof(record_size), record.data(), record_size); // Copy record data

        // Count the number of pages in the file
        auto file_size = std::filesystem::file_size(tableFile);
        size_t num_pages = file_size / PAGE_SIZE;

        uint16_t page_num = 0;
        PageHeader header;

        while (true) {
            if (page_num >= num_pages) {
                // If we reach the end of the file, we need to create a new page
                header = { 0, (uint16_t)PAGE_SIZE };
                page.seekp(page_num * PAGE_SIZE);
                page.write(reinterpret_cast<const char*>(&header), sizeof(header));
                break;
            }

            // Read the page header
            page.seekg(page_num * PAGE_SIZE);
            page.read(reinterpret_cast<char*>(&header), sizeof(header));

            // Calculate free space in the page
            size_t used_space = header.slot_count * sizeof(uint16_t);
            size_t free_space = header.free_space_offset - used_space - sizeof(header); // Subtract header size and used space

            if (free_space >= record_size + sizeof(uint16_t)) {
                break;
            }

            page_num++;
        }

        // Write the record to the page
        uint16_t new_data = header.free_space_offset - buffer.size();
        page.seekp(page_num * PAGE_SIZE + new_data);
        page.write(reinterpret_cast<const char*>(buffer.data()), buffer.size());

        uint16_t slot = header.slot_count;
        page.seekp(page_num * PAGE_SIZE + sizeof(PageHeader) + slot * sizeof(uint16_t));
        page.write(reinterpret_cast<const char*>(&new_data), sizeof(new_data));

        // Update the page header
        header.slot_count++;
        header.free_space_offset = new_data;
        page.seekp(page_num * PAGE_SIZE);
        page.write(reinterpret_cast<const char*>(&header), sizeof(header));

		recordId = make_record_id(page_num, slot); // Create record ID
    }

    if (!is_vacuum) {
        vacuum(table);
    }

	return recordId; // Return the record ID
}

std::vector<uint8_t> FileStorageLayer::get(const std::string& table, int record_id) {
    if (!is_open) {
        std::cout << "Storage is not open. Cannot retrieve record." << std::endl;
        return std::vector<uint8_t>();
	}

    if (!is_table_exists(table)) {
        std::cout << "Table does not exist." << std::endl;
		return std::vector<uint8_t>();
	}

	auto tableFile = std::filesystem::path(storage_path) / (table + ".db");

	std::ifstream page(tableFile, std::ios::binary);

    if (!page.is_open()) {
        std::cout << "Failed to open table file." << std::endl;
		return std::vector<uint8_t>(); 
	}

	uint16_t page_num, slot_num;
	split_record_id(record_id, page_num, slot_num);

    if (page_num < 0 || slot_num < 0) {
        std::cout << "Invalid record ID." << std::endl;
		return std::vector<uint8_t>(); 
	}

	PageHeader header;
	page.seekg(page_num * PAGE_SIZE);
	page.read(reinterpret_cast<char*>(&header), sizeof(header));

    if (slot_num >= header.slot_count) {
		std::cout << "Slot number out of bounds." << std::endl;
		return std::vector<uint8_t>();
	}

	// Read the slot offset
	uint16_t slot_offset;
	page.seekg(page_num * PAGE_SIZE + sizeof(PageHeader) + slot_num * sizeof(uint16_t));
	page.read(reinterpret_cast<char*>(&slot_offset), sizeof(slot_offset));
	
    if (slot_offset == 0) {
		std::cout << "Slot is empty." << std::endl;
		return std::vector<uint8_t>();
	}

    if (slot_offset == DELETE_SLOT) {
        std::cout << "Slot is marked as deleted." << std::endl;
        return std::vector<uint8_t>();
	}

	// Read the record size
	uint32_t record_size;
	page.seekg(page_num * PAGE_SIZE + slot_offset);
	page.read(reinterpret_cast<char*>(&record_size), sizeof(record_size));

	// Read the record data
	std::vector<uint8_t> record_data(record_size);
	page.read(reinterpret_cast<char*>(record_data.data()), record_size);
    if (!page) {
        std::cout << "Failed to read record data." << std::endl;
        return std::vector<uint8_t>();
	}

    return record_data;
}

bool FileStorageLayer::update(const std::string& table, int record_id, const std::vector<uint8_t>& updated_record) {
    if (!is_open) {
        std::cout << "Storage is not open. Cannot update record." << std::endl;
		return false;
    }

    if (!is_table_exists(table)) {
        std::cout << "Table does not exist." << std::endl;
        return false; 
    }

	auto tableFile = std::filesystem::path(storage_path) / (table + ".db");

    {
        std::fstream page(tableFile, std::ios::binary | std::ios::in | std::ios::out);

        if (!page.is_open()) {
            std::cout << "Failed to open table file." << std::endl;
            return false;
        }

        uint16_t page_num, slot_num;
        split_record_id(record_id, page_num, slot_num);

        if (page_num < 0 || slot_num < 0) {
            std::cout << "Invalid record ID." << std::endl;
            return false;
        }

        PageHeader header;
        page.seekg(page_num * PAGE_SIZE);
        page.read(reinterpret_cast<char*>(&header), sizeof(header));

        if (slot_num >= header.slot_count) {
            std::cout << "Slot number out of bounds." << std::endl;
            return false;
        }

        // Read the slot offset
        uint16_t slot_offset;
        page.seekg(page_num * PAGE_SIZE + sizeof(PageHeader) + slot_num * sizeof(uint16_t));
        page.read(reinterpret_cast<char*>(&slot_offset), sizeof(slot_offset));

        if (slot_offset == 0 || slot_offset == DELETE_SLOT) {
            std::cout << "Slot is empty or marked as deleted." << std::endl;
            return false;
        }

        // Read the record size
        uint32_t record_size;
        page.seekg(page_num * PAGE_SIZE + slot_offset);
        page.read(reinterpret_cast<char*>(&record_size), sizeof(record_size));


        uint32_t updated_record_size = static_cast<uint32_t>(updated_record.size());

        std::vector<uint8_t> buffer(sizeof(updated_record_size) + updated_record_size);
        std::memcpy(buffer.data(), &updated_record_size, sizeof(updated_record_size)); // Copy updated record size
        std::memcpy(buffer.data() + sizeof(updated_record_size), updated_record.data(), updated_record_size); // Copy updated record data
        size_t new_size = buffer.size();

        size_t used_space = header.slot_count * sizeof(uint16_t);
        size_t free_space = header.free_space_offset - used_space - sizeof(header); // Subtract header size and used space

        if (updated_record_size <= record_size) {
            page.seekp(page_num * PAGE_SIZE + slot_offset);
            page.write(reinterpret_cast<const char*>(buffer.data()), new_size);
            page.flush();
        }
        else if (free_space >= new_size) {
            // If the updated record is larger, we need to find a new slot
            uint16_t new_slot_offset = header.free_space_offset - new_size;
            page.seekp(page_num * PAGE_SIZE + new_slot_offset);
            page.write(reinterpret_cast<const char*>(buffer.data()), new_size);

            // Update slot pointer
            page.seekp(page_num * PAGE_SIZE + sizeof(PageHeader) + slot_num * sizeof(uint16_t));
            page.write(reinterpret_cast<const char*>(&new_slot_offset), sizeof(new_slot_offset));

            // Update the page header
            page.seekp(page_num * PAGE_SIZE);
            page.write(reinterpret_cast<const char*>(&header), sizeof(header));
        }
        else {
            std::cout << "Not enough space to update record." << std::endl;
            return false;
        }
    }

	if (!is_vacuum) {
        vacuum(table);
	}

    return true;
}

bool FileStorageLayer::delete_record(const std::string& table, int record_id) {
    if (!is_open) {
        std::cout << "Storage is not open. Cannot delete record." << std::endl;
        return false;
	}

    if (!is_table_exists(table)) {
        std::cout << "Table does not exist." << std::endl;
        return false; 
	}

	auto tableFile = std::filesystem::path(storage_path) / (table + ".db");

    {
        std::fstream page(tableFile, std::ios::binary | std::ios::in | std::ios::out);

        if (!page.is_open()) {
            std::cout << "Failed to open table file." << std::endl;
            return false;
        }

        uint16_t page_num, slot_num;
        split_record_id(record_id, page_num, slot_num);

        if (page_num < 0 || slot_num < 0) {
            std::cout << "Invalid record ID." << std::endl;
            return false;
        }

        PageHeader header;
        page.seekg(page_num * PAGE_SIZE);
        page.read(reinterpret_cast<char*>(&header), sizeof(header));

        if (slot_num >= header.slot_count) {
            std::cout << "Slot number out of bounds." << std::endl;
            return false;
        }

        uint16_t slot_position = page_num * PAGE_SIZE + sizeof(PageHeader) + slot_num * sizeof(uint16_t);
        page.seekg(slot_position);
        page.write(reinterpret_cast<const char*>(&DELETE_SLOT), sizeof(DELETE_SLOT)); // Mark slot as deleted
        page.flush();
    }

    if (!is_vacuum) {
        vacuum(table);
    }

    return true;
}

std::vector<std::vector<uint8_t>> FileStorageLayer::scan(
    const std::string& table,
    const std::optional<std::function<bool(int, const std::vector<uint8_t>&)>>& callback,
    const std::optional<std::vector<int>>& projection,
    const std::optional<std::function<bool(const std::vector<uint8_t>&)>>& filter_func) {

	std::vector<std::vector<uint8_t>> results;

    if (!is_open) {
        std::cout << "Storage is not open. Cannot scan table." << std::endl;
        return results;
	}

    if (!is_table_exists(table)) {
        std::cout << "Table does not exist." << std::endl;
		return results;
	}

	auto tableFile = std::filesystem::path(storage_path) / (table + ".db");
	std::ifstream page(tableFile, std::ios::binary);

    if (!page.is_open()) {
        std::cout << "Failed to open table file." << std::endl;
		return results;
	}

	size_t file_size = std::filesystem::file_size(tableFile);
	size_t num_pages = file_size / PAGE_SIZE;

    for (size_t page_num = 0; page_num < num_pages; ++page_num) {
        PageHeader header;
        page.seekg(page_num * PAGE_SIZE);
        page.read(reinterpret_cast<char*>(&header), sizeof(header));

        for (uint16_t slot_num = 0; slot_num < header.slot_count; slot_num++) {
			uint16_t slot_offset;
            page.seekg(page_num * PAGE_SIZE + sizeof(PageHeader) + slot_num * sizeof(uint16_t));
            page.read(reinterpret_cast<char*>(&slot_offset), sizeof(slot_offset));
            
            if (slot_offset == 0 || slot_offset == DELETE_SLOT) {
                continue; // Skip empty or deleted slots
            }
            
            // Read the record size
            uint32_t record_size;
            page.seekg(page_num * PAGE_SIZE + slot_offset);
            page.read(reinterpret_cast<char*>(&record_size), sizeof(record_size));
            
            // Read the record data
            std::vector<uint8_t> record_data(record_size);
			page.read(reinterpret_cast<char*>(record_data.data()), record_size);

            if (!page) {
                continue;
            }

            if (filter_func && !filter_func.value()(record_data)) {
                continue;
			}

			int record_id = make_record_id(page_num, slot_num);

            if (callback && !callback.value()(record_id, record_data)) {
                continue; // If callback returns false, skip this record
            }

            // If projection is specified, filter the record data
            if (projection) {
                std::vector<uint8_t> projected_record;
                for (int index : projection.value()) {
                    if (index < 0 || index >= static_cast<int>(record_data.size())) {
                        std::cout << "Projection index out of bounds." << std::endl;
                        continue;
                    }
					projected_record.push_back(record_data[index]);
                }
				results.push_back(projected_record);
            }
            else {
                results.push_back(record_data); // Add the full record data
            }
        }
    }

    return results;
}

bool FileStorageLayer::create_table(const std::string& table_name, const TableSchema& schema) {
    if (!is_open) {
        std::cout << "Storage is not open. Cannot create table." << std::endl;
        return false;
    }

    auto tableFile = std::filesystem::path(storage_path) / (table_name + ".db");
    auto schemaFile = std::filesystem::path(storage_path) / (table_name + ".schema");

    if (std::filesystem::exists(tableFile) || std::filesystem::exists(schemaFile)) {
        std::cout << "Table with such name is already exists!" << std::endl;
        return false;
    }

    std::ofstream page(tableFile, std::ios::binary);

    PageHeader header{ 0, (uint16_t)PAGE_SIZE };
	page.write(reinterpret_cast<const char*>(&header), sizeof(header));

    if (!page) {
        std::cout << "Failed to create table file." << std::endl;
        return false;
    }

	std::ofstream schema_page(schemaFile);

    if (!schema_page.is_open()) {
        std::cout << "Failed to create schema file." << std::endl;
        return false;
    }

    schema_page << schema.columns.size() << std::endl; // Write the number of columns

    for (const auto& column : schema.columns) {
        schema_page << column.name << " " 
                    << static_cast<int>(column.type) << " " 
                    << column.length << std::endl; // Write column name, type, and length
    }

	table_schemas[table_name] = schema; // Store the schema for the table
	return true;
}

bool FileStorageLayer::drop_table(const std::string& table_name) {
    if (!is_open) {
        std::cout << "Storage is not open. Cannot drop table." << std::endl;
        return false;
    }
    auto tableFile = std::filesystem::path(storage_path) / (table_name + ".db");
	auto schemaFile = std::filesystem::path(storage_path) / (table_name + ".schema");
    
    if (!std::filesystem::exists(tableFile)) {
        std::cout << "Table with such name does not exist!" << std::endl;
        return false;
    }

    std::filesystem::remove(tableFile);
	std::filesystem::remove(schemaFile); // Remove the schema file as well
	table_schemas.erase(table_name); // Remove the schema from the in-memory map
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

bool FileStorageLayer::vacuum(const std::string& table_name) {

    if (is_vacuum) {
        return true;
	}
	is_vacuum = true;

    if (!is_open) {
        std::cout << "Storage is not open. Cannot vacuum table." << std::endl;
        return false;
    }

    if (!is_table_exists(table_name)) {
        std::cout << "Table does not exist." << std::endl;
        return false;
    }

	// Read all records from the table
	auto records = scan(table_name);
	auto tableFile = std::filesystem::path(storage_path) / (table_name + ".db");

	std::filesystem::remove(tableFile); // Remove the old table file

    std::ofstream new_page(tableFile, std::ios::binary);
    if (!new_page.is_open()) {
        std::cout << "Failed to create new table file." << std::endl;
        return false;
    }

	PageHeader header{ 0, (uint16_t)PAGE_SIZE };
	new_page.write(reinterpret_cast<const char*>(&header), sizeof(header));
	new_page.close();

    for (const auto& record : records) {
        if (insert(table_name, record) == -1) {
            std::cout << "Failed to insert record during vacuum." << std::endl;
            return false;
        }
    }

	is_vacuum = false;
	return true;
}

// PRIVATE METHODS

void FileStorageLayer::ensure_directory_exists(const std::string& path) {
	std::filesystem::create_directory(path);
}

bool FileStorageLayer::is_table_exists(const std::string& table_name) const {
    auto tableFile = std::filesystem::path(storage_path) / (table_name + ".db");
    return std::filesystem::exists(tableFile);
}

int FileStorageLayer::make_record_id(uint16_t page, uint16_t slot) const {
    // Combine page and slot into a single record ID
    // Assuming page and slot are both 16-bit integers
	return (page << 16) | slot;
}

void FileStorageLayer::split_record_id(int record_id, uint16_t& page, uint16_t& slot) {
    page = (record_id >> 16) & 0xFFFF;
    slot = record_id & 0xFFFF;
}

void FileStorageLayer::load_table_schemas() {
    for (auto& entry : std::filesystem::directory_iterator(storage_path)) {
        if (entry.path().extension() == ".schema") {
			std::ifstream schema_file(entry.path());
			std::string table_name = entry.path().stem().string();

            size_t cnt;
			schema_file >> cnt; // Read the number of columns

			TableSchema schema;
            
            for (size_t i = 0; i < cnt; i++) {
				Column column;

                int type;
                schema_file >> column.name;
				schema_file >> type; // Read column type
				schema_file >> column.length; // Read column length

                column.type = static_cast<DataType>(type);
				schema.columns.push_back(column);
            }

			table_schemas[table_name] = schema; // Store the schema for the table
        }
	}
}