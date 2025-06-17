#include "file_storage_layer.h"

// HELPER FUNCTIONS

//static std::string data_type_to_string(DataType type) {
//	return type == DataType::INT ? "INT" : "VARCHAR";
//}
//
//static DataType string_to_data_type(const std::string& type_str) {
//    if (type_str == "INT") {
//        return DataType::INT;
//    } else if (type_str == "VARCHAR") {
//        return DataType::VARCHAR;
//    }
//
//    return DataType::UNKNOWN;
//}

// MAIN CLASS IMPLEMENTATION

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

 //   for (auto &s : std::filesystem::directory_iterator(storage_path)) {
 //       if (s.path().extension() == ".schema") {
	//		auto schema_file = s.path().stem().string();
	//		bool isSuccess = load_table_schema(schema_file);

 //           if (!isSuccess) {
 //               std::cout << "Failed to load schema for table: " << schema_file << std::endl;
 //               return;
 //           }

 //       }
	//}
    is_open = true;
}

void FileStorageLayer::close() {
    // TODO: Implement this method to close the storage safely
    is_open = false;
    // Implement closing logic
}

int FileStorageLayer::insert(const std::string& table, const std::vector<uint8_t>& record) {
    // TODO: Implement this method to insert a record and return its ID
    // Implement insert logic

    if (!is_open) {
        std::cout << "Storage is not open. Cannot insert record." << std::endl;
        return -1;
	}

    if (!is_table_exists(table)) {
        std::cout << "Table does not exist." << std::endl;
        return -1;
	}

	auto tableFile = std::filesystem::path(storage_path) / (table + ".db");

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
            header = { 0, (uint16_t)PAGE_SIZE, 0 };
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

	return make_record_id(page_num, slot); // Return the record ID
}

std::vector<uint8_t> FileStorageLayer::get(const std::string& table, int record_id) {
    // TODO: Implement this method to retrieve a record by ID
    // Implement retrieval logic

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

    return record_data; // Replace with actual implementation
}

bool FileStorageLayer::update(const std::string& table, int record_id, const std::vector<uint8_t>& updated_record) {
    // TODO: Implement this method to update a record
    // Implement update logic

    if (!is_open) {
        std::cout << "Storage is not open. Cannot update record." << std::endl;
		return false;
    }

    if (!is_table_exists(table)) {
        std::cout << "Table does not exist." << std::endl;
        return false; 
    }

	auto old_record = get(table, record_id);

    if (old_record.empty()) {
        std::cout << "Record not found." << std::endl;
        return false; 
    }

    if (updated_record.size() <= old_record.size()) {
        // If the updated record is smaller or equal in size, we can overwrite it
        auto tableFile = std::filesystem::path(storage_path) / (table + ".db");
        std::fstream page(tableFile, std::ios::binary | std::ios::in | std::ios::out);

        if (!page.is_open()) {
            std::cout << "Failed to open table file." << std::endl;
            return false;
        }

        uint16_t page_num, slot_num;
        split_record_id(record_id, page_num, slot_num);
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
        
        if (slot_offset == 0) {
            std::cout << "Slot is empty." << std::endl;
            return false;
        }
        
        // Write the updated record
        page.seekp(page_num * PAGE_SIZE + slot_offset);
        uint32_t new_record_size = updated_record.size();
        page.write(reinterpret_cast<const char*>(&new_record_size), sizeof(new_record_size));
        page.write(reinterpret_cast<const char*>(updated_record.data()), new_record_size);
        
        return true; // Update successful
    }
    else {
		delete_record(table, record_id);
		return insert(table, updated_record) != -1; // Reinsert the updated record
    }
}

bool FileStorageLayer::delete_record(const std::string& table, int record_id) {
    // TODO: Implement this method to delete a record
    // Implement delete logic

    if (!is_open) {
        std::cout << "Storage is not open. Cannot delete record." << std::endl;
        return false;
	}

    if (!is_table_exists(table)) {
        std::cout << "Table does not exist." << std::endl;
        return false; 
	}

	auto tableFile = std::filesystem::path(storage_path) / (table + ".db");

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

    return true;
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

bool FileStorageLayer::create_table(const std::string& table_name/*, const std::vector<Column>& schema*/) {
    if (!is_open) {
		std::cout << "Storage is not open. Cannot create table." << std::endl;
		return false;
    }

    auto tableFile = std::filesystem::path(storage_path) / (table_name + ".db");
	//auto schemaFile = std::filesystem::path(storage_path) / (table_name + ".schema");
    
    if (std::filesystem::exists(tableFile) /*|| std::filesystem::exists(schemaFile)*/) {
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
    
 //   table_schemas[table_name] = TableSchema{ table_name, schema };
	//bool isSuccess = save_table_schema(table_name);

 //   if (!isSuccess) {
 //       std::cout << "Failed to save table schema." << std::endl;
 //       return false;
 //   }

	return true;
}

bool FileStorageLayer::drop_table(const std::string& table_name) {
    if (!is_open) {
        std::cout << "Storage is not open. Cannot drop table." << std::endl;
        return false;
    }
    auto tableFile = std::filesystem::path(storage_path) / (table_name + ".db");
	//auto schemaFile = std::filesystem::path(storage_path) / (table_name + ".schema");
    
    if (!std::filesystem::exists(tableFile) /*|| !std::filesystem::exists(schemaFile)*/) {
        std::cout << "Table with such name does not exist!" << std::endl;
        return false;
    }
    std::filesystem::remove(tableFile);
	//std::filesystem::remove(schemaFile);

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