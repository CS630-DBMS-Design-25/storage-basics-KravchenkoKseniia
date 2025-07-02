#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include "file_storage_layer.h"
#include "table_schema.h"
#include "parser.h"
#include "ast.h"

void print_help() {
    std::cout << "Storage Layer CLI - Available commands:\n"
        << "  open <path>                              - Open storage at specified path\n"
        << "  close                                    - Close the storage\n"
		<< "  create <table name> <schema>             - Create a new table\n"
		<< "  drop <table name>                        - Drop an existing table\n"
		<< "  list                                     - List all tables\n"
        << "  insert <table name> <record>             - Insert a record\n"
        << "  get <table name> <record_id>             - Get a record by ID\n"
        << "  update <table name> <record_id> <record> - Update a record\n"
        << "  delete <table name> <record_id>          - Delete a record\n"
        << "  scan <table name> [--projection <field1> <field2> ...] - Scan records in a table\n"
        << "  find <table name> <key>                  - find records by index\n"
        << "  help                                     - Display this help message\n"
        << "  --query <SQL query>                      - Execute SQL using parser\n"
        << "  exit/quit                                - Exit the program\n";
}
// Parse command line arguments
std::vector<std::string> parse_args(const std::string& input) {
    std::vector<std::string> args;
    std::istringstream iss(input);
    std::string arg;

    while (iss >> arg) {
        args.push_back(arg);
    }

    return args;
}

// Split schema string into a vector of field names
std::vector<std::string> split_vector_by_delimeter(const std::string& schema, char delimeter = ',') {
    std::vector<std::string> fields;
    std::stringstream ss(schema);
    std::string field;
    
    while (std::getline(ss, field, delimeter)) {
		fields.push_back(field);
    }

	return fields;
}

// schema fields into bytes
static std::vector<uint8_t> schema_to_bytes(const TableSchema& schema, const std::vector<std::string>& fields) {
	std::vector<uint8_t> bytes;

    for (size_t i = 0; i < schema.columns.size(); i++) {
        const Column& column = schema.columns[i];
        const std::string& value = i < fields.size() ? fields[i] : "";

        if (column.type == DataType::INT) {
            int int_value = 0;
            
            if (!value.empty()) {
                int_value = std::stoi(value);
            }

			uint8_t buffer[sizeof(int)];
			std::memcpy(buffer, &int_value, sizeof(int));
            bytes.insert(bytes.end(), buffer, buffer + sizeof(int));
        }
        else if (column.type == DataType::VARCHAR) {
			uint16_t length = value.size() < column.length ? value.size() : column.length;
			uint8_t length_buffer[sizeof(uint16_t)];
			std::memcpy(length_buffer, &length, sizeof(uint16_t));
			bytes.insert(bytes.end(), length_buffer, length_buffer + sizeof(uint16_t));
            bytes.insert(bytes.end(), value.begin(), value.begin() + length); // Insert only up to the defined length
        }  
        else {
			std::cout << "Unsupported data type in schema: " << column.name << std::endl;
			return {};
        } 
    }

    return bytes;
}

// Convert a vector of bytes to a vector of strings
static std::vector<std::string> bytes_to_fields(const TableSchema& schema, const std::vector<uint8_t>& bytes) {
    std::vector<std::string> fields;
    size_t offset = 0;

    for (const auto& column : schema.columns) {
        if (column.type == DataType::INT) {
            if (offset + sizeof(int) > bytes.size()) {
                std::cout << "Error: Not enough bytes for INT field\n";
                return {};  
            }
            int value;
            std::memcpy(&value, &bytes[offset], sizeof(int));
            fields.push_back(std::to_string(value));
            offset += sizeof(int);
        }
        else if (column.type == DataType::VARCHAR) {
            if (offset + sizeof(uint16_t) > bytes.size()) {
                std::cout << "Error: Not enough bytes for VARCHAR length\n";
                return {};
            }
            uint16_t length;
            std::memcpy(&length, &bytes[offset], sizeof(uint16_t));
            offset += sizeof(uint16_t);
            if (offset + length > bytes.size()) {
                std::cout << "Error: Not enough bytes for VARCHAR field\n";
                return {};
            }
            fields.push_back(std::string(bytes.begin() + offset, bytes.begin() + offset + length));
            offset += length;
        }
        else {
            std::cout << "Unsupported data type in schema: " << column.name << std::endl;
            return {};
        }
    }
    return fields;
}

template<class... Ts> struct overloaded : Ts... {using Ts::operator()...; };
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

int main() {
    FileStorageLayer storage;

    std::cout << "Storage Layer CLI - Type 'help' for available commands or 'exit' to quit\n";

    while (true) {
        std::string input;
        std::cout << "storage-cli> ";
        std::getline(std::cin, input);

        if (input.empty()) {
            continue;
        }

        std::vector<std::string> args = parse_args(input);
        std::string command = args[0];

        if (command == "exit" || command == "quit") {
            break;
        }
        else if (command == "help") {
            print_help();
        }
        else if (command == "open") {
            if (args.size() < 2) {
                std::cout << "Error: Missing path argument\n";
                continue;
            }
            try {
                storage.open(args[1]);
                std::cout << "Storage opened at " << args[1] << std::endl;
            }
            catch (const std::exception& e) {
                std::cout << "Error: " << e.what() << std::endl;
            }
        }
        else if (command == "close") {
            try {
                storage.close();
                std::cout << "Storage closed\n";
            }
            catch (const std::exception& e) {
                std::cout << "Error: " << e.what() << std::endl;
            }
        }
        else if (command == "insert") {
            if (args.size() < 3) {
                std::cout << "Error: Missing arguments. Usage: insert <table> <record>\n";
                continue;
            }
            try {
				std::string table_name = args[1];
				std::vector<std::string> fields = split_vector_by_delimeter(args[2], ','); // Split by ',' to get individual fields

				TableSchema schema = storage.get_table_schema(table_name);

                if (schema.columns.empty()) {
                    std::cout << "Error: Table '" << table_name << "' does not exist or has no schema defined" << std::endl;
                    continue;
                }
				std::vector<uint8_t> record_bytes = schema_to_bytes(schema, fields);

                if (record_bytes.empty()) {
                    std::cout << "Error: Failed to convert record to bytes. Check schema and field types." << std::endl;
                    continue;
                }

                int record_id = storage.insert(table_name, record_bytes);

                if (record_id < 0) {
					std::cout << "Error: Failed to insert record. Table may not exist or schema mismatch." << std::endl;
					continue;
                }
				std::cout << "Record inserted with ID: " << record_id << std::endl;
            }
            catch (const std::exception& e) {
                std::cout << "Error: " << e.what() << std::endl;
            }
        }
        else if (command == "get") {
            if (args.size() < 3) {
                std::cout << "Error: Missing arguments. Usage: get <table> <record_id>\n";
                continue;
            }
            try {
				std::string table_name = args[1];
                int record_id = std::stoi(args[2]);
                auto record = storage.get(table_name, record_id);
                if (record.empty()) {
                    std::cout << "Error: Record with ID " << record_id << " not found in table '" << table_name << "'\n";
                    continue;
                }

				TableSchema schema = storage.get_table_schema(table_name);
                if (schema.columns.empty()) {
                    std::cout << "Error: Table '" << table_name << "' does not exist or has no schema defined" << std::endl;
                    continue;
				}

                std::vector<std::string> fields = bytes_to_fields(schema, record);
                if (fields.empty()) {
                    std::cout << "Error: Failed to convert record bytes to fields. Check schema and field types." << std::endl;
                    continue;
                }
                std::cout << "Record[" << record_id << "]: ";
                for (const auto& field : fields) {
                    std::cout << field << " ";
                }
				std::cout << std::endl;
            }
            catch (const std::exception& e) {
                std::cout << "Error: " << e.what() << std::endl;
            }
        }
        else if (command == "update") {
            if (args.size() < 4) {
                std::cout << "Error: Missing arguments. Usage: update <table> <record_id> <record>\n";
                continue;
            }
            try {
				std::string table_name = args[1];
				int record_id = std::stoi(args[2]);

				std::vector<std::string> fields = split_vector_by_delimeter(args[3], ',');

                TableSchema schema = storage.get_table_schema(table_name);
                if (schema.columns.empty()) {
                    std::cout << "Error: Table '" << table_name << "' does not exist or has no schema defined" << std::endl;
                    continue;
				}

                std::vector<uint8_t> record_bytes = schema_to_bytes(schema, fields);
                if (record_bytes.empty()) {
                    std::cout << "Error: Failed to convert record to bytes. Check schema and field types." << std::endl;
                    continue;
                }
                bool isUpdated = storage.update(table_name, record_id, record_bytes);
				if (!isUpdated) {
					std::cout << "Error: Failed to update record. Record may not exist or schema mismatch." << std::endl;
					continue;
                }
				std::cout << "Record updated\n";
            }
            catch (const std::exception& e) {
                std::cout << "Error: " << e.what() << std::endl;
            }
        }
        else if (command == "delete") {
            if (args.size() < 3) {
                std::cout << "Error: Missing arguments. Usage: delete <table> <record_id>\n";
                continue;
            }
            try {
				std::string table_name = args[1];
                int record_id = std::stoi(args[2]);
                bool isDeleted = storage.delete_record(table_name, record_id);
                if (!isDeleted) {
					std::cout << "Error: Record with ID " << record_id << " not found in table '" << table_name << "'\n";
					continue;
                }
				std::cout << "Record with ID " << record_id << " deleted from table '" << table_name << "'\n";
            }
            catch (const std::exception& e) {
                std::cout << "Error: " << e.what() << std::endl;
            }
        }
        else if (command == "scan") {
            if (args.size() < 2) {
                std::cout << "Error: Missing table argument. Usage: scan <table> [--projection <field1> <field2> ...]\n";
                continue;
            }

			std::string table = args[1];

            std::vector<int> projection;
            bool is_projection_provided = false;
            
            if (args.size() > 2 && args[2] == "--projection") {
                is_projection_provided = true;
                
				std::vector<std::string> projection_fields = split_vector_by_delimeter(args[3], ','); // Split by ',' to get individual fields
                
                for (const auto& field : projection_fields) {
                    try {
                        int index = std::stoi(field);
                        projection.push_back(index);
                    } catch (const std::exception& e) {
                        std::cout << "Error: Invalid projection index '" << field << "'. Must be an integer.\n";
                        is_projection_provided = false;
                        break;
                    }
				}
			}

            auto callback = [&](int rid, const std::vector<uint8_t>& record) {
				TableSchema schema = storage.get_table_schema(table);
                
                if (schema.columns.empty()) {
                    std::cout << "Error: Table '" << table << "' does not exist or has no schema defined" << std::endl;
                    return false;
				}

				std::vector<std::string> fields = bytes_to_fields(schema, record);

                if (is_projection_provided) {
                    for (int index : projection) {
                        if (index < 0 || index >= fields.size()) {
                            std::cout << "Error: Projection index " << index << " out of bounds for record size " << record.size() << std::endl;
                        }

						std::cout << "Field[" << index << "]: " << fields[index] << " ";
                    }
                } else {
                    for (size_t i = 0; i < fields.size(); i++) {
                        std::cout << "Field[" << i << "]: " << fields[i] << " ";
					}
                }
				std::cout << "\n";
				return true;
			};

            try {
				storage.scan(table, callback, projection, std::nullopt);
            }
            catch (const std::exception& e) {
                std::cout << "Error: " << e.what() << std::endl;
            }
        }
        else if (command == "create") {
            if (args.size() < 3) {
                std::cout << "Error: Missing table name argument. Usage: create <table name>\n";
                continue;
            }
            try {
				std::string table_name = args[1];
				std::string schema = args[2];

				TableSchema table_schema;

				std::vector<std::string> fields = split_vector_by_delimeter(schema);
				bool isSuccess = true;

                for (const auto& field : fields) {
					std::vector<std::string> field_parts = split_vector_by_delimeter(field, ':'); // Split by ':' to get field name and type

                    if (field_parts.size() != 2) {
                        std::cout << "Error: Invalid field definition '" << field << "'. Expected format: <field_name>:<field_type>\n";
                        isSuccess = false;
                        break;
                    }

					Column column;
					column.name = field_parts[0];
					std::string field_type = field_parts[1];

                    if (field_type == "INT") {
						column.type = DataType::INT;
						column.length = sizeof(int);
                    }
                    else if (field_type.rfind("VARCHAR(", 0) == 0) {
						column.type = DataType::VARCHAR;
						std::string length_str = field_type.substr(8, field_type.size() - 9); // Extract length from VARCHAR(length)
						try {
							column.length = std::stoi(length_str);
						}
						catch (const std::exception& e) {
							std::cout << "Error: Invalid VARCHAR length '" << length_str << "'. Must be an integer.\n";
							isSuccess = false;
							break;
                        }
                    }
                    else {
                        std::cout << "Error: Unsupported field type '" << field_type << "'. Supported types are INT and VARCHAR(n).\n";
                        isSuccess = false;
                        break;
					}

					table_schema.columns.push_back(column);
                }

                if (!isSuccess) {
                    std::cout << "Error: Failed to create table due to invalid schema" << std::endl;
                    continue;
                }
                bool isCreated = storage.create_table(table_name, table_schema);

                if (!isCreated) {
                    std::cout << "Error: Table '" << table_name << "' already exists" << std::endl;
                }
                else {
                    std::cout << "Table '" << table_name << "' created with schema: " << schema << std::endl;
                }
				continue;
            }
            catch (const std::exception& e) {
                std::cout << "Error: " << e.what() << std::endl;
            }
        }
        else if (command == "drop") {
            if (args.size() < 2) {
                std::cout << "Error: Missing table name argument. Usage: drop <table name>\n";
                continue;
            }
            try {
                bool isSuccess = storage.drop_table(args[1]);

                if (!isSuccess) {
                    std::cout << "Error: Table '" << args[1] << "' does not exist\n";
					continue;
				}

                std::cout << "Table '" << args[1] << "' dropped\n";
            }
            catch (const std::exception& e) {
                std::cout << "Error: " << e.what() << std::endl;
            }
		}
        else if (command == "list") {
            try {
                auto tables = storage.list_tables();

                if (tables.empty()) {
                    std::cout << "No tables found.\n";
					continue;
				}

                std::cout << "Tables:\n";
                for (const auto& table : tables) {
                    std::cout << "  " << table << "    < ";

                    try {
                        auto schema = storage.get_table_schema(table);
                        for (const auto& column : schema.columns) {
                            std::cout << column.name << ":";
                            if (column.type == DataType::INT) {
                                std::cout << "INT ";
                            } else if (column.type == DataType::VARCHAR) {
                                std::cout << "VARCHAR(" << column.length << ") ";
                            }
                        }
                        std::cout << ">\n";
                    }
                    catch (const std::exception& e) {
                        std::cout << "Error retrieving schema for table '" << table << "': " << e.what() << std::endl;
                    }
                }
            }
            catch (const std::exception& e) {
                std::cout << "Error: " << e.what() << std::endl;
            }
		}
        else if (command == "find") {

            if (args.size() < 3) {
                std::cout << "Error: Missing arguments. Usage: find <table name> <key>\n";
                continue;
			}

            std::string table = args[1];
            std::string key = args[2];
            auto ids = storage.find(table, key);

            for (int id : ids) {
                std::cout << "Found ID = " << id << std::endl;
            }
        }
        else if (command == "--query") {
            if (args.size() < 2) {
                std::cout << "Error: missing SQL query" << std::endl;
                continue;
            }

            size_t query_pos = input.find("--query");

            if (query_pos == std::string::npos) {
                std::cout << "Error: missing <--query>" << std::endl;
                continue;
            }

            size_t pos = query_pos + std::strlen("--query");
            pos = input.find_first_not_of(" ", pos);

            if (pos == std::string::npos) {
                std::cout << "Error: missing sql query" << std::endl;
                continue;
            }

            bool quoted = input[pos] == '"';
            
            if (quoted) {
                pos++;
            }

            size_t end = input.size();

            while (end > pos && std::isspace(input[end - 1])) {
                end--;
            }

            if (quoted && end > pos && input[end - 1] == '"') {
                end--;
            }

            std::string sql = input.substr(pos, end - pos);

            if (sql.empty()) {
                std::cout << "Error: SQL query is empty" << std::endl;
                continue;  
            }

            try {
                AST stmt = parse_sql_to_ast(sql);

                std::visit(overloaded{
                    [&](const CreateTableStatement& create) {
                        TableSchema schema;

                        for (auto& [name, type] : create.columns) {
                            Column column;
                            column.name = name;
                            if (type == "INT") {
                                column.type = DataType::INT;
                                column.length = sizeof(int);
                            }
                            else if (type.rfind("VARCHAR(", 0) == 0) {
                                std::string length = type.substr(8, type.size() - 9);
                                column.type = DataType::VARCHAR;
                                column.length = std::stoi(length);
                            }
                            else {
                                std::cout << "Unsupported column type" << std::endl;
                                return;
                            }

                            schema.columns.push_back(column);
                        }

                        if (storage.create_table(create.table_name, schema)) {
                            std::cout << "Table " << create.table_name << " created" << std::endl;
                        }
                        else {
                            std::cout << "Could not create the table " << create.table_name << std::endl;
                        }
                    },
                    [&](const InsertStatement& insert) {
                        TableSchema schema = storage.get_table_schema(insert.table_name);
                        if (schema.columns.empty()) {
                            std::cout << "Error: table " << insert.table_name << " not found" << std::endl;
                            return;
                        }

                        std::vector<uint8_t> record_bytes = schema_to_bytes(schema, insert.values);

                        if (record_bytes.empty()) {
                            std::cout << "Error: failed to serialize the record" << std::endl;
                            return;
                        }

                        int new_record_id = storage.insert(insert.table_name, record_bytes);

                        if (new_record_id >= 0) {
                            std::cout << "Inserted record into " << insert.table_name << " with ID " << new_record_id << std::endl;
                        }
                        else {
                            std::cout << "Error while inserting value into " << insert.table_name << std::endl;
                        }

                    },
                    [&](const SelectStatement& select) {
                        TableSchema schema = storage.get_table_schema(select.table_name);
                        if (schema.columns.empty()) {
                            std::cout << "Error: table " << select.table_name << " not found" << std::endl;
                            return;
                        }

                        std::vector<int> projection_indexes;

                        if (!select.columns.empty()) {
                            for (auto& column : select.columns) {
                                auto it = std::find_if(schema.columns.begin(), schema.columns.end(),
                                    [&](auto& c) {return c.name == column; });

                                if (it == schema.columns.end()) {
                                    std::cout << "Error: no such columnin the table: " << column << std::endl;
                                    return;
                                }

                                projection_indexes.push_back(std::distance(schema.columns.begin(), it));
                            }
                        }

                        auto callback = [&](int rid, const std::vector<uint8_t>& raw) {
                            auto fields = bytes_to_fields(schema, raw);
                            std::cout << "Record[" << rid << "]: ";

                            if (projection_indexes.empty()) {
                                for (auto& field : fields) {
                                    std::cout << field << " ";
                                }
                            }
                            else {
                                for (int index : projection_indexes) {
                                    std::cout << fields[index] << " ";
                                }
                            }

                            std::cout << '\n';
                            return true;
                        };

                        storage.scan(select.table_name, callback, std::nullopt, std::nullopt);
                    }
                }, stmt);
            }
            catch (const std::exception& e) {
                std::cout << "SQL parse error: " << e.what() << std::endl;
            }
        }
        else {
            std::cout << "Unknown command: " << command << "\nType 'help' for available commands\n";
        }
    }

    return 0;
}