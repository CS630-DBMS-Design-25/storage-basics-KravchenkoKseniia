#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include "file_storage_layer.h"
#include "table_schema.h"

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
        << "  scan <table name> [--projection <field1> <field2> ...] - Scan records in a table (not implemented yet)\n"
        << "  help                                     - Display this help message\n"
        << "  exit/quit                                - Exit the program\n";
}

// Convert a string to a vector of bytes
std::vector<uint8_t> string_to_bytes(const std::string& str) {
    return std::vector<uint8_t>(str.begin(), str.end());
}

// Convert a vector of bytes to a string
std::string bytes_to_string(const std::vector<uint8_t>& bytes) {
    return std::string(bytes.begin(), bytes.end());
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
                int record_id = storage.insert(args[1], string_to_bytes(args[2]));

                if (record_id < 0) {
                    std::cout << "Error: Failed to insert record. Table may not exist.\n";
                    continue;
				}

                std::cout << "Record inserted with ID " << record_id << std::endl;
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
                int record_id = std::stoi(args[2]);
                auto record = storage.get(args[1], record_id);

                if (record.empty()) {
                    std::cout << "Error: Record not found\n";
                    continue;
				}

                std::cout << "Retrieved record: " << bytes_to_string(record) << std::endl;
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
                int record_id = std::stoi(args[2]);
                bool isSuccess = storage.update(args[1], record_id, string_to_bytes(args[3]));

                if (!isSuccess) {
                    std::cout << "Error: Failed to update record. Record may not exist.\n";
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
                int record_id = std::stoi(args[2]);
                bool isSucces = storage.delete_record(args[1], record_id);

                if (!isSucces) {
					std::cout << "Error: Failed to delete record. Record may not exist.\n";
					continue;
				}

                std::cout << "Record deleted\n";
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
                
                for (size_t i = 3; i < args.size(); ++i) {
                    try {
                        projection.push_back(std::stoi(args[i]));
                    }
                    catch (const std::exception& e) {
                        std::cout << "Error: Invalid projection index '" << args[i] << "'. Must be an integer.\n";
                        continue;
                    }
				}
			}

            auto callback = [&](int rid, const std::vector<uint8_t>& record) {
				std::vector<uint8_t> output_record;
                if (is_projection_provided) {
                    for (int index : projection) {
                        if (index < 0 || index >= record.size()) {
                            std::cout << "Error: Projection index " << index << " out of bounds for record size " << record.size() << std::endl;
                        }
                        output_record.push_back(record[index]);
                    }
                } else {
                    output_record = record; // No projection
                }
				std::cout << "Record[" << rid << "]: " << bytes_to_string(output_record) << std::endl;
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
        else {
            std::cout << "Unknown command: " << command << "\nType 'help' for available commands\n";
        }
    }

    return 0;
}