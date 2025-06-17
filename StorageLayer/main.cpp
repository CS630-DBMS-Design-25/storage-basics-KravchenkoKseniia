#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include "file_storage_layer.h"

void print_help() {
    std::cout << "Storage Layer CLI - Available commands:\n"
        << "  open <path>                              - Open storage at specified path\n"
        << "  close                                    - Close the storage\n"
		<< "  create <table name>                      - Create a new table\n"
		<< "  drop <table name>                        - Drop an existing table\n"
		<< "  list                                     - List all tables\n"
        << "  insert <table name> <record>             - Insert a record (not implemented yet)\n"
        << "  get <table name> <record_id>             - Get a record by ID (not implemented yet)\n"
        << "  update <table name> <record_id> <record> - Update a record (not implemented yet)\n"
        << "  delete <table name> <record_id>          - Delete a record (not implemented yet)\n"
        << "  scan <table name> [--projection <field1> <field2> ...] - Scan records in a table (not implemented yet)\n"
        << "  flush                                    - Flush data to disk (not implemented yet)\n"
        << "  help                                     - Display this help message (not implemented yet)\n"
        << "  exit/quit                                - Exit the program (not implemented yet)\n";
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
                storage.update(args[1], record_id, string_to_bytes(args[3]));
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
                storage.delete_record(args[1], record_id);
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

            try {
				std::cout << "Will be implemented in future versions.\n";
            }
            catch (const std::exception& e) {
                std::cout << "Error: " << e.what() << std::endl;
            }
        }
        else if (command == "flush") {
            try {
                storage.flush();
                std::cout << "Storage flushed\n";
            }
            catch (const std::exception& e) {
                std::cout << "Error: " << e.what() << std::endl;
            }
        }
        else if (command == "create") {
            if (args.size() < 2) {
                std::cout << "Error: Missing table name argument. Usage: create <table name>\n";
                continue;
            }
            try {
                bool isSuccess = storage.create_table(args[1]);

                if (!isSuccess) {
                    std::cout << "Error: Table '" << args[1] << "' already exists\n";
                    continue;
				}

                std::cout << "Table '" << args[1] << "' created\n";
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
                    std::cout << "  " << table << "\n";
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