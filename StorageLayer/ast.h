#pragma once
#include <string>
#include <vector>
#include <variant>
#include "json.hpp"

struct CreateTableStatement {
	std::string table_name;
	std::vector<std::pair<std::string, std::string>> columns;
};

struct InsertStatement {
	std::string table_name;
	std::vector<std::string> values; // Values to insert, can be strings or numbers
};

struct SelectStatement {
	std::string table_name;
	std::vector<std::string> columns; // Columns to select, empty means all columns
};

using AST = std::variant<CreateTableStatement, InsertStatement, SelectStatement>;


CreateTableStatement parse_create_table_json(const nlohmann::json& json);
InsertStatement parse_insert_json(const nlohmann::json& json);
SelectStatement parse_select_json(const nlohmann::json& json);

