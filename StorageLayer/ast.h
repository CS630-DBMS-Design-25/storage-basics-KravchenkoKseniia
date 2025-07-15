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
	std::optional<std::string> where_column; // Optional WHERE clause column
	std::optional<std::string> where_operator; // Optional WHERE clause operator (e.g., '=', '>', '<', etc.)
	std::optional<std::string> where_value; // Optional WHERE clause value
	std::optional<std::string> order_by_column; // Optional ORDER BY column
	std::optional<size_t> limit; // Optional LIMIT clause
};

struct DeleteStatement
{
	std::string table_name;
	std::optional<std::string> where_column;
	std::optional<std::string> where_operator;
	std::optional<std::string> where_value;
};

struct CTASStatement
{
	std::string table_name;
	SelectStatement selectStmt;
};

using AST = std::variant<CreateTableStatement,
	InsertStatement,
	SelectStatement,
	DeleteStatement,
	CTASStatement>;


CreateTableStatement parse_create_table_json(const nlohmann::json& json);
InsertStatement parse_insert_json(const nlohmann::json& json);
SelectStatement parse_select_json(const nlohmann::json& json);

