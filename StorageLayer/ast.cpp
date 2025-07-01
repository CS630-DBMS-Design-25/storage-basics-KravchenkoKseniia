#include "ast.h"
#include "json.hpp"

CreateTableStatement parse_create_table_json(const nlohmann::json& json) {
	CreateTableStatement stmt;
	stmt.table_name = json["relation"]["relname"];

	for (auto& column : json["tableElts"]) {
		auto& column_def = column["ColumnDef"];
		std::string column_name = column_def["colname"];

		auto names = column_def["typeName"]["names"];
		std::string type_name = names.back();

		stmt.columns.push_back({ column_name, type_name });
	}

	return stmt;
}

InsertStatement parse_insert_json(const nlohmann::json& json) {
	InsertStatement stmt;
	stmt.table_name = json["relation"]["relname"];
	for (auto& value : json["selectStmt"]["SelectStmt"]["valueLists"][0]) {
		stmt.values.push_back(value.dump());
	}
	return stmt;
}

SelectStatement parse_select_json(const nlohmann::json& json) {
	SelectStatement stmt;
	auto& selected = json["selectStmt"]["SelectStmt"];

	for (auto& table : selected["targetList"]) {
		stmt.columns.push_back(table["ResTarget"]["val"]["ColumnRef"]["fields"][1]);
	}

	stmt.table_name = selected["fromClause"][0]["relname"];

	return stmt;
}