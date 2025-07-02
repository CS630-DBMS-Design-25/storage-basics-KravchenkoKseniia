#include "ast.h"
#include "json.hpp"

CreateTableStatement parse_create_table_json(const nlohmann::json& json) {
	CreateTableStatement stmt;
	stmt.table_name = json.at("relation").at("relname").get<std::string>();

	for (auto& column : json.at("tableElts")) {
		auto& column_def = column.at("ColumnDef");
		std::string column_name = column_def.at("colname").get<std::string>();

		auto typeName = column_def.at("typeName");

		std::string type = typeName.at("names").at(1).at("String").at("sval").get<std::string>();

		if (type == "int4") {
			stmt.columns.emplace_back(column_name, "INT");
		}
		else if (type == "varchar") {
			int len = typeName.value("typmods", nlohmann::json::array()).at(0).at("A_Const").at("ival").at("ival").get<int>();
			stmt.columns.emplace_back(column_name, "VARCHAR(" + std::to_string(len) + ")");
		}
		else {
			stmt.columns.emplace_back(column_name, type);
		}
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