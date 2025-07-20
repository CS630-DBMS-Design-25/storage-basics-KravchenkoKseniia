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
	stmt.table_name = json.at("relation").at("relname").get<std::string>();
	
	auto& valuesLists = json.at("selectStmt").at("SelectStmt").at("valuesLists");

	if (!valuesLists.empty()) {
		auto items = valuesLists.at(0).at("List").at("items");

		for (auto& item : items) {
			auto& aconst = item.at("A_Const");

			if (aconst.contains("ival")) {
				int value = aconst.at("ival").at("ival").get<int>();

				stmt.values.push_back(std::to_string(value));
			}
			else if (aconst.contains("sval")) {
				std::string value = aconst.at("sval").at("sval").get<std::string>();
				stmt.values.push_back(value);
			}
			else {
				stmt.values.push_back(aconst.dump());
			}
		}
	}

	return stmt;
}

SelectStatement parse_select_json(const nlohmann::json& json) {
	SelectStatement stmt;

	stmt.table_name = json.at("fromClause").at(0).at("RangeVar").at("relname");

	bool star = false;

	//projection
	for (auto& target : json.at("targetList")) {
		auto& res_target = target.at("ResTarget").at("val");
		if (res_target.contains("ColumnRef")) {
			auto& fields = res_target.at("ColumnRef").at("fields");

			if (fields.at(0).contains("A_Star")) {
				star = true;
				break;
			}

			for (auto& field: fields) {
				if (field.contains("String")) {
					stmt.columns.push_back(field.at("String").at("sval"));
				}
			}
		}
	}

	if (star) {
		stmt.columns.clear();
	}

	// WHERE

	if (json.contains("whereClause")) {
		auto& where_clause = json.at("whereClause").at("A_Expr");

		stmt.where_column = where_clause.at("lexpr").at("ColumnRef").at("fields").at(0).at("String").at("sval");

		stmt.where_operator = where_clause.at("name").at(0).at("String").at("sval");
		
		auto& rc = where_clause.at("rexpr").at("A_Const");
		if (rc.contains("ival"))
			stmt.where_value = std::to_string(rc.at("ival").at("ival").get<int>());
		else
			stmt.where_value = rc.at("sval").at("sval").get<std::string>();
	}

	// ORDER BY

	if (json.contains("sortClause") && !stmt.columns.empty()) {
		auto& sort_clause = json.at("sortClause").at(0).at("SortBy");
		stmt.order_by_column = sort_clause.at("node").at("ColumnRef").at("fields").at(0).at("String").at("sval");
	}

	// LIMIT

	if (json.contains("limitCount")) {
		auto& limit_clause = json.at("limitCount").at("A_Const");
		stmt.limit = limit_clause.at("ival").at("ival").get<int>();
	}
	
	return stmt;
}