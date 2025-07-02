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
	bool isStar = false;

	auto& targets = json.at("targetList");

	for (auto& element : targets) {
		auto& resTarget = element.at("ResTarget").at("val");

		if (resTarget.contains("ColumnRef")) {
			auto& ref = resTarget.at("ColumnRef");
			auto& fields = ref.at("fields");

			for (auto& field : fields) {
				if (field.contains("A_Star")) {
					isStar = true;
					break;
				}
				else if (field.contains("String")) {
					stmt.columns.push_back(field.at("String").at("sval").get<std::string>());
				}
			}

			if (isStar) {
				break;
			}
		}
	}

	if (isStar) {
		stmt.columns.clear();
	}

	auto& fromClause = json.at("fromClause").at(0).at("RangeVar");
	stmt.table_name = fromClause.at("relname").get<std::string>();

	return stmt;
}