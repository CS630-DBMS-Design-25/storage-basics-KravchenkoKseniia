#include <string>
#include "ast.h"
#include <pg_query.h>
#include "json.hpp"
#include <iostream>

AST parse_sql_to_ast(const std::string& sql) {
	PgQueryParseResult res = pg_query_parse(sql.c_str());

	if (res.error) {
		std::string message = res.error->message;
		pg_query_free_parse_result(res);

		throw std::runtime_error("Parse error: " + message);
	}

	std::string json_string = res.parse_tree;
	pg_query_free_parse_result(res);

	auto root = nlohmann::json::parse(json_string);

	if (!root.contains("stmts") || !root["stmts"].is_array() || root.at("stmts").empty()) {
		throw std::runtime_error("Invalid JSON structure: 'stmts' is missing/empty/not an array");
	}

	nlohmann::json stmt_json = root.at("stmts").at(0).at("stmt");

	// debuging output

	std::cout << "Parsed JSON: " << stmt_json.dump(4) << std::endl;

	if (stmt_json.contains("CreateStmt")) {
		const auto& cs = stmt_json.at("CreateStmt");
		return parse_create_table_json(cs);
	}
	else if (stmt_json.contains("CreateTableAsStmt")) {
		const auto& ctas = stmt_json.at("CreateTableAsStmt");
		CTASStatement stmt;
		stmt.table_name = ctas.at("into").at("rel").at("relname").get<std::string>();
		stmt.selectStmt = parse_select_json(ctas.at("query").at("SelectStmt"));
		return stmt;
	}
	else if (stmt_json.contains("InsertStmt")) {
		return parse_insert_json(stmt_json["InsertStmt"]);
	}
	else if (stmt_json.contains("DeleteStmt")) {
		const auto& ds = stmt_json.at("DeleteStmt");
		DeleteStatement stmt;
		stmt.table_name = ds.at("relation").at("relname").get<std::string>();
		if (ds.contains("whereClause")) {
			auto& aexpr = ds.at("whereClause").at("A_Expr");
			stmt.where_column = aexpr.at("lexpr").at("ColumnRef").at("fields").at(0).at("String").at("sval").get<std::string>();
			stmt.where_operator = aexpr.at("name").at(0).at("String").at("sval").get<std::string>();
			auto& rvalue = aexpr.at("rexpr").at("A_Const");
			if (rvalue.contains("ival")) {
				stmt.where_value = std::to_string(rvalue.at("ival").at("ival").get<int>());
			}
			else {
				stmt.where_value = rvalue.at("sval").at("sval").get<std::string>();
			}
		}

		return stmt;
	}
	else if (stmt_json.contains("SelectStmt")) {
		return parse_select_json(stmt_json["SelectStmt"]);
	}

	throw std::runtime_error("Unknown statement");
}