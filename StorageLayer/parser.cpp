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

	nlohmann::json stmt_json;

	if (root.is_array()) {
		stmt_json = root.at(0)["RawStmt"]["stmt"];
	}
	else if (root.contains("stmts")) {
		auto arr = root["stmts"];
		stmt_json = arr.at(0)["stmt"];
	}
	else if (root.contains("RawStmt")) {
		stmt_json = root["RawStmt"]["stmt"];
	}
	else if (root.contains("parsetree")) {
		auto arr = root["parsetree"];
		stmt_json = arr.at(0)["RawStmt"]["stmt"];
	}
	else {
		throw std::runtime_error("Unexpected format from pg_query");
	}

	if (stmt_json.contains("CreateStmt")) {
		return parse_create_table_json(stmt_json["CreateStmt"]);
	}
	else if (stmt_json.contains("InsertStmt")) {
		return parse_insert_json(stmt_json["InsertStmt"]);
	}
	else if (stmt_json.contains("SelectStmt")) {
		return parse_select_json(stmt_json["SelectStmt"]);
	}

	throw std::runtime_error("Unknown statement");
}