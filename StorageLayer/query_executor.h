#pragma once

#include <string>
#include <vector>
#include <optional>
#include "file_storage_layer.h"
#include "table_schema.h"
#include "ast.h"


class QueryExecutor
{
	FileStorageLayer& storage;

	std::vector<uint8_t> packRecord(const TableSchema& schema, const std::vector<std::string>& values);
	std::vector<std::string> unpackRecord(const TableSchema& schema, const std::vector<uint8_t>& values);

public:
	QueryExecutor(FileStorageLayer& s);

	int executeInsert(const InsertStatement& insertStmt);

	std::vector<std::vector<std::string>> executeSelect(const SelectStatement& selectStmt);

	size_t executeDelete(const DeleteStatement& deleteStmt);

	int executeCreateTableAs(const CTASStatement& ctasStmt);

	std::vector<std::vector<std::string>> executeHashJoin(const SelectStatement& stmt,
		const TableSchema& leftSchema, const TableSchema& rightSchema,
		std::vector<std::vector<uint8_t>> &leftData, std::vector<std::vector<uint8_t>>& rightData);

	std::vector<std::vector<std::string>> applyAgregation(const SelectStatement& stmt,
		const TableSchema& schema, const std::vector<std::vector<std::string>>& rows);
};

