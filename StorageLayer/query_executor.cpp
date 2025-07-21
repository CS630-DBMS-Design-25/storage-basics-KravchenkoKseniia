#include "query_executor.h"

std::vector<uint8_t> QueryExecutor::packRecord(const TableSchema& schema, const std::vector<std::string>& values)
{
	std::vector<uint8_t> packedRecord;

	for(size_t i = 0; i < schema.columns.size(); i++)
	{
		const auto& column = schema.columns[i];
		std::string value = i < values.size() ? values[i] : "";

		if (column.type == DataType::INT) {
			int val = std::stoi(value);
			uint8_t buffer[sizeof(int)];
			std::memcpy(buffer, &val, sizeof(int));
			packedRecord.insert(packedRecord.end(), buffer, buffer + sizeof(int));
		}
		else {
			uint16_t strLength = (uint16_t)std::min((size_t)column.length, value.size());
			uint8_t lengthBuffer[sizeof(uint16_t)];
			std::memcpy(lengthBuffer, &strLength, sizeof(uint16_t));
			packedRecord.insert(packedRecord.end(), lengthBuffer, lengthBuffer + sizeof(uint16_t));
			packedRecord.insert(packedRecord.end(), value.begin(), value.begin() + strLength);
		}
	}

	return packedRecord;
}

std::vector<std::string> QueryExecutor::unpackRecord(const TableSchema& schema, const std::vector<uint8_t>& values)
{
	std::vector<std::string> unpackedRecord;
	size_t offset = 0;

	for (auto& column : schema.columns) {
		if (column.type == DataType::INT) {
			if (offset + sizeof(int) > values.size()) {
				throw std::runtime_error("Invalid record size for INT column");
			}
			int val;
			std::memcpy(&val, values.data() + offset, sizeof(int));
			unpackedRecord.push_back(std::to_string(val));
			offset += sizeof(int);
		}
		else {
			if (offset + sizeof(uint16_t) > values.size()) {
				throw std::runtime_error("Invalid record size for STRING column");
			}
			uint16_t strLength;
			std::memcpy(&strLength, values.data() + offset, sizeof(uint16_t));

			offset += sizeof(uint16_t);
			if (offset + strLength > values.size()) {
				throw std::runtime_error("Invalid record size for STRING column");
			}
			unpackedRecord.emplace_back((char*)values.data() + offset, strLength);
			offset += strLength;
		}
	}
	return unpackedRecord;
}


QueryExecutor::QueryExecutor(FileStorageLayer& s) : storage(s) {}

int QueryExecutor::executeInsert(const InsertStatement& stmt)
{
	auto schema = storage.get_table_schema(stmt.table_name);
	if (schema.columns.empty()) {
		throw std::runtime_error("Table schema not found for " + stmt.table_name);
	}

	auto packedRecord = packRecord(schema, stmt.values);
	if (packedRecord.empty()) {
		throw std::runtime_error("No values provided for insert into " + stmt.table_name);
	}
	return storage.insert(stmt.table_name, packedRecord);
};

std::vector<std::vector<std::string>> QueryExecutor::executeSelect(const SelectStatement& stmt)
{
	auto schema = storage.get_table_schema(stmt.table_name);

	auto selectColumns = stmt.columns;

	if(schema.columns.empty()) {
		throw std::runtime_error("Table schema not found for " + stmt.table_name);
	}


	std::optional<std::function<bool(const std::vector<uint8_t>&)>> filter_func;
	if (stmt.where_column) {
		int index = -1;
		for (int i = 0; i < (int)schema.columns.size(); i++) {
			if (schema.columns[i].name == *stmt.where_column) {
				index = i;
				break;
			}
		}

		if (index < 0) {
			throw std::runtime_error("Unknown WHERE column");
		}

		filter_func = [=](auto& raw) {
			auto fields = unpackRecord(schema, raw);
			auto& fv = fields[index];

			if (*stmt.where_operator == ">") {
				return fv > *stmt.where_value;
			}

			if (*stmt.where_operator == "<") {
				return fv < *stmt.where_value;
			}

			if (*stmt.where_operator == "=") {
				return fv == *stmt.where_value;
			}

			if (*stmt.where_operator == "<=") {
				return fv <= *stmt.where_value;
			}

			if (*stmt.where_operator == ">=") {
				return fv >= *stmt.where_value;
			}

			if (*stmt.where_operator == "!=") {
				return fv != *stmt.where_value;
			}

			return false;
		};
	}

	auto left_raws = storage.scan(stmt.table_name, std::nullopt, std::nullopt, filter_func);

	std::vector<std::vector<std::string>> rows;
	TableSchema resultSchema;

	if (stmt.join_table) {
		TableSchema left_schema = storage.get_table_schema(stmt.table_name);
		TableSchema right_schema = storage.get_table_schema(*stmt.join_table);
		auto right_raws = storage.scan(*stmt.join_table, std::nullopt, std::nullopt, std::nullopt);

		rows = executeHashJoin(stmt, left_schema, right_schema, left_raws, right_raws);

		resultSchema.columns.clear();

		for (auto& col : left_schema.columns)
			resultSchema.columns.push_back({ stmt.table_name + "." + col.name, col.type, col.length });

		for (auto& col : right_schema.columns)
			resultSchema.columns.push_back({ *stmt.join_table + "." + col.name, col.type, col.length });
		
	}
	else {
		resultSchema = storage.get_table_schema(stmt.table_name);
		for (auto& raw : left_raws) {
			rows.push_back(unpackRecord(resultSchema, raw));
		}
	}


	if (!stmt.aggregate_functions.empty()) {
		rows = applyAgregation(stmt, schema, rows);

		resultSchema.columns.clear();

		for (auto& col : stmt.group_by) {
			auto it = std::find_if(schema.columns.begin(), schema.columns.end(),
				[&](auto& c) { return c.name == col; });
			resultSchema.columns.push_back({ col, it->type, it->length });
		}

		for (auto& agg_func : stmt.aggregate_functions) {
			std::string agg_col_name = agg_func.function_name + "(" + agg_func.column_name + ")";
			resultSchema.columns.push_back({ agg_col_name, DataType::INT, sizeof(int) });
			selectColumns.push_back(agg_col_name);
		}

	}

	for (auto& spec : stmt.scalar_functions) {
		int columnIndex = std::distance(
			schema.columns.begin(),
			std::find_if(schema.columns.begin(), schema.columns.end(),
			[&](auto& c) { return c.name == spec.arguments[0]; }));

		std::string alias = spec.function_name + "(" + spec.arguments[0] + ")";
		resultSchema.columns.push_back({ alias, DataType::VARCHAR, schema.columns[columnIndex].length });
		selectColumns.push_back(alias);

		for (auto& row : rows) {
			std::string resultValue;
			if (spec.function_name == "substr" && spec.arguments.size() == 3) {
				int start = std::stoi(spec.arguments[1]);
				int length = std::stoi(spec.arguments[2]);
				if (columnIndex >= 0 && columnIndex < (int)row.size()) {
					resultValue = row[columnIndex].substr(start, length);
				}
			}
			else if (spec.function_name == "upper" && columnIndex >= 0 && columnIndex < (int)row.size()) {
				resultValue = row[columnIndex];
				std::transform(resultValue.begin(), resultValue.end(), resultValue.begin(), ::toupper);
			}
			else if (spec.function_name == "lower" && columnIndex >= 0 && columnIndex < (int)row.size()) {
				resultValue = row[columnIndex];
				std::transform(resultValue.begin(), resultValue.end(), resultValue.begin(), ::tolower);
			}

			row.push_back(std::move(resultValue));
		}
	}

	if (!selectColumns.empty()) {
		std::vector<std::vector<std::string>> projected;
		std::vector<int> column_indices;

		for(auto& col : selectColumns) {

			auto it = std::find_if(resultSchema.columns.begin(), resultSchema.columns.end(),
				[&](const auto& c) { return c.name == col; });

			if (it == resultSchema.columns.end()) {
				throw std::runtime_error("Unknown column: " + col);
			}

			column_indices.push_back(std::distance(resultSchema.columns.begin(), it));
		}

		for (auto& row : rows) {
			std::vector<std::string> projected_row;
			for (int index : column_indices) {
				if (index < 0 || index >= (int)row.size()) {
					throw std::runtime_error("Column index out of bounds: " + std::to_string(index));
				}
				projected_row.push_back(row[index]);
			}
			projected.push_back(std::move(projected_row));
		}
		return projected;
	}

	return rows;
}

size_t QueryExecutor::executeDelete(const DeleteStatement& stmt)
{
	auto schema = storage.get_table_schema(stmt.table_name);
	if (schema.columns.empty()) {
		throw std::runtime_error("Table schema not found for " + stmt.table_name);
	}

	std::optional<std::function<bool(const std::vector<uint8_t>&)>> filter_func;
	if (stmt.where_column) {
		int index = -1;
		for (int i = 0; i < (int)schema.columns.size(); i++) {
			if (schema.columns[i].name == *stmt.where_column) {
				index = i;
				break;
			}
		}

		if (index < 0) {
			throw std::runtime_error("Unknown WHERE column");
		}

		filter_func = [=](auto& raw) {
			auto fields = unpackRecord(schema, raw);
			auto& fv = fields[index];

			if (*stmt.where_operator == ">") {
				return fv > *stmt.where_value;
			}

			if (*stmt.where_operator == "<") {
				return fv < *stmt.where_value;
			}

			if (*stmt.where_operator == "=") {
				return fv == *stmt.where_value;
			}

			if (*stmt.where_operator == "<=") {
				return fv <= *stmt.where_value;
			}

			if (*stmt.where_operator == ">=") {
				return fv >= *stmt.where_value;
			}

			if (*stmt.where_operator == "!=") {
				return fv != *stmt.where_value;
			}

			return false;
		};
	}

	std::vector<int> ids;
	storage.scan(
		stmt.table_name, 
		[&](int record_id, const std::vector<uint8_t>& raw) {
			ids.push_back(record_id);
			return true;
		}, 
		std::nullopt,
		filter_func
	);

	size_t deleted = 0;
	for (int id : ids) {
		if (storage.delete_record(stmt.table_name, id)) {
			deleted++;
		}
	}
	return deleted;
}

int QueryExecutor::executeCreateTableAs(const CTASStatement& stmt)
{
	auto rows = executeSelect(stmt.selectStmt);

	auto schema = storage.get_table_schema(stmt.selectStmt.table_name);
	if (schema.columns.empty()) {
		throw std::runtime_error("Source table not found");
	}

	if (!storage.create_table(stmt.table_name, schema)) {
		throw std::runtime_error("Could not create table: " + stmt.table_name);
	}

	for (auto& row : rows) {
		auto b = packRecord(schema, row);
		storage.insert(stmt.table_name, b);
	}

	return (int)rows.size();
}

std::vector<std::vector<std::string>> QueryExecutor::executeHashJoin(const SelectStatement& stmt,
	const TableSchema& leftSchema, const TableSchema& rightSchema,
	std::vector<std::vector<uint8_t>>& leftData, std::vector<std::vector<uint8_t>>& rightData)
{
	int leftJoinIndex = std::distance(leftSchema.columns.begin(),
		std::find_if(leftSchema.columns.begin(), leftSchema.columns.end(),
			[&](auto& c) { return c.name == *stmt.join_left_column; }));

	int rightJoinIndex = std::distance(rightSchema.columns.begin(),
		std::find_if(rightSchema.columns.begin(), rightSchema.columns.end(),
			[&](auto& c) { return c.name == *stmt.join_right_column; }));

	const auto& buildRaws = (leftData.size() < rightData.size()) ? leftData : rightData;
	const TableSchema& buildSchema = (leftData.size() < rightData.size()) ? leftSchema : rightSchema;
	int buildIndex = (leftData.size() < rightData.size()) ? leftJoinIndex : rightJoinIndex;

	std::unordered_map<std::string, std::vector<std::string>> hashTable;

	for (auto& raw : buildRaws) {
		auto fields = unpackRecord(buildSchema, raw);
		if (fields.size() <= buildIndex) {
			throw std::runtime_error("Join column index out of bounds");
		}
		hashTable.emplace(fields[buildIndex], fields);
	}

	std::vector<std::vector<std::string>> resultRows;
	const auto& probeRaws = (leftData.size() < rightData.size()) ? rightData : leftData;
	const TableSchema& probeSchema = (leftData.size() < rightData.size()) ? rightSchema : leftSchema;
	int probeIndex = (leftData.size() < rightData.size()) ? rightJoinIndex : leftJoinIndex;

	for (auto& raw : probeRaws) {
		auto fields = unpackRecord(probeSchema, raw);
		if (fields.size() <= probeIndex) {
			throw std::runtime_error("Join column index out of bounds");
		}
		
		auto rangeIt = hashTable.equal_range(fields[probeIndex]);
		for (auto it = rangeIt.first; it != rangeIt.second; it++) {
			std::vector<std::string> joinedRow;
			const auto& buildRow = it->second;

			bool buildLeft = leftData.size() < rightData.size();

			if (buildLeft) {
				joinedRow.insert(joinedRow.end(), buildRow.begin(), buildRow.end());
				joinedRow.insert(joinedRow.end(), fields.begin(), fields.end());
			}
			else {
				joinedRow.insert(joinedRow.end(), fields.begin(), fields.end());
				joinedRow.insert(joinedRow.end(), buildRow.begin(), buildRow.end());
			}

			resultRows.push_back(std::move(joinedRow));
		}
	}

	return resultRows;
}

std::vector<std::vector<std::string>> QueryExecutor::applyAgregation(const SelectStatement& stmt,
	const TableSchema& schema, const std::vector<std::vector<std::string>>& rows)
{
	std::vector<int> groupByIndices;
	for (auto& column : stmt.group_by) {
		int index = std::distance(schema.columns.begin(),
			std::find_if(schema.columns.begin(), schema.columns.end(),
				[&](auto& c) { return c.name == column; }));
		groupByIndices.push_back(index);
	}

	std::map <std::vector<std::string>, std::pair<std::vector<std::string>, std::string >> state;

	auto& agg_func = stmt.aggregate_functions[0];

	int aggIndex = std::distance(schema.columns.begin(),
		std::find_if(schema.columns.begin(), schema.columns.end(),
			[&](auto& c) { return c.name == agg_func.column_name; }));

	for (auto& row : rows) {

		std::vector<std::string> groupKey;
		for (int index : groupByIndices) {
			if (index < 0 || index >= (int)row.size()) {
				throw std::runtime_error("Group by index out of bounds");
			}
			groupKey.push_back(row[index]);
		}

		auto it = state.find(groupKey);
		
		if (it == state.end()) {
			state.emplace(groupKey, std::make_pair(groupKey, row[aggIndex]));
		}
		else {
			if (row[aggIndex] > it->second.second) {
				it->second.second = row[aggIndex];
			}
		}
	}

	std::vector<std::vector<std::string>> resultRows;

	for (const auto& [key, value] : state) {
		std::vector<std::string> resultRow = key;
		resultRow.push_back(value.second); // Add the aggregated value
		resultRows.push_back(std::move(resultRow));
	}

	return resultRows;
};

