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

	auto raws = storage.scan(stmt.table_name, std::nullopt, std::nullopt, filter_func);

	std::vector<std::vector<std::string>> rows;
	for (auto& r : raws) {
		auto all = unpackRecord(schema, r);
		if (!stmt.columns.empty()) {
			std::vector<std::string> pr;
			for (auto& column : stmt.columns) {
				auto it = std::find_if(schema.columns.begin(), schema.columns.end(), [&](auto& c) {return c.name == column; });
				int index = std::distance(schema.columns.begin(), it);
				pr.push_back(all[index]);
			}

			rows.push_back(std::move(pr));
		}
		else {
			rows.push_back(std::move(all));
		}

		if (stmt.limit && rows.size() >= *stmt.limit) {
			break;
		}
	}

	if (stmt.order_by_column) {
		int col_index = std::distance(stmt.columns.begin(),
			std::find(stmt.columns.begin(), stmt.columns.end(), *stmt.order_by_column));

		if (col_index >= 0) {
			std::sort(rows.begin(), rows.end(),
				[&](auto& a, auto& b) {return a[col_index] < b[col_index]; });
		}
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
};

