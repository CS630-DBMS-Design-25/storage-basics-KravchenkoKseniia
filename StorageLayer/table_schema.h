#pragma once
#include <string>
#include <vector>

enum DataType {
	INT,
	VARCHAR
};

struct Column {
	std::string name;
	DataType type;
	int length; // For VARCHAR, this is the maximum length
};

struct TableSchema {
	std::vector<Column> columns;
};