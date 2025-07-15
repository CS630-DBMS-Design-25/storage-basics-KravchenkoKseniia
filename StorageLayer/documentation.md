# Storage Layer Documentation

# Overview
The solution provides a simple, page-based storage layer, which supports:
- Tables: Each table is stored with `.db` file on disk with matching `.schema` file for schema definition and `.index` file for indexing,
using a hashed-based algorithm on first column of the table.
- Records: Variable-length records packed into 4KB pages, using slotted page format.
- CRUD Operations: Basic Create, Drop, List for tables, and Create, Get, Update, Delete, Scan and Find for records.
- Hash Index: A simple hash index is created for the first column of each table, allowing for fast lookups.
- Vacuuming: Automatic vacuuming of pages to reclaim space after inserts/updates/deletes.

# On-Disk Structure
## `.db` File
Each 4KB page contains:
- Page Header: Contains metadata about the page, such as slot count, free space.
- Slot directory: An array of slots, each pointing to a record within the page.
- Records: Variable-length records stored in the free space area of the page. After deletition, marked by special offset in the slot directory.

## `.schema` File
A text file containing the schema definition of the table, including:
- Number of columns
- Column name, type (INT, VARCHAR) and length (static for INT, dynamic for VARCHAR)

## `.index` File
A text file containing the hash index for the first column of the table. 

# RID (Record Identifier)
A record ID packs page and slot into 32 bits integer
- Page ID: 16 bits for page number
- Slot ID: 16 bits for slot number within the page

# API Overview

## open/close
- `open db-name`: Opens the database file and loads the schema and index.
- `close`: Closes the database file and releases resources.

# create/drop/list
- `create table-name <columns>`: Creates a new table with the specified columns, only if it does not already exist and db is open.
- `drop table-name`: Drops the specified table, only if it exists and db is open.
- `list`: Lists all tables in the database, only if db is open.

# insert/update/delete
- `insert table-name <values>`: Inserts a new record into the specified table, only if db is open.
- `update table-name <rid> <values>`: Updates an existing record identified by the RID in the specified table, only if db is open.
- `delete table-name <rid>`: Deletes a record identified by the RID in the specified table, only if db is open.

# get/scan/find
- `get table-name <rid>`: Retrieves a record identified by the RID from the specified table, only if db is open.
- `scan table-name [--projection <field1>...}`: Scans all records in the specified table, only if db is open. Have an option to project specific fields.
- `find table-name <value>`: Finds records in the specified table where the first column matches the given value, only if db is open.

# --query `<SQL query>`

- `--query <SQL query>`: Executes a SQL-like query on the database, only if db is open. It uses an embedded SQL parser to interpret
the query and execute it against the database, AST and execution engine.

## Usage Example
```bash
# Open the database
open my_database
# Create Table As Select
storage-cli> --query "CREATE TABLE my_table AS SELECT * FROM another_table WHERE condition"
```
## Supported Statements
- `CREATE TABLE table_name (column1 type, column2 type, ...)`: Creates a new table with the specified columns.
- `INSERT INTO table_name (column1, column2, ...) VALUES (value1, value2, ...)`: Inserts a new record into the specified table.
- `SELECT column1, column2 FROM table_name WHERE condition`: Selects records from the specified table based on the condition.
- `DELETE FROM table_name WHERE condition`: Deletes records from the specified table based on the condition.
- `CREATE TABLE table_name AS SELECT column1, column2 FROM another_table WHERE condition`: Creates a new table based on the result of a SELECT query.

## How it works
1. In `main()`, the CLI scans input for the `--query` flag, extracts the SQL query, and passes it to the `AST parse_sql_to_ast` function.
2. Than I used pg_query parser to parse the SQL query into an AST (Abstract Syntax Tree) and as a result,we have a JSON object representing the AST.
3. `AST parse_sql_to_ast` function converts the JSON into C++ AST variant: 
```cpp
using AST = std::variant<CreateTableStatement,
	InsertStatement,
	SelectStatement,
	DeleteStatement,
	CTASStatement>;
```
It inspects top-level JSON key to determine the type of statement and constructs the corresponding AST node and calls one of:
- `parse_create_table_json()` - Creates a `CreateTableStatement` AST node from the JSON object.
- `parse_insert_json()` - Creates an `InsertStatement`.
- `parse_select_json()` - Creates a `SelectStatement`.
- `parse_delete_json()` - Creates a `DeleteStatement`.
- handles `CTASStatement` for `CREATE TABLE AS SELECT`.

4. Once we have the AST, we pass it to the `QueryExecutor` class, which is responsible for executing the query against the database:
- `executeInsert()` for `InsertStatement`.
- `executeSelect()` for `SelectStatement`.
- `executeDelete()` for `DeleteStatement`.
- `executeCreateTableAs()` for `CreateTableStatement`.