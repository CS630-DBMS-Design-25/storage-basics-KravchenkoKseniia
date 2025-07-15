#pragma once
#include <string>
#include "ast.h"
#include <pg_query.h>
#include "json.hpp"

AST parse_sql_to_ast(const std::string& sql);