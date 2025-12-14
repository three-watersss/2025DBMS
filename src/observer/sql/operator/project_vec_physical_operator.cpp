/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/operator/project_vec_physical_operator.h"
#include "common/log/log.h"
#include "storage/record/record.h"
#include "storage/table/table.h"
#include "sql/expr/expression.h"
#include "common/lang/unordered_set.h"

using namespace std;

ProjectVecPhysicalOperator::ProjectVecPhysicalOperator(vector<unique_ptr<Expression>> &&expressions)
    : expressions_(std::move(expressions))
{
  int expr_pos = 0;
  for (auto &expr : expressions_) {
    chunk_.add_column(make_unique<Column>(expr->value_type(), expr->value_length()), expr_pos);
    expr_pos++;
  }
}
RC ProjectVecPhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }
  RC rc = children_[0]->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  return RC::SUCCESS;
}

RC ProjectVecPhysicalOperator::next(Chunk &chunk)
{
  if (children_.empty()) {
    return RC::RECORD_EOF;
  }
  chunk_.reset();
  RC rc = children_[0]->next(chunk_);
  if (rc == RC::RECORD_EOF) {
    return rc;
  } else if (rc == RC::SUCCESS) {
    rc = chunk.reference(chunk_);
  } else {
    LOG_WARN("failed to get next tuple: %s", strrc(rc));
    return rc;
  }
  return rc;
}

RC ProjectVecPhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}

RC ProjectVecPhysicalOperator::tuple_schema(TupleSchema &schema) const
{
  // 检查是否是多表查询：收集所有FieldExpr的表名
  unordered_set<string> table_names;
  for (const unique_ptr<Expression> &expression : expressions_) {
    if (expression->type() == ExprType::FIELD) {
      const FieldExpr *field_expr = static_cast<const FieldExpr *>(expression.get());
      const char *table_name = field_expr->table_name();
      if (table_name != nullptr && strlen(table_name) > 0) {
        table_names.insert(string(table_name));
      }
    }
  }
  bool is_multi_table = table_names.size() > 1;

  // 为每个表达式创建TupleCellSpec
  for (const unique_ptr<Expression> &expression : expressions_) {
    if (expression->type() == ExprType::FIELD) {
      const FieldExpr *field_expr = static_cast<const FieldExpr *>(expression.get());
      const char *table_name = field_expr->table_name();
      const char *field_name = field_expr->field_name();
      
      if (is_multi_table && table_name != nullptr && strlen(table_name) > 0) {
        // 多表查询：使用表名.字段名
        schema.append_cell(table_name, field_name);
      } else {
        // 单表查询：只使用字段名
        schema.append_cell(expression->name());
      }
    } else {
      // 非FieldExpr表达式：使用原来的方式
      schema.append_cell(expression->name());
    }
  }
  return RC::SUCCESS;
}
