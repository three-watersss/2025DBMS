/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2022/07/01.
//

#include "sql/operator/project_physical_operator.h"
#include "common/log/log.h"
#include "storage/record/record.h"
#include "storage/table/table.h"
#include "sql/expr/expression.h"
#include "common/lang/unordered_set.h"

using namespace std;

ProjectPhysicalOperator::ProjectPhysicalOperator(vector<unique_ptr<Expression>> &&expressions)
  : expressions_(std::move(expressions)), tuple_(expressions_)
{
}

RC ProjectPhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  PhysicalOperator *child = children_[0].get();
  RC                rc    = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  return RC::SUCCESS;
}

RC ProjectPhysicalOperator::next()
{
  if (children_.empty()) {
    return RC::RECORD_EOF;
  }
  return children_[0]->next();
}

RC ProjectPhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}
Tuple *ProjectPhysicalOperator::current_tuple()
{
  tuple_.set_tuple(children_[0]->current_tuple());
  return &tuple_;
}

RC ProjectPhysicalOperator::tuple_schema(TupleSchema &schema) const
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