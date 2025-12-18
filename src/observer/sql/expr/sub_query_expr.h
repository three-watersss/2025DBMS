#pragma once

#include "sql/expr/expression.h"
#include "sql/operator/logical_operator.h"
#include "common/lang/vector.h"

class SubQueryExpr : public Expression
{
public:
  SubQueryExpr(std::unique_ptr<LogicalOperator> logical_plan);
  ~SubQueryExpr() override = default;

  ExprType type() const override { return ExprType::SUB_QUERY; }

  AttrType value_type() const override;

  std::unique_ptr<Expression> copy() const override;

  RC get_value(const Tuple &tuple, Value &value) const override;

  RC init(Trx *trx) override;

  bool check_in(const Value &val) const;

  const std::vector<Value> &result_set() const { return result_set_; }

private:
  std::unique_ptr<LogicalOperator> logical_plan_;
  std::vector<Value> result_set_;
  bool executed_ = false;
};
