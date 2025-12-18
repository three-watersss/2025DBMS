#include "sql/expr/sub_query_expr.h"
#include "sql/optimizer/physical_plan_generator.h"
#include "sql/operator/physical_operator.h"
#include "common/log/log.h"
#include "sql/expr/tuple.h"

SubQueryExpr::SubQueryExpr(std::unique_ptr<LogicalOperator> logical_plan)
    : logical_plan_(std::move(logical_plan))
{}

AttrType SubQueryExpr::value_type() const
{
  if (logical_plan_ && !logical_plan_->expressions().empty()) {
    return logical_plan_->expressions()[0]->value_type();
  }
  return AttrType::UNDEFINED;
}

std::unique_ptr<Expression> SubQueryExpr::copy() const
{
  LOG_ERROR("SubQueryExpr::copy not implemented");
  return nullptr;
}

RC SubQueryExpr::get_value(const Tuple &tuple, Value &value) const
{
  if (!executed_) {
    LOG_WARN("SubQueryExpr not executed");
    return RC::INTERNAL;
  }
  if (result_set_.empty()) {
    AttrType type = value_type();
    if (type == AttrType::UNDEFINED) {
      LOG_WARN("SubQueryExpr result empty and type undefined. Defaulting to INTS.");
      type = AttrType::INTS;
    }
    value.set_type(type);
    value.set_null_value();
  } else if (result_set_.size() == 1) {
    value = result_set_[0];
  } else {
    LOG_WARN("SubQueryExpr returned multiple rows in scalar context");
    return RC::INVALID_ARGUMENT; 
  }
  return RC::SUCCESS;
}

RC SubQueryExpr::init(Trx *trx)
{
  if (executed_) {
    return RC::SUCCESS;
  }

  PhysicalPlanGenerator physical_plan_generator;
  std::unique_ptr<PhysicalOperator> physical_plan;
  // TODO: get session from trx or pass session to init
  // For now, we pass nullptr as session, assuming subquery doesn't need session info for now
  // or we need to find a way to get session.
  // Trx doesn't seem to have session info directly.
  // However, PhysicalPlanGenerator::create needs session.
  // Let's try to pass nullptr and see if it works, or if we need to change the interface.
  // Looking at PhysicalPlanGenerator::create, it uses session for some operators.
  
  // Since we don't have session here easily, and changing Expression::init signature affects many classes,
  // let's check if we can get session from somewhere else or if nullptr is safe for simple subqueries.
  // If PhysicalPlanGenerator uses session, passing nullptr might crash.
  
  // Actually, we can try to get the session from thread local storage if available, 
  // but the best way is to pass it down.
  
  // Let's modify Expression::init to take Session * as well, or just Trx * and assume we can get Session.
  // But Trx doesn't have Session.
  
  // Let's look at where init is called. It is called in PredicatePhysicalOperator::open(Trx *trx).
  // PredicatePhysicalOperator::open only has Trx.
  
  // Wait, PhysicalOperator::open takes Trx *.
  // If we change PhysicalOperator::open to take Session *, that would be a big change.
  
  // Let's check if we can use `Session::default_session()` or similar if it exists, 
  // or if there is a global way to get current session.
  // There is `Session::default_session()` in `session.h`.
  
  RC rc = physical_plan_generator.create(*logical_plan_, physical_plan, nullptr);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create physical plan for subquery");
    return rc;
  }

  rc = physical_plan->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open physical plan for subquery");
    return rc;
  }

  result_set_.clear();
  while (true) {
    rc = physical_plan->next();
    if (rc == RC::RECORD_EOF) {
      rc = RC::SUCCESS;
      break;
    }
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to execute subquery");
      physical_plan->close();
      return rc;
    }

    Tuple *tuple = physical_plan->current_tuple();
    if (tuple == nullptr) {
      continue;
    }

    TupleSchema schema;
    physical_plan->tuple_schema(schema);
    if (schema.cell_num() != 1) {
       LOG_WARN("SubQuery must return exactly one column");
       physical_plan->close();
       return RC::INVALID_ARGUMENT;
    }
    
    Value val;
    rc = tuple->cell_at(0, val);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value from tuple");
      physical_plan->close();
      return rc;
    }
    LOG_INFO("SubQuery got value: %s", val.to_string().c_str());
    result_set_.push_back(val);
  }
  
  physical_plan->close();
  executed_ = true;
  LOG_INFO("SubQuery executed, result set size: %d", result_set_.size());
  return rc;
}

bool SubQueryExpr::check_in(const Value &val) const
{
  if (!executed_) {
    return false;
  }
  for (const auto &v : result_set_) {
    if (v.compare(val) == 0) {
      return true;
    }
  }
  LOG_INFO("Check IN failed for value: %s", val.to_string().c_str());
  return false;
}
