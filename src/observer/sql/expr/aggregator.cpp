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
// Created by Wangyunlai on 2024/05/29.
//

#include "sql/expr/aggregator.h"
#include "common/log/log.h"

RC SumAggregator::accumulate(const Value &value)
{
  if (value.is_null()) {
    return RC::SUCCESS;  // 忽略 NULL
  }

  if (!seen_non_null_) {
    value_ = value;
    seen_non_null_ = true;
    return RC::SUCCESS;
  }

  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s",
         attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));

  Value::add(value, value_, value_);
  return RC::SUCCESS;
}

RC SumAggregator::evaluate(Value& result)
{
  if (!seen_non_null_) {
    Value tmp;
    tmp.set_is_null(true);
    result = tmp;
  } else {
    result = value_;
  }
  return RC::SUCCESS;
}

RC MaxAggregator::accumulate(const Value &value)
{
  if (value.is_null()) {
    return RC::SUCCESS;  // 忽略 NULL
  }

  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    seen_non_null_ = true;
    return RC::SUCCESS;
  }

  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s",
         attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));

  if (value.compare(value_) > 0) {
    value_ = value;
  }
  return RC::SUCCESS;
}

RC MaxAggregator::evaluate(Value &result)
{
  if (!seen_non_null_) {
    Value tmp;
    tmp.set_is_null(true);
    result = tmp;
  } else {
    result = value_;
  }
  return RC::SUCCESS;
}

RC MinAggregator::accumulate(const Value &value)
{
  if (value.is_null()) {
    return RC::SUCCESS;  // 忽略 NULL
  }

  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    seen_non_null_ = true;
    return RC::SUCCESS;
  }

  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s",
         attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));

  if (value.compare(value_) < 0) {
    value_ = value;
  }
  return RC::SUCCESS;
}

RC MinAggregator::evaluate(Value &result)
{
  if (!seen_non_null_) {
    Value tmp;
    tmp.set_is_null(true);
    result = tmp;
  } else {
    result = value_;
  }
  return RC::SUCCESS;
}

RC CountAggregator::accumulate(const Value &value)
{
  if (value.is_null()) {
    return RC::SUCCESS;  // 忽略 NULL
  }
  count_++;
  return RC::SUCCESS;
}

RC CountAggregator::evaluate(Value &result)
{
  result.set_int(count_);
  return RC::SUCCESS;
}

RC AvgAggregator::accumulate(const Value &value)
{
  if (value.is_null()) {
    return RC::SUCCESS;  // 忽略 NULL
  }

  if (value.attr_type() == AttrType::INTS) {
    sum_ += static_cast<double>(value.get_int());
  } else if (value.attr_type() == AttrType::FLOATS) {
    sum_ += static_cast<double>(value.get_float());
  } else {
    ASSERT(false, "AVG only supports numeric types");
    return RC::INVALID_ARGUMENT;
  }
  count_++;
  return RC::SUCCESS;
}

RC AvgAggregator::evaluate(Value &result)
{
  if (count_ == 0) {
    Value tmp;
    tmp.set_is_null(true);
    result = tmp;
  } else {
    float avg = static_cast<float>(sum_ / static_cast<double>(count_));
    result.set_float(avg);
  }
  return RC::SUCCESS;
}
