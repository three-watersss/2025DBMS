#pragma once

#include "common/type/data_type.h"

/**
 * @brief 日期数据类型
 * @ingroup DataType
 */
class DateType : public DataType
{
public:
  DateType() : DataType(AttrType::DATES) {}
  virtual ~DateType() = default;

  static bool is_valid(const string date);

  static int to_int(const string date);

  static const string to_str(int date);

  int compare(const Value &left, const Value &right) const override;
  
  RC set_value_from_str(Value &val, const string &data) const override;

  RC to_string(const Value &val, string &result) const override;
};