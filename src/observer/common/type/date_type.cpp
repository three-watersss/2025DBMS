#include <iomanip>

#include "common/lang/comparator.h"
#include "common/lang/sstream.h"
#include "common/log/log.h"
#include "common/type/date_type.h"
#include "common/value.h"
#include "common/lang/limits.h"
#include "common/types.h"

namespace {

constexpr int MIN_YEAR = 1970;
constexpr int MAX_YEAR = 2038;

constexpr int DAYS_IN_MONTH[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

//判断是否为闰年
bool is_leap_year(int year)
{
  return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
}

//返回从1970年1月1日到year年1月1日之间的天数
int days_from_epoch_to_year(int year)
{
  int days = 0;
  for (int y = MIN_YEAR; y < year; ++y) {
    days += is_leap_year(y) ? 366 : 365;
  }
  return days;
}

//返回year年month月day日是该年的第几天
int day_of_year(int year, int month, int day)
{
  int days_in_month[12];
  memcpy(days_in_month, DAYS_IN_MONTH, sizeof(DAYS_IN_MONTH));
  if (is_leap_year(year)) {
    days_in_month[1] = 29;
  }

  int days = 0;
  for (int m = 1; m < month; ++m) {
    days += days_in_month[m - 1];
  }
  days += day;
  return days;
}

//解析日期字符串为年月日
bool parse_ymd(const string &date, int &year, int &month, int &day)
{
  char dash1 = 0;
  char dash2 = 0;
  istringstream ss(date);
  if (!(ss >> year >> dash1 >> month >> dash2 >> day) || dash1 != '-' || dash2 != '-') {
    return false;
  }
  return true;
}

//验证年月日是否合法
bool validate_ymd(int year, int month, int day)
{
  if (year < MIN_YEAR || year > MAX_YEAR) {
    return false;
  }
  if (month < 1 || month > 12) {
    return false;
  }
  int days_in_month[12];
  memcpy(days_in_month, DAYS_IN_MONTH, sizeof(DAYS_IN_MONTH));
  if (month == 2 && is_leap_year(year)) {
    days_in_month[1] = 29;
  }
  if (day < 1 || day > days_in_month[month - 1]) {
    return false;
  }
  return true;
}

}  // namespace

bool DateType::is_valid(const string date)
{
  if (date.length() < 8 || date.length() > 10) {  // 粗略长度检查
    return false;
  }

  int year = 0, month = 0, day = 0;
  if (!parse_ymd(date, year, month, day)) {
    return false;
  }
  return validate_ymd(year, month, day);
}

int DateType::to_int(const string date)
{
  int year = 0, month = 0, day = 0;
  char dash1 = 0, dash2 = 0;
  istringstream ss(date);
  ss >> year >> dash1 >> month >> dash2 >> day;
  // 行为保持一致：假定传入已校验好的日期
  return days_from_epoch_to_year(year) + day_of_year(year, month, day) - 1;
}

const string DateType::to_str(int date)
{
  int year = MIN_YEAR;
  int remaining = date;

  while (true) {
    int days_in_year = is_leap_year(year) ? 366 : 365;
    if (remaining < days_in_year) {
      break;
    }
    remaining -= days_in_year;
    year++;
  }

  int days_in_month[12];
  memcpy(days_in_month, DAYS_IN_MONTH, sizeof(DAYS_IN_MONTH));
  if (is_leap_year(year)) {
    days_in_month[1] = 29;
  }

  int month = 1;
  while (remaining >= days_in_month[month - 1]) {
    remaining -= days_in_month[month - 1];
    month++;
  }
  int day = remaining + 1;

  stringstream ss;
  ss << std::setw(4) << std::setfill('0') << year << "-";
  ss << std::setw(2) << std::setfill('0') << month << "-";
  ss << std::setw(2) << std::setfill('0') << day;
  return ss.str();
}

int DateType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::DATES && right.attr_type() == AttrType::DATES,
         "left and right type is not both date");
  int left_val  = left.get_int();
  int right_val = right.get_int();
  return common::compare_int(&left_val, &right_val);
}

RC DateType::set_value_from_str(Value &val, const string &data) const
{
  RC           rc = RC::SUCCESS;
  stringstream deserialize_stream;
  deserialize_stream.clear();
  deserialize_stream.str(data);

  int int_value = 0;
  deserialize_stream >> int_value;
  if (!deserialize_stream || !deserialize_stream.eof()) {
    rc = RC::SCHEMA_FIELD_TYPE_MISMATCH;
  } else {
    val.set_date(int_value);  // 保持原有语义：直接使用整数形式
  }
  return rc;
}

RC DateType::to_string(const Value &val, string &result) const
{
  if (val.is_null()) {
    result = "NULL";
    return RC::SUCCESS;
  }

  result = to_str(val.get_int());
  return RC::SUCCESS;
}