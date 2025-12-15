#include "sql/stmt/update_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "common/type/date_type.h"

UpdateStmt::~UpdateStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}

RC UpdateStmt::create(Db *db_instance, UpdateSqlNode &update_node, Stmt *&stmt_instance)
{
  // Validate the arguments
  const char *target_table_name = update_node.relation_name.c_str();
  const char *target_field_name = update_node.attribute_name.c_str();

  if (nullptr == db_instance || nullptr == target_table_name || nullptr == target_field_name ||
      0 == update_node.value.length()) {
    LOG_WARN("Invalid argument. db=%p, table_name=%p, field_name=%p, value_length=%d",
        db_instance, target_table_name, target_field_name, update_node.value.length());
    return RC::INVALID_ARGUMENT;
  }

  // Check if the table exists
  Table *target_table = db_instance->find_table(target_table_name);
  if (nullptr == target_table) {
    LOG_WARN("No such table. db=%s, table_name=%s", db_instance->name(), target_table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  // Check if the field exists
  const TableMeta &meta_data    = target_table->table_meta();
  const FieldMeta *target_field = meta_data.field(target_field_name);
  if (nullptr == target_field) {
    LOG_WARN("No such field. db=%s, field_name=%s", db_instance->name(), target_field_name);
    return RC::SCHEMA_FIELD_NOT_EXIST;
  }

  // Check if the type matches
  Value processed_value;
  if (target_field->type() != update_node.value.attr_type()) {
    if (update_node.value.is_null()) {     // Insert a null value
      if (not target_field->nullable()) {  // The column doesn't allow null values
        return RC::NULL_CANT_INSERT;
      }
      processed_value = update_node.value;
      processed_value.set_type(target_field->type());  // Set the type of the null value
      processed_value.set_null_value();
    } else {
      RC rc = Value::cast_to(update_node.value, target_field->type(), processed_value);
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to cast value. table name:%s, field name:%s, value:%s ",
          target_table_name, target_field->name(), update_node.value.to_string().c_str());
        return rc;
      }
    }
    update_node.value = processed_value;
  }

  // Handle the condition
  std::unordered_map<std::string, Table *> table_map;
  table_map.insert(std::pair<std::string, Table *>(std::string(target_table_name), target_table));

  FilterStmt *filter_stmt_instance = nullptr;
  RC          rc                   = FilterStmt::create(db_instance,
      target_table,
      &table_map,
      update_node.conditions.data(),
      static_cast<int>(update_node.conditions.size()),
      filter_stmt_instance);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }

  // Create the update statement with the relevant table, field, value, and conditions
  stmt_instance =
      new UpdateStmt(target_table, (Value *)&update_node.value, 1, filter_stmt_instance, (FieldMeta *)target_field);
  return RC::SUCCESS;
}