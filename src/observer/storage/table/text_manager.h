//用于管理text类型的数据，最大长度为4096字节
#pragma once
#define OBJ_MAX_LENGTH 4096

#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <cstdint>

#include "common/sys/rc.h"

class TextManager
{
public:
  TextManager()  = default;
  ~TextManager() = default;

  void set_path(std::string path);
  RC   open();
  RC   drop();
  RC   flush();
  RC   add_obj(std::string obj, uint32_t &index);
  RC   find_obj(std::string &obj, uint32_t index) const;
  RC   update_obj(const std::string &new_obj, uint32_t index);

private:
  std::string              path_;
  std::vector<std::string> values_;
};