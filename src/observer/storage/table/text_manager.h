#pragma once
#define TEXT_MAX_LENGTH 4096

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
  RC   add_text(std::string text, uint32_t &index);
  RC   find_text(std::string &text, uint32_t index) const;
  RC   update_text(const std::string &new_text, uint32_t index);

private:
  std::string              path_;
  std::vector<std::string> values_;
};