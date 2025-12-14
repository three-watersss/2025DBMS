#include "text_manager.h"

void TextManager::set_path(std::string path) { path_.assign(path); }

RC TextManager::open()
{
  std::ifstream file(path_, std::ios::binary);
  if (not file.is_open()) {
    return RC::FILE_CLOSE;
  }
  values_.clear();
  // 读取字符串
  while (file.peek() != EOF) {
    // 读取字符串长度
    uint32_t length = 0;
    file.read(reinterpret_cast<char *>(&length), sizeof(length));
    // 读取字符串数据
    std::string str(length, '\0');  // 预分配字符串长度
    file.read(&str[0], length);
    values_.push_back(std::move(str));
  }
  file.close();
  return RC::SUCCESS;
}

RC TextManager::drop()
{
  values_.clear();
  std::remove(path_.c_str());
  return RC::SUCCESS;
}

RC TextManager::flush()
{
  std::ofstream file(path_, std::ios::binary | std::ios::trunc);
  if (not file.is_open()) {
    return RC::FILE_CLOSE;
  }
  // 写入字符串
  for (const auto &str : values_) {
    // 写入字符串长度
    uint32_t length = static_cast<uint32_t>(str.size());
    file.write(reinterpret_cast<const char *>(&length), sizeof(length));
    // 写入字符串数据
    file.write(str.data(), length);
  }
  file.close();
  return RC::SUCCESS;
}

RC TextManager::add_text(std::string text, uint32_t &index)
{
  std::string s;
  s.assign(text);
  if (s.size() > TEXT_MAX_LENGTH) {
    s = s.substr(0, TEXT_MAX_LENGTH);  // 只保留前 TEXT_MAX_LENGTH 字节
  }
  values_.push_back(s);
  index = values_.size() - 1;
  return RC::SUCCESS;
}

RC TextManager::find_text(std::string &text, uint32_t index) const
{
  if (index >= values_.size()) {
    return RC::NOT_EXIST;
  }
  text.assign(values_[index]);
  return RC::SUCCESS;
}

RC TextManager::update_text(const std::string &new_text, uint32_t index)
{
  if (index >= values_.size()) {
    return RC::NOT_EXIST;
  }
  std::string s;
  s.assign(new_text);
  if (s.size() > TEXT_MAX_LENGTH) {
    s = s.substr(0, TEXT_MAX_LENGTH);  // 只保留前 TEXT_MAX_LENGTH 字节
  }
  values_[index].assign(s);
  return RC::SUCCESS;
}