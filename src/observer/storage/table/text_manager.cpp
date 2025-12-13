#include "text_manager.h"


//设置存储字符串的文件路径
void TextManager::set_path(std::string path) { path_.assign(path); }

//打开存储字符串的文件读取到缓存
RC TextManager::open()
{
  std::ifstream file(path_, std::ios::binary);
  if (not file.is_open()) {
    return RC::FILE_CLOSE;
  }
  // printf("lom open.\n");
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

//删除存储字符串的文件
RC TextManager::drop()
{
  values_.clear();
  std::remove(path_.c_str());
  return RC::SUCCESS;
}

//将缓存中的数据写入到文件
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

//将字符串插入到缓存的指定位置
RC TextManager::add_obj(std::string obj, uint32_t &index)
{
  std::string s;
  s.assign(obj);
  if (s.size() > OBJ_MAX_LENGTH) {
    s = s.substr(0, OBJ_MAX_LENGTH);  // 只保留前 OBJ_MAX_LENGTH 字节
  }
  values_.push_back(s);
  index = values_.size() - 1;
  return RC::SUCCESS;
}

//从缓存中获取指定位置的字符串
RC TextManager::find_obj(std::string &obj, uint32_t index) const
{
  if (index >= values_.size()) {
    return RC::NOT_EXIST;
  }
  obj.assign(values_[index]);
  return RC::SUCCESS;
}

//更新缓存中指定位置的字符串
RC TextManager::update_obj(const std::string &new_obj, uint32_t index)
{
  if (index >= values_.size()) {
    return RC::NOT_EXIST;
  }
  std::string s;
  s.assign(new_obj);
  if (s.size() > OBJ_MAX_LENGTH) {
    s = s.substr(0, OBJ_MAX_LENGTH);  // 只保留前 OBJ_MAX_LENGTH 字节
  }
  values_[index].assign(s);
  return RC::SUCCESS;
}