/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/type/char_type.h"
#include "common/type/float_type.h"
#include "common/type/integer_type.h"
#include "common/type/data_type.h"
#include "common/type/vector_type.h"
#include "common/type/date_type.h"

// Todo: 实现新数据类型
// your code here

array<unique_ptr<DataType>, static_cast<int>(AttrType::MAXTYPE)> DataType::type_instances_ = {
    make_unique<DataType>(AttrType::UNDEFINED),  // 0: UNDEFINED
    make_unique<CharType>(),                     // 1: CHARS
    make_unique<CharType>(),                     // 2: TEXTS (使用 CharType 处理，因为都是字符串类型)
    make_unique<IntegerType>(),                  // 3: INTS
    make_unique<DateType>(),                     // 4: DATES
    make_unique<FloatType>(),                    // 5: FLOATS
    make_unique<VectorType>(),                   // 6: VECTORS
    make_unique<DataType>(AttrType::BOOLEANS),   // 7: BOOLEANS
};