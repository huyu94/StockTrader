# pandas.to_numeric 使用指南

## 一、什么是 pd.to_numeric

`pd.to_numeric` 是 pandas 提供的一个函数，用于将参数转换为数值类型（int 或 float）。它是处理数据类型转换的重要工具，特别适用于将字符串、Decimal、或其他非数值类型转换为数值类型。

## 二、为什么需要 pd.to_numeric

### 2.1 常见问题场景

在数据处理过程中，经常会遇到以下情况：

1. **从数据库读取的数据**：
   - MySQL 的 `DECIMAL` 类型会被 SQLAlchemy 转换为 Python 的 `Decimal` 对象
   - `Decimal` 对象在 pandas DataFrame 中会被识别为 `object` 类型
   - 无法直接进行数学运算

2. **从 CSV/Excel 读取的数据**：
   - 数字可能被读取为字符串（如 "123.45"）
   - 包含特殊字符（如 "1,234.56" 或 "¥100"）

3. **API 返回的数据**：
   - JSON 数据中的数字可能被解析为字符串
   - 需要转换为数值类型才能进行计算

### 2.2 错误示例

```python
import pandas as pd
from decimal import Decimal

# 示例：从数据库读取的数据包含 Decimal 对象
df = pd.DataFrame({
    'price': [Decimal('10.50'), Decimal('20.30'), Decimal('30.40')]
})

print(df['price'].dtype)  # 输出: object

# 尝试进行数学运算会报错
try:
    result = df['price'] * 2  # 可能报错：Expected numeric dtype, got object instead
except Exception as e:
    print(f"错误: {e}")
```

## 三、pd.to_numeric 的基本用法

### 3.1 函数签名

```python
pandas.to_numeric(arg, errors='raise', downcast=None)
```

### 3.2 参数说明

- **arg**：要转换的值，可以是标量、列表、数组或 Series
- **errors**：错误处理方式
  - `'raise'`（默认）：遇到无法转换的值时抛出异常
  - `'coerce'`：无法转换的值转换为 `NaN`
  - `'ignore'`：遇到无法转换的值时返回原始值
- **downcast**：可选，将数值类型向下转换以节省内存
  - `'integer'`：转换为整数类型
  - `'signed'`：转换为有符号整数
  - `'unsigned'`：转换为无符号整数
  - `'float'`：转换为浮点数类型

### 3.3 基本示例

```python
import pandas as pd
from decimal import Decimal

# 示例1：转换字符串为数值
s = pd.Series(['1', '2.5', '3.14', '4'])
result = pd.to_numeric(s)
print(result)
# 输出: 0    1.00
#      1    2.50
#      2    3.14
#      3    4.00
#      dtype: float64

# 示例2：转换 Decimal 对象
s = pd.Series([Decimal('10.50'), Decimal('20.30')])
result = pd.to_numeric(s)
print(result)
# 输出: 0    10.50
#      1    20.30
#      dtype: float64

# 示例3：处理无法转换的值（使用 errors='coerce'）
s = pd.Series(['1', '2.5', 'abc', '4'])
result = pd.to_numeric(s, errors='coerce')
print(result)
# 输出: 0    1.0
#      1    2.5
#      2    NaN
#      3    4.0
#      dtype: float64
```

## 四、在本项目中的应用场景

### 4.1 场景1：从数据库读取价格数据

**问题**：MySQL 的 `DECIMAL(10, 2)` 类型被 SQLAlchemy 转换为 Python `Decimal` 对象，在 DataFrame 中为 `object` 类型。

**解决方案**：在计算前将价格列转换为数值类型

```python
# 在 qfq_calculator.py 中的应用
price_columns = ['close', 'open', 'high', 'low']
for col in price_columns:
    if col in result_df.columns:
        result_df[col] = pd.to_numeric(result_df[col], errors='coerce')
```

**说明**：
- 使用 `errors='coerce'` 确保无法转换的值变为 `NaN`，不会中断计算
- 转换后可以正常进行数学运算（如乘法、除法等）

### 4.2 场景2：处理 API 返回的字符串数字

**问题**：某些 API 可能返回字符串格式的数字，需要转换为数值类型。

**解决方案**：

```python
# 示例：处理 API 返回的数据
df = pd.DataFrame({
    'price': ['10.50', '20.30', '30.40']
})

# 转换为数值类型
df['price'] = pd.to_numeric(df['price'], errors='coerce')
```

### 4.3 场景3：清理包含特殊字符的数据

**问题**：数据中可能包含逗号、货币符号等特殊字符。

**解决方案**：

```python
# 示例：清理包含逗号的数字字符串
s = pd.Series(['1,234.56', '2,345.67', '3,456.78'])

# 先移除逗号，再转换
s_cleaned = s.str.replace(',', '')
result = pd.to_numeric(s_cleaned, errors='coerce')
```

## 五、最佳实践

### 5.1 推荐做法

1. **使用 `errors='coerce'`**：
   - 在不确定数据质量的情况下，使用 `errors='coerce'` 可以避免程序崩溃
   - 转换后的 `NaN` 值可以通过 `isna()` 或 `notna()` 进行过滤

2. **在计算前转换**：
   - 在进行数学运算前，确保数据类型正确
   - 避免在循环中进行转换，应该在批量处理前统一转换

3. **检查转换结果**：
   - 转换后检查是否有 `NaN` 值
   - 记录转换失败的记录数量

### 5.2 示例代码

```python
import pandas as pd
from loguru import logger

def convert_to_numeric_safe(df, columns):
    """
    安全地将指定列转换为数值类型
    
    Args:
        df: DataFrame
        columns: 要转换的列名列表
    
    Returns:
        转换后的 DataFrame
    """
    df = df.copy()
    
    for col in columns:
        if col in df.columns:
            # 记录转换前的非空数量
            before_count = df[col].notna().sum()
            
            # 转换为数值类型
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 记录转换后的非空数量
            after_count = df[col].notna().sum()
            
            # 如果有数据丢失，记录警告
            if after_count < before_count:
                lost_count = before_count - after_count
                logger.warning(
                    f"列 {col} 转换时丢失 {lost_count} 条数据 "
                    f"({before_count} -> {after_count})"
                )
    
    return df
```

## 六、常见错误和解决方案

### 6.1 错误：Expected numeric dtype, got object instead

**原因**：尝试对 `object` 类型的列进行数学运算。

**解决方案**：使用 `pd.to_numeric` 先转换数据类型。

```python
# 错误示例
df['result'] = df['price'] * 2  # 如果 price 是 object 类型会报错

# 正确做法
df['price'] = pd.to_numeric(df['price'], errors='coerce')
df['result'] = df['price'] * 2
```

### 6.2 错误：无法转换的值导致程序崩溃

**原因**：使用默认的 `errors='raise'`，遇到无法转换的值时抛出异常。

**解决方案**：使用 `errors='coerce'` 将无法转换的值转换为 `NaN`。

```python
# 错误示例
result = pd.to_numeric(['1', '2', 'abc'])  # 会抛出 ValueError

# 正确做法
result = pd.to_numeric(['1', '2', 'abc'], errors='coerce')  # 'abc' 变为 NaN
```

## 七、性能考虑

### 7.1 批量转换 vs 逐个转换

```python
# 推荐：批量转换整个 Series
df['price'] = pd.to_numeric(df['price'], errors='coerce')

# 不推荐：在循环中逐个转换
for idx in df.index:
    df.at[idx, 'price'] = pd.to_numeric(df.at[idx, 'price'], errors='coerce')
```

### 7.2 内存优化

如果需要节省内存，可以使用 `downcast` 参数：

```python
# 转换为整数类型（如果可能）
df['count'] = pd.to_numeric(df['count'], errors='coerce', downcast='integer')

# 转换为浮点数类型（如果可能）
df['price'] = pd.to_numeric(df['price'], errors='coerce', downcast='float')
```

## 八、总结

`pd.to_numeric` 是处理数据类型转换的重要工具，特别适用于：

1. **从数据库读取的数据**：将 `Decimal` 对象转换为数值类型
2. **字符串数字**：将字符串格式的数字转换为数值类型
3. **数据清洗**：处理包含特殊字符的数值数据
4. **类型统一**：确保数据在进行数学运算前是正确的数值类型

在本项目中，主要应用于前复权价格计算时，将从数据库读取的 `Decimal` 类型价格数据转换为数值类型，以便进行数学运算。

