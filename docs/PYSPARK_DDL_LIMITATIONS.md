# PySpark DDL 限制與替代方案

## 問題說明

PySpark 預設配置不支援標準 SQL DDL（Data Definition Language）語句，因為這些功能需要 Hive Metastore 支援。

### 不支援的 DDL 語句

```python
# ❌ 這些語句會產生錯誤
spark.sql("CREATE DATABASE my_database")
spark.sql("CREATE TABLE my_table (id INT, name STRING)")
spark.sql("DROP TABLE my_table")
spark.sql("ALTER TABLE my_table ADD COLUMN age INT")
```

### 錯誤訊息

```
AnalysisException: [NOT_SUPPORTED_COMMAND_WITHOUT_HIVE_SUPPORT] 
CREATE Hive TABLE (AS SELECT) is not supported, 
if you want to enable it, please set "spark.sql.catalogImplementation" to "hive".
```

## 解決方案

### 方案 1：使用臨時視圖（推薦用於學習和測試）

#### CREATE TABLE 的替代方案

```python
# ✅ 使用 DataFrame + createOrReplaceTempView
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# 定義結構
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

# 建立 DataFrame
df = spark.createDataFrame([], schema)

# 建立臨時視圖（類似 CREATE TABLE）
df.createOrReplaceTempView("my_table")

# 現在可以使用 SQL 查詢
spark.sql("SELECT * FROM my_table").show()
```

#### DROP TABLE 的替代方案

```python
# ✅ 使用 dropTempView
spark.catalog.dropTempView("my_table")

# 或使用 dropGlobalTempView（如果是全域臨時視圖）
spark.catalog.dropGlobalTempView("my_table")
```

#### CREATE DATABASE 的替代方案

```python
# ✅ 使用命名規則來組織資料
# 不需要建立資料庫，直接使用有意義的視圖名稱

# 模擬資料庫分組
sales_df.createOrReplaceTempView("sales_db_orders")
sales_df.createOrReplaceTempView("sales_db_customers")
hr_df.createOrReplaceTempView("hr_db_employees")
```

### 方案 2：啟用 Hive 支援（生產環境）

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.warehouse.dir", "/path/to/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# 現在可以使用 DDL 語句
spark.sql("CREATE DATABASE IF NOT EXISTS my_db")
spark.sql("CREATE TABLE my_db.my_table (id INT, name STRING)")
```

**注意**：需要安裝 Hive 並正確配置。

### 方案 3：使用 Delta Lake（推薦用於生產環境）

```python
# 安裝：pip install delta-spark

from delta import *

spark = SparkSession.builder \
    .appName("DeltaApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 建立 Delta 表格
df.write.format("delta").save("/path/to/delta/table")

# 讀取 Delta 表格
delta_df = spark.read.format("delta").load("/path/to/delta/table")
```

### 方案 4：儲存為檔案格式

```python
# ✅ 儲存為 Parquet
df.write.mode("overwrite").parquet("/path/to/data/my_table")

# 讀取 Parquet
my_table_df = spark.read.parquet("/path/to/data/my_table")
my_table_df.createOrReplaceTempView("my_table")

# 現在可以使用 SQL
spark.sql("SELECT * FROM my_table")
```

## 約束條件的處理

### PRIMARY KEY 的替代方案

```python
# ✅ 透過程式邏輯驗證唯一性
def validate_primary_key(df, key_column):
    total = df.count()
    unique = df.select(key_column).distinct().count()
    
    if total != unique:
        raise ValueError(f"主鍵違反唯一性約束：{key_column}")
    
    return df

# 使用
validated_df = validate_primary_key(my_df, "id")
```

### FOREIGN KEY 的替代方案

```python
# ✅ 透過 JOIN 驗證參照完整性
def validate_foreign_key(child_df, parent_df, fk_col, pk_col):
    invalid = child_df.join(
        parent_df,
        child_df[fk_col] == parent_df[pk_col],
        "left_anti"  # 找出在 parent 中不存在的記錄
    )
    
    invalid_count = invalid.count()
    if invalid_count > 0:
        print(f"警告：發現 {invalid_count} 筆違反外鍵約束的記錄")
        invalid.show()
        return False
    
    return True

# 使用
validate_foreign_key(orders_df, customers_df, "customer_id", "id")
```

### NOT NULL 的替代方案

```python
# ✅ 透過 filter 移除 null 值
df_no_null = df.filter(df["column_name"].isNotNull())

# 或在 schema 中設定 nullable=False
schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=False)
])
```

### CHECK 約束的替代方案

```python
# ✅ 透過 filter 驗證條件
def validate_check_constraint(df, condition, error_msg):
    invalid = df.filter(~condition)
    invalid_count = invalid.count()
    
    if invalid_count > 0:
        print(f"警告：{error_msg}")
        print(f"發現 {invalid_count} 筆違反約束的記錄")
        invalid.show()
        return False
    
    return True

# 使用：驗證價格必須 > 0
from pyspark.sql.functions import col
validate_check_constraint(
    df,
    col("price") > 0,
    "價格必須大於 0"
)
```

## 完整範例：模擬傳統資料庫操作

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col

# 建立 Spark Session
spark = SparkSession.builder.appName("DDL_Alternative").getOrCreate()

# 1. 建立「資料表」（使用 DataFrame + 臨時視圖）
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("price", IntegerType(), True)
])

data = [
    (1, "商品A", 100),
    (2, "商品B", 200),
    (3, "商品C", 300)
]

products_df = spark.createDataFrame(data, schema)
products_df.createOrReplaceTempView("products")

# 2. 驗證主鍵
total = products_df.count()
unique = products_df.select("id").distinct().count()
assert total == unique, "主鍵唯一性驗證失敗"

# 3. 驗證 CHECK 約束（價格 > 0）
invalid_price = products_df.filter(col("price") <= 0).count()
assert invalid_price == 0, "價格約束驗證失敗"

# 4. 查詢資料
result = spark.sql("SELECT * FROM products WHERE price > 150")
result.show()

# 5. 「刪除資料表」
spark.catalog.dropTempView("products")

print("✅ 所有操作完成！")
```

## 最佳實踐建議

### 學習與測試環境
1. 使用臨時視圖（`createOrReplaceTempView`）
2. 使用 DataFrame API 進行資料操作
3. 透過程式邏輯實現約束驗證

### 開發環境
1. 考慮使用 Delta Lake 提供 ACID 事務支援
2. 使用 Parquet 格式儲存資料
3. 建立資料驗證函數庫

### 生產環境
1. 啟用 Hive 支援（如果需要完整的 DDL 功能）
2. 使用 Delta Lake 或 Iceberg 等資料湖格式
3. 實作完整的資料品質檢查流程
4. 建立自動化測試確保資料完整性

## 常見問題

### Q: 為什麼 PySpark 不預設支援 CREATE TABLE？
A: PySpark 設計為分散式計算引擎，著重於資料處理而非資料庫管理。DDL 功能需要 Metastore（如 Hive）來管理表格元數據。

### Q: 臨時視圖會持久化嗎？
A: 不會。臨時視圖只存在於當前 Spark Session 中，Session 結束後就會消失。

### Q: 如何持久化資料？
A: 使用 `df.write` 將資料儲存為檔案格式（Parquet、Delta、CSV 等）或寫入外部資料庫。

### Q: Global Temp View 和 Temp View 的差異？
A: 
- Temp View：僅在當前 Session 可見
- Global Temp View：在同一個 Spark Application 的所有 Session 間共享

```python
# 建立 Global Temp View
df.createGlobalTempView("my_global_view")

# 查詢（需要加上 global_temp 前綴）
spark.sql("SELECT * FROM global_temp.my_global_view")
```

## 相關資源

- [PySpark SQL 官方文件](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Delta Lake 文件](https://docs.delta.io/)
- [Apache Hive 整合](https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html)
