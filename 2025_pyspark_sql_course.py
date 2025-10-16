# -*- coding: utf-8 -*-
# %% [md]
## PySpark SQL 零基礎入門教學 🎈
# 

# 這份教學專為完全沒有 SQL 經驗的你設計！
# SQL 就像是跟資料庫溝通的語言，學會了就能輕鬆管理各種資料。
# 我們會用生活化的例子，讓你快速上手！✨
# 
# 作者：QChoice AI 教學團隊
# 日期：2025-01-15

# 
# 🎉 歡迎學習 SQL！讓我們開始吧！
#
# --------------------------------------------------
#
# 🎯 環境設定 - 準備開發環境

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# 創建 Spark Session - 啟動資料處理引擎
spark = SparkSession.builder \
    .appName("SQL入門教學") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# %% [md]
### 📚 第一章：基礎查詢操作

#### 1️⃣ SELECT - 查詢資料（從資料表中取出你要的資料）

# 🎈 概念解釋：
# SELECT 是最基本的查詢指令，用來從資料表中取出資料。
# 就像在圖書館找書一樣，你可以選擇看全部的書，或只看特定類型的書。
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，從玩具資料表中選取所有玩具的名稱和顏色，
# 並顯示前5筆資料。

# %%
# 範例資料：建立一個玩具清單
toys_data = [
    (1, "小熊", "棕色", 299),
    (2, "機器人", "藍色", 599),
    (3, "芭比娃娃", "粉色", 399),
    (4, "樂高", "彩色", 899),
    (5, "小汽車", "紅色", 199)
]
toys_df = spark.createDataFrame(toys_data, ["id", "名稱", "顏色", "價格"])
toys_df.createOrReplaceTempView("toys")
# %% [md]
##### 📌 範例 1: SELECT - 選取玩具名稱和顏色")

# %%
print("📌 玩具資料表內容：")
toys_df.show()

# SQL 方式 1：使用 SQL 語法
result = spark.sql("SELECT `名稱`, `顏色` FROM toys")
print("📌 使用 SQL 語法的結果：")
result.show()

# PySpark DataFrame 方式（另一種寫法）
result2 = toys_df.select("名稱", "價格")
print("📌 使用 DataFrame API 的結果：")
result2.show()

# %% [md]
#### 2️⃣ INSERT - 新增資料（在資料表中加入新的記錄）

# 🎈 概念解釋：
# INSERT 用來在資料表中新增資料。
# 就像在筆記本上寫下新的一筆記錄，資料庫會永久保存這些資料。
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，將一個新玩具「泰迪熊」新增到玩具資料表中，
# 顏色是「白色」，價格是 450 元。

##### 📌 範例 2: INSERT - 新增新玩具
# %% 
print("📌 玩具資料表內容：")
toys_df.show()

# 新玩具資料
new_toy = [(6, "泰迪熊", "白色", 450)]
new_toy_df = spark.createDataFrame(new_toy, ["id", "名稱", "顏色", "價格"])
print("📌 新增的玩具資料：")
new_toy_df.show()

# 合併資料（模擬 INSERT）
union_toys_df = toys_df.union(new_toy_df)
union_toys_df.createOrReplaceTempView("toys")
print("📌 新玩具已新增到資料表中！")
union_toys_df.show()

spark.sql("SELECT * FROM toys").show()

# %% [md]
##### 3️⃣ UPDATE - 更新資料（修改已存在的資料內容）

# 🎈 概念解釋：
# UPDATE 用來修改資料表中已存在的資料。
# 就像修改文件內容一樣，把舊的資料改成新的資料。
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，將玩具「小熊」的價格從 299 元更新為 350 元。

# PySpark 沒有直接的 UPDATE，我們用 withColumn 和 when

##### 📌 範例 3: UPDATE - 更新小熊的價格

# %%
toys_df = toys_df.withColumn(
    "價格",
    when(col("名稱") == "小熊", 350).otherwise(col("價格"))
)
toys_df.createOrReplaceTempView("toys")
spark.sql("SELECT * FROM toys WHERE `名稱` = '小熊'").show()

# %% [md]
#### 4️⃣ DELETE - 刪除資料（移除不需要的記錄）

# 🎈 概念解釋：
# DELETE 用來從資料表中移除資料。
# 就像刪除檔案一樣，被刪除的資料就不會再出現了。
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，刪除價格低於 200 元的所有玩具。

##### 📌 範例 4: DELETE - 刪除便宜的玩具

# %%
# 使用 filter 保留價格 >= 200 的玩具
toys_df = toys_df.filter(col("價格") >= 200)
toys_df.createOrReplaceTempView("toys")
spark.sql("SELECT * FROM toys").show()

# %% [md]
### 📚 第二章：資料庫和資料表管理

#### 5️⃣ CREATE DATABASE - 建立資料庫（建立資料的容器）
# 🎈 概念解釋：
# CREATE DATABASE 用來建立一個新的資料庫。
# 資料庫就像一個資料夾，裡面可以存放很多個資料表。
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，建立一個名為「toy_warehouse」的資料庫。
# 
# ⚠️ 注意：PySpark 預設不支援 CREATE DATABASE（需要 Hive）
# 在 PySpark 中，我們使用臨時視圖（Temp Views）來組織資料

#### 📌 範例 5: CREATE DATABASE - 建立資料庫（概念說明）

# 在 PySpark 中，我們不需要建立資料庫
# 直接使用臨時視圖即可
# 在標準 SQL 中：
#   CREATE DATABASE toy_warehouse
# 
# 在 PySpark 中：
#   使用臨時視圖來組織資料，不需要明確建立資料庫
#   臨時視圖會自動在 'default' 命名空間中
#   
# 如果需要資料庫功能，可以：
#   1. 使用 Hive：設定 spark.sql.catalogImplementation=hive
#   2. 使用 Delta Lake：提供更強大的資料管理功能
#   3. 使用檔案系統：直接儲存為 Parquet/CSV 等格式

#### 6️⃣ CREATE TABLE - 建立資料表（定義資料的結構）

# 🎈 概念解釋：
# CREATE TABLE 用來建立一個新的資料表，並定義欄位名稱和資料型態。
# 就像建立一個 Excel 工作表，先定義好有哪些欄位。
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，建立一個「學生成績」資料表，
# 包含學生姓名、科目、分數三個欄位。
# 
# ⚠️ 注意：PySpark 預設不支援 CREATE TABLE DDL 語法（需要 Hive）
# 我們使用 DataFrame 來模擬建立資料表的效果

##### 📌 範例 6: CREATE TABLE - 建立資料表（使用 DataFrame

# %%
# 方法 1：使用 DataFrame 定義結構並建立臨時視圖
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
students_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("subject", StringType(), True),
    StructField("score", IntegerType(), True)
])

# 建立空的 DataFrame 作為「資料表」
students_df = spark.createDataFrame([], students_schema)
students_df.createOrReplaceTempView("students")

print("✅ 已建立 students 臨時視圖（類似資料表）")
print("資料表結構：")

students_df.printSchema()

# %% [md]
#### 7️⃣ DROP DATABASE - 刪除資料庫（整個倉庫都拆掉）

# 🎈 概念解釋：
# DROP DATABASE 會刪除整個資料庫及其所有內容
# 裡面的所有資料表都會被刪除，使用時需特別小心
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，刪除名為「old_warehouse」的資料庫。
# （注意：會刪除所有裡面的資料表）
# 
# ⚠️ 注意：PySpark 預設不支援 CREATE/DROP DATABASE（需要 Hive）
# 這裡僅作為概念說明

##### 📌 範例 7: DROP DATABASE - 刪除資料庫（概念說明

# 在 PySpark 中，我們使用臨時視圖而非資料庫
# 概念說明：

# 在標準 SQL 中：
#   DROP DATABASE IF EXISTS old_warehouse
# 
# 在 PySpark 中：
#   使用臨時視圖（Temp Views）來組織資料
#   可以透過命名規則來模擬資料庫分組，例如：
#   - db1_table1
#   - db1_table2
#   - db2_table1

#### 8️⃣ DROP TABLE - 刪除資料表（丟掉一個盒子）
# 🎈 概念解釋：
# DROP TABLE 會刪除整個資料表
# 資料庫仍然存在，只是移除了這個資料表
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，刪除「舊玩具」資料表。
# 
# ⚠️ 注意：在 PySpark 中，我們使用 dropTempView 來刪除臨時視圖

##### 📌 範例 8: DROP TABLE - 刪除資料表（使用 dropTempView

# %%
# 先建立一個測試用的臨時視圖
test_data = [(1, "舊玩具")]
test_df = spark.createDataFrame(test_data, ["id", "name"])
test_df.createOrReplaceTempView("old_toys")
print("✅ 已建立 old_toys 臨時視圖")

# 刪除臨時視圖
spark.catalog.dropTempView("old_toys")
print("✅ 已刪除 old_toys 臨時視圖")

# 驗證是否已刪除
try:
    spark.sql("SELECT * FROM old_toys").show()
except Exception as e:
    print(f"✅ 確認已刪除：{e}")

# %% [md]
#### 9️⃣ ALTER TABLE - 修改資料表（改造盒子）

# 🎈 概念解釋：
# ALTER TABLE 可以新增欄位、修改欄位名稱或改變欄位型態
# 常用於調整資料表結構
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，在玩具資料表中新增一個「製造商」欄位。

##### 📌 範例 9: ALTER TABLE - 修改資料表結構

# %%
# 新增欄位
toys_df = toys_df.withColumn("製造商", lit("快樂玩具公司"))
toys_df.createOrReplaceTempView("toys")

spark.sql("SELECT * FROM toys LIMIT 3").show()

# %% [md]
#### 🔟 TRUNCATE TABLE - 清空資料表（把盒子裡的東西全倒掉）

# 🎈 概念解釋：
# TRUNCATE TABLE 會清空資料表中的所有資料
# 但資料表結構保留，只是沒有任何記錄
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，清空「暫存資料」資料表的所有內容。

##### 📌 範例 10: TRUNCATE TABLE - 清空資料表

# %%
# 建立空的 DataFrame（模擬 TRUNCATE）
empty_df = spark.createDataFrame([], toys_df.schema)
print("✅ 資料表已清空（保留結構）")

empty_df.printSchema()

# %% [md]
### 📚 第三章：索引管理

#### 1️⃣1️⃣ CREATE INDEX - 建立索引（做一本目錄）
# 🎈 概念解釋：
# CREATE INDEX 建立索引可以加快查詢速度
# 想找東西的時候可以更快找到
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，在玩具資料表的「名稱」欄位上建立索引，
# 以加快查詢速度。（說明：PySpark 使用 cache 來優化）

##### 📌 範例 11: CREATE INDEX - 優化查詢速度

# %%
# PySpark 使用 cache() 來優化常用查詢
toys_df.cache()
print("✅ 資料已快取，查詢會更快！")

# %% [md]
#### 1️⃣2️⃣ DROP INDEX - 刪除索引（把目錄撕掉）

# 🎈 概念解釋：
# DROP INDEX 刪除不需要的索引
# 不會影響內容，只是找東西會慢一點
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，移除玩具資料表上的快取索引。

##### 📌 範例 12: DROP INDEX - 移除快取

# %%
toys_df.unpersist()
print("✅ 快取已移除")

# %% [md]
### 📚 第四章：資料表連接（JOIN）

# %%
# 準備範例資料：學生和成績
students_data = [
    (1, "小明", 10),
    (2, "小華", 10),
    (3, "小美", 11)
]

scores_data = [
    (1, "數學", 85),
    (2, "數學", 90),
    (3, "數學", 88),
    (4, "數學", 92)  # 這個學生不在學生表中
]

students_df = spark.createDataFrame(students_data, ["id", "姓名", "年齡"])

scores_df = spark.createDataFrame(scores_data, ["student_id", "科目", "分數"])

students_df.createOrReplaceTempView("students")
scores_df.createOrReplaceTempView("scores")

# %% [md]
#### 1️⃣3️⃣ INNER JOIN - 內部連接（只拿兩邊都有的）

# 🎈 概念解釋：
# INNER JOIN 只保留兩個資料表都有匹配的記錄
# 只有兩邊都有的才會被選出來
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，將學生資料表和成績資料表做 INNER JOIN，
# 只顯示有成績記錄的學生資訊。

##### 📌 範例 13: INNER JOIN - 兩邊都有的才留下

# %%
result = spark.sql("""
    SELECT s.`姓名`, sc.`科目`, sc.`分數`
    FROM students s
    INNER JOIN scores sc ON s.id = sc.student_id
""")
result.show()

# %% [md]
#### 1️⃣4️⃣ LEFT JOIN - 左連接（保留左邊全部）

# 🎈 概念解釋：
# LEFT JOIN 保留左表的所有記錄
# 左邊的全部保留，右邊有配對的就加上，沒有就留空
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，顯示所有學生，即使他們沒有成績記錄也要顯示。

##### 📌 範例 14: LEFT JOIN - 保留所有學生

# %%
result = spark.sql("""
    SELECT s.`姓名`, sc.`科目`, sc.`分數`
    FROM students s
    LEFT JOIN scores sc ON s.id = sc.student_id
""")
result.show()

# %% [md]
#### 1️⃣5️⃣ RIGHT JOIN - 右連接（保留右邊全部）

# 🎈 概念解釋：
# RIGHT JOIN 保留右表的所有記錄
# 右邊的全部保留，左邊有配對的就加上
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，顯示所有成績記錄，
# 即使某些成績找不到對應的學生也要顯示。

##### 📌 範例 15: RIGHT JOIN - 保留所有成績

# %%
result = spark.sql("""
    SELECT s.`姓名`, sc.`科目`, sc.`分數`
    FROM students s
    RIGHT JOIN scores sc ON s.id = sc.student_id
""")
result.show()

# %% [md]
#### 1️⃣6️⃣ FULL OUTER JOIN - 全外部連接（兩邊都保留）

# 🎈 概念解釋：
# FULL OUTER JOIN 保留兩個資料表的所有記錄
# 不管有沒有配對，全部都要！
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，顯示所有學生和所有成績記錄，
# 不管是否有配對都要顯示。

##### 📌 範例 16: FULL OUTER JOIN - 全部都要

# %%
result = spark.sql("""
    SELECT s.`姓名`, sc.`科目`, sc.`分數`
    FROM students s
    FULL OUTER JOIN scores sc ON s.id = sc.student_id
""")
result.show()

## 📚 第五章：集合操作
# 準備範例資料
class_a = [(1, "小明"), (2, "小華"), (3, "小美")]
class_b = [(3, "小美"), (4, "小強"), (5, "小芳")]
class_a_df = spark.createDataFrame(class_a, ["id", "姓名"])
class_b_df = spark.createDataFrame(class_b, ["id", "姓名"])

# %% [md]
#### 1️⃣7️⃣ UNION - 聯集（把兩堆玩具合在一起，重複的只留一個）

# 🎈 概念解釋：
# UNION 合併兩個查詢結果
# 如果有重複的人，只會記錄一次
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，合併 A 班和 B 班的學生名單，
# 重複的學生只顯示一次。
##### 📌 範例 17: UNION - 合併並去除重複

# %%
class_a_df.createOrReplaceTempView("class_a")
class_b_df.createOrReplaceTempView("class_b")

result = spark.sql("""
    SELECT * FROM class_a
    UNION
    SELECT * FROM class_b
""")
result.show()

# %% [md]
#### 1️⃣8️⃣ UNION ALL - 全部聯集（重複的也保留）

# 🎈 概念解釋：
# UNION ALL 合併所有查詢結果
# 就算有一樣的也不管，全部都要
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，合併兩個班級的點名記錄，
# 包含所有重複的記錄。
##### 📌 範例 18: UNION ALL - 全部合併（含重複）

# %%
result = spark.sql("""
    SELECT * FROM class_a
    UNION ALL
    SELECT * FROM class_b
""")
result.show()

# %% [md]
## 📚 第六章：資料篩選與排序

# 1️⃣9️⃣ DISTINCT - 去除重複（只留不一樣的）

# 🎈 概念解釋：
# DISTINCT 去除重複的記錄
# 重複的口味只拿一顆
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，找出玩具資料表中所有不同的顏色。
##### 📌 範例 19: DISTINCT - 找出所有不同的顏色

# %%
result = spark.sql("SELECT DISTINCT `顏色` FROM toys")
result.show()

# %% [md]
#### 2️⃣0️⃣ WHERE - 條件篩選（只要符合條件的）

# 🎈 概念解釋：
# WHERE 用來篩選符合條件的資料
# 設定條件，只有符合的才能被選出來
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，找出價格大於 400 元的所有玩具。

##### 📌 範例 20: WHERE - 篩選貴的玩具

# %%
result = spark.sql("SELECT * FROM toys WHERE `價格` > 400")
result.show()

# %% [md]
#### 2️⃣1️⃣ ORDER BY - 排序（把東西排排站）

# 🎈 概念解釋：
# ORDER BY 用來排序查詢結果
# 讓資料有順序
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，將所有玩具按照價格從高到低排序。
##### 📌 範例 21: ORDER BY - 價格排序

# %%
result = spark.sql("SELECT * FROM toys ORDER BY `價格` DESC")
result.show()

# %% [md]
#### 2️⃣2️⃣ GROUP BY - 分組（把同類的放一起）

# 🎈 概念解釋：
# GROUP BY 將資料依指定欄位分組
# 紅色的放一堆，藍色的放一堆
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，計算每種顏色有幾個玩具。
##### 📌 範例 22: GROUP BY - 依顏色分組計數

# %%
result = spark.sql("""
    SELECT `顏色`, COUNT(*) as `數量`
    FROM toys
    GROUP BY `顏色`
""")
result.show()

# %% [md]
#### 2️⃣3️⃣ HAVING - 分組後篩選（分好組後再挑）

# 🎈 概念解釋：
# HAVING 用來篩選分組後的結果
# 先分組，再選出符合條件的組
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，找出玩具數量大於 1 個的顏色。
##### 📌 範例 23: HAVING - 篩選玩具數量多的顏色

# %%
result = spark.sql("""
    SELECT `顏色`, COUNT(*) as `數量`
    FROM toys
    GROUP BY `顏色`
    HAVING COUNT(*) > 1
""")
result.show()

# %% [md]
## 📚 第七章：聚合函數（統計運算）

#### 2️⃣4️⃣ COUNT - 計數（數數看有幾個）

# 🎈 概念解釋：
# COUNT 計算記錄的數量
# 告訴你總共有幾個
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，計算玩具資料表中總共有幾個玩具。
##### 📌 範例 24: COUNT - 數玩具總數

# %%
result = spark.sql("SELECT COUNT(*) as `玩具總數` FROM toys")
result.show()

# %% [md]
#### 2️⃣5️⃣ SUM - 總和（對指定欄位進行加總）

# 🎈 概念解釋：
# SUM 計算數值欄位的總和
# 算算看總共要花多少錢
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，計算所有玩具的總價值。
##### 📌 範例 25: SUM - 計算總價值

# %%
result = spark.sql("SELECT SUM(`價格`) as `總價值` FROM toys")
result.show()

# %% [md]
#### 2️⃣6️⃣ AVG - 平均（算平均值）

# 🎈 概念解釋：
# AVG 計算數值欄位的平均值
# 所有價格加起來除以數量
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，計算玩具的平均價格。
##### 📌 範例 26: AVG - 計算平均價格

# %%
result = spark.sql("SELECT AVG(`價格`) as `平均價格` FROM toys")
result.show()

# %% [md]
#### 2️⃣7️⃣ MIN - 最小值（找最小的）

# 🎈 概念解釋：
# MIN 找出最小值
# 看看哪個價格最低
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，找出價格最低的玩具價格。
##### 📌 範例 27: MIN - 找最低價格
# %%

result = spark.sql("SELECT MIN(`價格`) as `最低價格` FROM toys")
result.show()

# %% [md]
#### 2️⃣8️⃣ MAX - 最大值（找最大的）

# 🎈 概念解釋：
# MAX 找出最大值
# 看看哪個價格最高
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，找出價格最高的玩具及其價格。
##### 📌 範例 28: MAX - 找最高價格

# %%
result = spark.sql("SELECT MAX(`價格`) as `最高價格` FROM toys")
result.show()

# %% [md]
## 📚 第八章：條件與範圍查詢

#### 2️⃣9️⃣ BETWEEN - 範圍查詢（在某個範圍內）

# 🎈 概念解釋：
# BETWEEN 用來查詢某個範圍內的資料
# 找出在某個範圍內的東西
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，找出價格在 300 到 500 元之間的玩具。
##### 📌 範例 29: BETWEEN - 找特定價格範圍的玩具

# %%
result = spark.sql("SELECT * FROM toys WHERE `價格` BETWEEN 300 AND 500")
result.show()

# %% [md]
#### 3️⃣0️⃣ LIKE - 模糊搜尋（名字像什麼的）

# 🎈 概念解釋：
# LIKE 用來進行模糊搜尋
# 不用完全一樣，有包含就可以
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，找出名稱中包含「熊」字的所有玩具。
##### 📌 範例 30: LIKE - 模糊搜尋玩具名稱

# %%
result = spark.sql("SELECT * FROM toys WHERE `名稱` LIKE '%熊%'")
result.show()

# %% [md]
#### 3️⃣1️⃣ IN - 清單比對（在這些裡面的）

# 🎈 概念解釋：
# IN 檢查值是否在指定清單中
# 只要在清單裡的都可以
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，找出顏色是「紅色」、「藍色」或「粉色」的玩具。
##### 📌 範例 31: IN - 找特定顏色的玩具

# %%
result = spark.sql("SELECT * FROM toys WHERE `顏色` IN ('紅色', '藍色', '粉色')")
result.show()

# %% [md]
#### 3️⃣2️⃣ NOT - 否定（不要這個）

# 🎈 概念解釋：
# NOT 用來反轉條件
# 把條件相反
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，找出顏色不是「棕色」的所有玩具。
##### 📌 範例 32: NOT - 排除特定條件

# %%
result = spark.sql("SELECT * FROM toys WHERE NOT `顏色` = '棕色'")
result.show()

# %% [md]
#### 3️⃣3️⃣ IS NULL - 檢查空值（有沒有遺失）

# 🎈 概念解釋：
# IS NULL 檢查欄位是否為空值
# 找出資料遺失的地方
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，找出沒有填寫顏色的玩具。
##### 📌 範例 33: IS NULL - 找遺失資料

# %%
# 先加一個沒有顏色的玩具
toys_with_null = toys_df.union(
    spark.createDataFrame([(7, "神秘玩具", None, 999, "快樂玩具公司")], 
                         toys_df.schema)
)
toys_with_null.createOrReplaceTempView("toys_with_null")

result = spark.sql("SELECT * FROM toys_with_null WHERE `顏色` IS NULL")
result.show()

# %% [md]
#### 3️⃣4️⃣ IS NOT NULL - 檢查非空值（有資料的）

# 🎈 概念解釋：
# IS NOT NULL 檢查欄位是否有值
# 找出有完整資訊的
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，找出所有有填寫顏色的玩具。
##### 📌 範例 34: IS NOT NULL - 找完整資料

# %%
result = spark.sql("SELECT * FROM toys_with_null WHERE `顏色` IS NOT NULL")
result.show()

# %% [md]
## 📚 第九章：進階條件邏輯

#### 3️⃣5️⃣ CASE - 條件判斷（如果...那麼...）

# 🎈 概念解釋：
# CASE 根據不同條件回傳不同的值
# 根據不同情況給不同答案
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，依據價格將玩具分為「昂貴」（>500）、
# 「中等」（300-500）、「便宜」（<300）三個等級。
##### 📌 範例 35: CASE - 價格等級分類
# %%
spark.sql("SELECT * FROM toys").show()

result = spark.sql("""
    SELECT `名稱`, `價格`,
        CASE 
            WHEN `價格` > 500 THEN '昂貴'
            WHEN `價格` >= 300 THEN '中等'
            ELSE '便宜'
        END AS `價格等級`
    FROM toys
""")
result.show()

# %% [md]
#### 3️⃣6️⃣ COALESCE - 取第一個非空值（找替代方案）

# 🎈 概念解釋：
# COALESCE 回傳第一個非 NULL 的值
# 找第一個不是空的值
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，如果玩具沒有顏色資訊，就顯示「未知」。
##### 📌 範例 36: COALESCE - 處理空值

# %%
result = spark.sql("""
    SELECT `名稱`, COALESCE(`顏色`, '未知') as `顏色`
    FROM toys_with_null
""")
result.show()

# %% [md]
## 📚 第十章：子查詢

#### 3️⃣7️⃣ EXISTS - 存在檢查（有沒有這個東西）

# 🎈 概念解釋：
# EXISTS 檢查子查詢是否有結果
# 檢查是否存在符合條件的資料
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，找出有昂貴玩具（>500元）的製造商。
##### 📌 範例 37: EXISTS - 檢查是否存在

# %%
result = spark.sql("""
    SELECT DISTINCT `製造商`
    FROM toys t1
    WHERE EXISTS (
        SELECT 1 FROM toys t2 
        WHERE t2.`製造商` = t1.`製造商` AND t2.`價格` > 500
    )
""")
result.show()

# %% [md]
#### 3️⃣8️⃣ ANY/SOME - 任一比對（只要有一個符合）NOTE: Spark SQL 沒有支援 ANY/SOME

# 🎈 概念解釋：
# ANY/SOME 與子查詢的任一值比較
# 和一堆值比較，有一個符合就可以
# 註:
# 你可以用 MAX()（或 MIN()） 來達到相同邏輯。
# 例如你原本意思是：「找出價格 大於任一藍色玩具價格 的玩具」，
# 這其實等價於「價格大於藍色玩具中最低價格」。
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，找出價格比任一藍色玩具還貴的玩具。
##### 📌 範例 38: ANY/SOME - 任一比對

# %%
result = spark.sql("""
    SELECT `名稱`, `價格`
    FROM toys
    WHERE `價格` > (
        SELECT MIN(`價格`)
        FROM toys
        WHERE `顏色` = '藍色'
    )
""")
result.show()

# %% [md]
#### 3️⃣9️⃣ ALL - 全部比對（要全部都符合）NOTE: Spark SQL 沒有支援 ALL

# 🎈 概念解釋：
# ALL 與子查詢的所有值比較
# 和一堆值比較，全部都要符合
# 註:
# 用聚合函數 MAX() 取代 ALL 的語意：
# 
# 「大於藍色玩具所有價格」 ≡ 「大於藍色玩具的最高價」
# 
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，找出價格比所有藍色玩具都貴的玩具。
##### 📌 範例 39: ALL - 全部比對

# %%
result = spark.sql("""
    SELECT `名稱`, `價格`
    FROM toys
    WHERE `價格` > (
        SELECT MAX(`價格`)
        FROM toys
        WHERE `顏色` = '藍色'
    )
""")
result.show()

# %% [md]
#### 4️⃣0️⃣ JOIN - 一般連接（把兩個表格接起來）

# 🎈 概念解釋：
# JOIN 將兩個資料表連接起來
# 找到相同的地方連接起來
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，將玩具表和製造商詳細資料表連接，
# 顯示玩具名稱和製造商地址。
##### 📌 範例 40: JOIN - 連接兩個表格

# %%
# 建立製造商資料
manufacturers = [(1, "快樂玩具公司", "台北市")]
manufacturers_df = spark.createDataFrame(
    manufacturers, ["id", "公司名稱", "地址"]
)
manufacturers_df.createOrReplaceTempView("manufacturers")
result = spark.sql("""
    SELECT t.`名稱`, m.`公司名稱`, m.`地址`
    FROM toys t
    JOIN manufacturers m ON t.`製造商` = m.`公司名稱`
""")
result.show()

# %% [md]
## 📚 第十一章：資料表約束

#### 4️⃣1️⃣ PRIMARY KEY - 主鍵（每個玩具的身份證）

# 🎈 概念解釋：
# PRIMARY KEY 唯一識別每一筆記錄的欄位
# 確保每個資料都有獨一無二的識別碼
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，建立一個玩具資料表，
# 以 id 作為主鍵（唯一識別碼）。
# 
# ⚠️ 注意：PySpark DataFrame 沒有內建 PRIMARY KEY 約束
# 但可以透過程式邏輯來確保唯一性
##### 📌 範例 41: PRIMARY KEY - 主鍵概念（透過唯一性驗證）

# %%
# 在 PySpark 中，我們透過程式邏輯來確保主鍵唯一性
toys_with_id = [
    (1, "小熊"),
    (2, "機器人"),
    (3, "芭比娃娃"),
    # (1, "重複ID")  # 這會違反主鍵唯一性
]
toys_pk_df = spark.createDataFrame(toys_with_id, ["id", "name"])

# 驗證 id 的唯一性（模擬主鍵檢查）
total_count = toys_pk_df.count()
unique_count = toys_pk_df.select("id").distinct().count()
print(f"總記錄數: {total_count}")
print(f"唯一 ID 數: {unique_count}")

if total_count == unique_count:
    print("✅ ID 唯一性驗證通過（符合主鍵要求）")
    toys_pk_df.show()
else:
    print("❌ 發現重複的 ID（違反主鍵約束）")

# %% [md]
#### 4️⃣2️⃣ FOREIGN KEY - 外鍵（連結到另一個表格）

# 🎈 概念解釋：
# FOREIGN KEY 確保參照的資料存在於另一個資料表中
# 確保資料之間有正確的關聯
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，建立訂單資料表，
# 其中客戶 ID 是外鍵，必須參照客戶資料表。
##### 📌 範例 42: FOREIGN KEY - 外鍵關聯（透過 JOIN 驗證）

# %%
# 在 PySpark 中，我們透過 JOIN 來確保外鍵關聯的正確性
customers_data = [
    (1, "王小明"),
    (2, "李小華"),
    (3, "張小美")
]
customers_df = spark.createDataFrame(customers_data, ["id", "name"])

orders_data = [
    (101, 1, "訂單A"),  # customer_id = 1 存在
    (102, 2, "訂單B"),  # customer_id = 2 存在
    (103, 1, "訂單C"),  # customer_id = 1 存在
    # (104, 99, "訂單D")  # customer_id = 99 不存在，違反外鍵約束
]
orders_df = spark.createDataFrame(orders_data, ["order_id", "customer_id", "description"])

# 驗證外鍵完整性（使用 LEFT JOIN 檢查）
validation_df = orders_df.join(
    customers_df, 
    orders_df.customer_id == customers_df.id, 
    "left"
).filter(customers_df.id.isNull())

print("檢查外鍵約束...")
invalid_count = validation_df.count()

if invalid_count == 0:
    print("✅ 外鍵完整性驗證通過")
    print("\n訂單與客戶關聯：")
    orders_df.join(customers_df, orders_df.customer_id == customers_df.id) \
        .select(orders_df.order_id, customers_df.name, orders_df.description) \
        .show()
else:
    print(f"❌ 發現 {invalid_count} 筆無效的外鍵參照")

# %% [md]
#### 4️⃣3️⃣ CONSTRAINT - 約束條件（設定規則）

# 🎈 概念解釋：
# CONSTRAINT 定義資料表的限制條件
# 給資料設定必須遵守的規則
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，建立玩具資料表，
# 並設定價格必須大於 0 的約束條件。
##### 📌 範例 43: CONSTRAINT - 設定約束條件

# %%
# 使用 DataFrame API 驗證
from pyspark.sql.functions import col

# 驗證價格都大於 0
valid_toys = toys_df.filter(col("價格") > 0)
print(f"✅ 驗證通過，所有玩具價格都大於 0")
valid_toys.select("名稱", "價格").show(3)

# %% [md]
#### 4️⃣4️⃣ INDEX / PARTITION - 索引優化（加快搜尋速度）

# 🎈 概念解釋：
# INDEX / PARTITION 加快資料查詢的速度
# 可以快速找到想要的資料
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，對常查詢的欄位建立索引/分區以提升效能。
##### 📌 範例 44: INDEX / PARTITION - 效能優化

# %%
# PySpark 使用分區和快取來優化
toys_df.repartition("顏色").cache()
print("✅ 已按顏色分區並快取資料")

# %% [md]
## 📚 第十二章：交易控制

#### 4️⃣5️⃣ TRANSACTION - 交易（一起做完或一起取消）

# 🎈 概念解釋：
# TRANSACTION 確保一系列操作要嘛全部成功，要嘛全部失敗
# 確保多個操作要一起成功或一起失敗
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式說明交易的概念，
# 將多個操作包在一個交易中執行。
##### 📌 範例 45: TRANSACTION - 交易控制

# 交易概念說明：
# 想像你要買 3 個玩具，但錢只夠買 3 個
# 如果其中一個賣完了，那就全部都不買
# 這樣才不會只買到一部分玩具
# 
# PySpark 中交易通常在寫入資料庫時使用：
# - 開始交易 (BEGIN)
# - 執行多個操作
# - 全部成功就提交 (COMMIT)
# - 有錯誤就回滾 (ROLLBACK)

#### 4️⃣6️⃣ COMMIT - 提交（確定要存檔）

# 🎈 概念解釋：
# COMMIT 永久保存所有變更
# 確定要把改變永久保存
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，示範如何在完成所有操作後提交交易。
##### 📌 範例 46: COMMIT - 提交變更

# %%
# 模擬交易提交
try:
    # 執行操作
    new_data = [(8, "新玩具", "黃色", 350, "快樂玩具公司")]
    new_df = spark.createDataFrame(new_data, toys_df.schema)
    
    # 寫入資料（模擬 COMMIT）
    updated_toys = toys_df.union(new_df)
    print("✅ 交易已提交，新玩具已加入")
    updated_toys.select("名稱", "價格").tail(3)
except Exception as e:
    print(f"❌ 發生錯誤，交易回滾: {e}")

# %% [md]
#### 4️⃣7️⃣ ROLLBACK - 回滾（取消不要存檔）

# 🎈 概念解釋：
# ROLLBACK 取消尚未提交的變更
# 回復到交易開始前的狀態
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，示範當發生錯誤時如何回滾交易。
##### 📌 範例 47: ROLLBACK - 回滾交易

# 回滾範例：
# 假設你不小心刪掉了重要資料
# ROLLBACK 可以讓你回到刪除之前的狀態
# 還原所有未提交的操作
# 
# 在 PySpark 中：
# - 可以使用 checkpoint 保存檢查點
# - 發生錯誤時可以讀取檢查點資料
# - 恢復到之前的狀態

#### 4️⃣8️⃣ SAVEPOINT - 儲存點（設定中途存檔點）

# 🎈 概念解釋：
# SAVEPOINT 在交易中設定中途檢查點
# 可以選擇回滾到特定的儲存點
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，示範如何在交易中設定多個儲存點。
# 
# ⚠️ 注意：PySpark 不支援傳統資料庫的 SAVEPOINT 概念
# 在 PySpark 中，我們使用版本控制或快照來達到類似效果
##### 📌 範例 48: SAVEPOINT - 儲存點概念（使用快照替代）

# 在 PySpark 中，我們使用變數保存不同階段的 DataFrame
# 這類似於設定多個「儲存點」

# %%
# 儲存點 1：原始資料
savepoint_1 = toys_df
print("✅ 儲存點 1：原始玩具資料")
print(f"   記錄數：{savepoint_1.count()}")
savepoint_1.show()

# 執行一些操作
filtered_df = toys_df.filter(col("價格") > 500)

# 儲存點 2：過濾後的資料
savepoint_2 = filtered_df
print("✅ 儲存點 2：過濾後的資料")
print(f"   記錄數：{savepoint_2.count()}")
savepoint_2.show()

# 如果需要回到某個儲存點，直接使用該變數
print("💡 回到儲存點 1：")
savepoint_1.show()

# %% [md]
# 在傳統 SQL 中：
#   SAVEPOINT sp1;
#   -- 執行操作
#   SAVEPOINT sp2;
#   -- 可以 ROLLBACK TO sp1;
# 
# 在 PySpark 中：
#   使用變數保存不同階段的 DataFrame
#   或使用 Delta Lake 的時間旅行功能

## 📚 第十三章：權限管理

#### 4️⃣9️⃣ GRANT - 授予權限（給別人鑰匙）

# 🎈 概念解釋：
# GRANT 授予使用者特定的操作權限
# 控制誰可以存取資料
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，授予使用者查詢玩具資料表的權限。
##### 📌 範例 49: GRANT - 授予權限

# 權限授予範例：
# 
# -- 讓 user1 可以查詢玩具表
# GRANT SELECT ON toys TO user1;
# 
# -- 讓 user2 可以新增玩具
# GRANT INSERT ON toys TO user2;
# 
# -- 讓 admin 擁有所有權限
# GRANT ALL ON toys TO admin;
#### 5️⃣0️⃣ REVOKE - 撤銷權限（收回鑰匙）

# 🎈 概念解釋：
# REVOKE 撤銷使用者的權限
# 移除已授予的存取權限
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，撤銷使用者對玩具資料表的刪除權限。
##### 📌 範例 50: REVOKE - 撤銷權限

# 權限撤銷範例：
# 
# -- 不讓 user1 刪除玩具
# REVOKE DELETE ON toys FROM user1;
# 
# -- 收回 user2 的所有權限
# REVOKE ALL ON toys FROM user2;

## 🎓 課程總結
### 🎊 恭喜！你已經完成了 SQL 的 50 個重要觀念！")
# 📝 學習總結：
# 
# 1️⃣ 基礎操作：SELECT, INSERT, UPDATE, DELETE
#    - 這些是最常用的基本操作
# 
# 2️⃣ 資料表管理：CREATE, DROP, ALTER, TRUNCATE
#    - 管理資料庫結構
# 
# 3️⃣ 連接操作：INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL OUTER JOIN
#    - 連接多個資料表
# 
# 4️⃣ 資料篩選：WHERE, HAVING, DISTINCT, ORDER BY, GROUP BY
#    - 篩選和排序資料
# 
# 5️⃣ 聚合函數：COUNT, SUM, AVG, MIN, MAX
#    - 執行統計運算
# 
# 6️⃣ 進階查詢：CASE, EXISTS, ANY, ALL, BETWEEN, LIKE, IN
#    - 進階的查詢技巧
# 
# 7️⃣ 交易控制：TRANSACTION, COMMIT, ROLLBACK, SAVEPOINT
#    - 維護資料一致性
# 
# 8️⃣ 權限管理：GRANT, REVOKE
#    - 管理使用者權限
# 
# 💡 學習建議：
# - 每個概念都要實際練習
# - 嘗試用真實的資料練習
# - 遇到問題可以尋求協助
# - 循序漸進地學習
# 
# 🚀 下一步：
# - 練習組合多個 SQL 語句
# - 應用於實際案例
# - 深入學習進階功能
# 
# 記住：熟能生巧，多練習就能掌握 SQL！
## 📚 教學文件結束 - 祝你學習愉快！🎈

# 關閉 Spark Session

# %%
# spark.stop()
