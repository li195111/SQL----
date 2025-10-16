# -*- coding: utf-8 -*-
# %% [md]
## PySpark SQL 進階教學 🚀
# 
# 這份教學專為已經掌握 SQL 基礎的你設計！
# 我們將深入探討進階的 SQL 技巧，包括視窗函數、CTE、效能優化等主題。
# 讓你的 SQL 技能更上一層樓！✨
# 
# 作者：QChoice AI 教學團隊
# 日期：2025-01-15

# 
# 🚀 歡迎來到 SQL 進階課程！
#
# --------------------------------------------------
#
# 🎯 環境設定 - 準備進階開發環境

# %%
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

# 創建 Spark Session - 配置進階效能參數
spark = SparkSession.builder \
    .appName("SQL進階教學") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# %% [md]
### 📚 第一章：視窗函數 (Window Functions)

#### 1️⃣ ROW_NUMBER - 為每一列分配唯一的序號

# 🎈 概念解釋：
# ROW_NUMBER() 為分區內的每一列分配一個唯一的序號。
# 常用於排名、去重、分頁等場景。
# 
# 🎯 AI Prompt 範例：
# 請幫我寫一個 PySpark 程式，為每個部門的員工按薪資排序並分配序號。

##### 📌 範例 1: ROW_NUMBER - 排名與編號

# %%

# 建立員工資料
employees_data = [
    (1, "Alice", "工程部", 75000),
    (2, "Bob", "工程部", 82000),
    (3, "Charlie", "工程部", 68000),
    (4, "David", "行銷部", 65000),
    (5, "Eve", "行銷部", 72000),
    (6, "Frank", "行銷部", 70000),
    (7, "Grace", "人資部", 60000),
    (8, "Henry", "人資部", 63000)
]

employees_df = spark.createDataFrame(
    employees_data, 
    ["員工編號", "姓名", "部門", "薪資"]
)

# 定義視窗規格：按部門分組，按薪資降序排列
windowSpec = Window.partitionBy("部門").orderBy(desc("薪資"))

# 使用 ROW_NUMBER
result = employees_df.withColumn("部門排名", row_number().over(windowSpec))

result.orderBy("部門", "部門排名").show()

# SQL 寫法
employees_df.createOrReplaceTempView("employees")

# %%
sql_result = spark.sql("""
    SELECT 
        `員工編號`,
        `姓名`,
        `部門`,
        `薪資`,
        ROW_NUMBER() OVER (PARTITION BY `部門` ORDER BY `薪資` DESC) as `部門排名`
    FROM employees
    ORDER BY `部門`, `部門排名`
""")
sql_result.show()

# %% [md]
#### 2️⃣ RANK & DENSE_RANK - 處理並列排名

# 🎈 概念解釋：
# RANK() 和 DENSE_RANK() 都用於排名，但處理相同值的方式不同：
# - RANK(): 相同值得到相同排名，下一個排名會跳號
# - DENSE_RANK(): 相同值得到相同排名，下一個排名連續
# 
# 🎯 應用場景：
# 成績排名、銷售業績排名等需要處理並列情況的場景

##### 📌 範例 2: RANK vs DENSE_RANK - 並列排名處理

# %%

# 建立考試成績資料
scores_data = [
    (1, "Alice", 95),
    (2, "Bob", 95),
    (3, "Charlie", 90),
    (4, "David", 90),
    (5, "Eve", 85),
    (6, "Frank", 80),
]

scores_df = spark.createDataFrame(scores_data, ["學號", "姓名", "分數"])

# 定義視窗規格
windowSpec = Window.orderBy(desc("分數"))

result = scores_df.withColumn("RANK", rank().over(windowSpec)) \
                  .withColumn("DENSE_RANK", dense_rank().over(windowSpec)) \
                  .withColumn("ROW_NUMBER", row_number().over(windowSpec))

result.show()

# %% [md]
# 💡 說明：
# - Alice 和 Bob 都是 95 分，RANK 和 DENSE_RANK 都是 1
# - Charlie 和 David 都是 90 分：
#   * RANK 是 3（因為前面有 2 個人）
#   * DENSE_RANK 是 2（連續編號）
# - ROW_NUMBER 為每個人分配唯一編號

#### 3️⃣ LEAD & LAG - 訪問前後列的資料
# 🎈 概念解釋：
# LEAD() 和 LAG() 允許你訪問當前列之前或之後的資料：
# - LAG(): 訪問前面的列
# - LEAD(): 訪問後面的列
# 
# 🎯 應用場景：
# 計算增長率、同比環比分析、時間序列分析

##### 📌 範例 3: LEAD & LAG - 時間序列分析

# %%

# 建立月度銷售資料
sales_data = [
    ("2024-01", 100000),
    ("2024-02", 120000),
    ("2024-03", 115000),
    ("2024-04", 130000),
    ("2024-05", 135000),
    ("2024-06", 140000),
]

sales_df = spark.createDataFrame(sales_data, ["月份", "銷售額"])

# 定義視窗規格
windowSpec = Window.orderBy("月份")

result = sales_df.withColumn("上月銷售額", lag("銷售額", 1).over(windowSpec)) \
                 .withColumn("下月銷售額", lead("銷售額", 1).over(windowSpec)) \
                 .withColumn("月增長額", col("銷售額") - lag("銷售額", 1).over(windowSpec)) \
                 .withColumn("月增長率%", 
                            round((col("銷售額") - lag("銷售額", 1).over(windowSpec)) / 
                                  lag("銷售額", 1).over(windowSpec) * 100, 2))

result.show()

# %% \[md]
#### 4️⃣ 累計計算 - SUM/AVG OVER
# 🎈 概念解釋：
# 視窗函數配合聚合函數可以進行累計計算，而不會折疊資料列。
# 
# 🎯 應用場景：
# 累計銷售額、移動平均、滾動統計



##### 📌 範例 4: 累計計算與移動平均

# %%

# 繼續使用銷售資料
windowSpec = Window.orderBy("月份").rowsBetween(Window.unboundedPreceding, Window.currentRow)
movingAvgSpec = Window.orderBy("月份").rowsBetween(-2, 0)  # 3個月移動平均

result = sales_df.withColumn("累計銷售額", sum("銷售額").over(windowSpec)) \
                 .withColumn("3月移動平均", round(avg("銷售額").over(movingAvgSpec), 2))

result.show()


# %% [md]
### 📚 第二章：通用表表達式 (Common Table Expressions - CTE)

# %% [md]
#### 5️⃣ WITH 子句 - 建立臨時結果集
# 🎈 概念解釋：
# CTE (WITH 子句) 讓你建立臨時的命名結果集，提高查詢的可讀性和可維護性。
# 可以把複雜查詢分解為多個步驟。
# 
# 🎯 優勢：
# 1. 提高可讀性
# 2. 可重複使用
# 3. 便於除錯
# 4. 支援遞迴查詢



##### 📌 範例 5: CTE - 簡化複雜查詢

# %%

# 建立訂單資料
orders_data = [
    (1, "Alice", "2024-01-15", 1500),
    (2, "Bob", "2024-01-20", 2300),
    (3, "Alice", "2024-02-10", 1800),
    (4, "Charlie", "2024-02-15", 3200),
    (5, "Bob", "2024-03-05", 2100),
    (6, "Alice", "2024-03-20", 2500),
]

orders_df = spark.createDataFrame(
    orders_data,
    ["訂單編號", "客戶", "訂單日期", "金額"]
)
orders_df.createOrReplaceTempView("orders")

# 使用 CTE 計算客戶總消費和平均消費
sql_with_cte = spark.sql("""
    WITH customer_stats AS (
        SELECT 
            `客戶`,
            COUNT(*) as `訂單數`,
            SUM(`金額`) as `總消費`,
            AVG(`金額`) as `平均消費`
        FROM orders
        GROUP BY `客戶`
    ),
    high_value_customers AS (
        SELECT 
            `客戶`,
            `訂單數`,
            `總消費`,
            ROUND(`平均消費`, 2) as `平均消費`
        FROM customer_stats
        WHERE `總消費` > 5000
    )
    SELECT * FROM high_value_customers
    ORDER BY `總消費` DESC
""")

sql_with_cte.show()

# %% \[md]
#### 6️⃣ 多層 CTE - 複雜業務邏輯
# 🎈 概念解釋：
# 多層 CTE 可以將複雜的業務邏輯分解為多個清晰的步驟。
# 
# 🎯 應用場景：
# 多步驟的資料轉換、複雜的業務指標計算



##### 📌 範例 6: 多層 CTE - 客戶分級分析

# %%

sql_multi_cte = spark.sql("""
    WITH monthly_sales AS (
        -- 第一層：計算每個客戶的月度銷售
        SELECT 
            `客戶`,
            DATE_FORMAT(TO_DATE(`訂單日期`), 'yyyy-MM') as `月份`,
            SUM(`金額`) as `月銷售額`
        FROM orders
        GROUP BY `客戶`, DATE_FORMAT(TO_DATE(`訂單日期`), 'yyyy-MM')
    ),
    customer_summary AS (
        -- 第二層：匯總客戶統計
        SELECT 
            `客戶`,
            COUNT(DISTINCT `月份`) as `活躍月數`,
            SUM(`月銷售額`) as `總銷售額`,
            AVG(`月銷售額`) as `平均月銷售額`
        FROM monthly_sales
        GROUP BY `客戶`
    ),
    customer_level AS (
        -- 第三層：客戶分級
        SELECT 
            `客戶`,
            `活躍月數`,
            ROUND(`總銷售額`, 2) as `總銷售額`,
            ROUND(`平均月銷售額`, 2) as `平均月銷售額`,
            CASE 
                WHEN `總銷售額` >= 6000 THEN '白金客戶'
                WHEN `總銷售額` >= 4000 THEN '金牌客戶'
                ELSE '一般客戶'
            END as `客戶等級`
        FROM customer_summary
    )
    SELECT * FROM customer_level
    ORDER BY `總銷售額` DESC
""")

sql_multi_cte.show()


# %% [md]
### 📚 第三章：子查詢優化
# 7️⃣ 相關子查詢 vs 非相關子查詢

# 🎈 概念解釋：
# - 非相關子查詢：內部查詢獨立執行，只執行一次
# - 相關子查詢：內部查詢依賴外部查詢，可能執行多次（效能較差）
# 
# 🎯 優化建議：
# 盡量使用 JOIN 或視窗函數替代相關子查詢



##### 📌 範例 7: 子查詢優化 - 找出高於部門平均薪資的員工

# %%

# 方法 1：使用相關子查詢（較慢）
sql_correlated = spark.sql("""
    SELECT 
        e1.`姓名`,
        e1.`部門`,
        e1.`薪資`,
        (SELECT AVG(e2.`薪資`) 
         FROM employees e2 
         WHERE e2.`部門` = e1.`部門`) as `部門平均薪資`
    FROM employees e1
    WHERE e1.`薪資` > (
        SELECT AVG(e2.`薪資`) 
        FROM employees e2 
        WHERE e2.`部門` = e1.`部門`
    )
    ORDER BY e1.`部門`, e1.`薪資` DESC
""")

sql_correlated.show()

# 方法 2：使用 JOIN（較快）
sql_join = spark.sql("""
    WITH dept_avg AS (
        SELECT `部門`, AVG(`薪資`) as `平均薪資`
        FROM employees
        GROUP BY `部門`
    )
    SELECT 
        e.`姓名`,
        e.`部門`,
        e.`薪資`,
        ROUND(d.`平均薪資`, 2) as `部門平均薪資`
    FROM employees e
    JOIN dept_avg d ON e.`部門` = d.`部門`
    WHERE e.`薪資` > d.`平均薪資`
    ORDER BY e.`部門`, e.`薪資` DESC
""")

sql_join.show()

# 方法 3：使用視窗函數（最快）
windowSpec = Window.partitionBy("部門")
result_window = employees_df.withColumn("部門平均薪資", round(avg("薪資").over(windowSpec), 2)) \
                            .filter(col("薪資") > col("部門平均薪資")) \
                            .orderBy("部門", desc("薪資"))

result_window.show()


# %% [md]
### 📚 第四章：進階 JOIN 技巧

# %% [md]
#### 8️⃣ CROSS JOIN - 笛卡爾積
# 🎈 概念解釋：
# CROSS JOIN 產生兩個表的笛卡爾積，即所有可能的組合。
# 
# 🎯 應用場景：
# 生成測試資料、建立時間序列、產生所有可能的組合



##### 📌 範例 8: CROSS JOIN - 產生所有可能的配對

# %%

# 建立產品和顏色資料
products = spark.createDataFrame([("手機",), ("平板",), ("筆電",)], ["產品"])
colors = spark.createDataFrame([("黑色",), ("白色",), ("銀色",)], ["顏色"])

# CROSS JOIN
cross_result = products.crossJoin(colors)

cross_result.show()

# %% \[md]
#### 9️⃣ SELF JOIN - 自我連接
# 🎈 概念解釋：
# SELF JOIN 是表與自己進行連接，常用於查找層級關係或比較同表內的記錄。
# 
# 🎯 應用場景：
# 員工-主管關係、組織架構、尋找重複記錄



##### 📌 範例 9: SELF JOIN - 員工與主管關係

# %%

# 建立包含主管資訊的員工資料
emp_manager_data = [
    (1, "Alice", None),      # CEO，沒有主管
    (2, "Bob", 1),           # Bob 的主管是 Alice
    (3, "Charlie", 1),       # Charlie 的主管是 Alice
    (4, "David", 2),         # David 的主管是 Bob
    (5, "Eve", 2),           # Eve 的主管是 Bob
    (6, "Frank", 3),         # Frank 的主管是 Charlie
]

emp_manager_df = spark.createDataFrame(
    emp_manager_data,
    ["員工編號", "姓名", "主管編號"]
)
emp_manager_df.createOrReplaceTempView("emp_manager")

# 使用 SELF JOIN 查詢員工和其主管
sql_self_join = spark.sql("""
    SELECT 
        e.`員工編號`,
        e.`姓名` as `員工姓名`,
        COALESCE(m.`姓名`, '無主管') as `主管姓名`
    FROM emp_manager e
    LEFT JOIN emp_manager m ON e.`主管編號` = m.`員工編號`
    ORDER BY e.`員工編號`
""")

sql_self_join.show()


# 🔟 ANTI JOIN & SEMI JOIN - 高效過濾

# 🎈 概念解釋：
# - SEMI JOIN (LEFT SEMI): 返回左表中在右表中有匹配的記錄
# - ANTI JOIN (LEFT ANTI): 返回左表中在右表中沒有匹配的記錄
# 
# 🎯 優勢：
# 比 IN / NOT IN 子查詢效能更好



##### 📌 範例 10: ANTI JOIN & SEMI JOIN - 客戶訂單分析

# %%

# 建立客戶資料
customers_data = [
    (1, "Alice"),
    (2, "Bob"),
    (3, "Charlie"),
    (4, "David"),
    (5, "Eve"),
]

customers_df = spark.createDataFrame(customers_data, ["客戶編號", "客戶姓名"])

# 建立訂單資料（只有部分客戶有訂單）
order_customers = orders_df.select("客戶").distinct()

# SEMI JOIN - 有訂單的客戶
customers_with_orders = customers_df.join(
    order_customers,
    customers_df["客戶姓名"] == order_customers["客戶"],
    "leftsemi"
)

customers_with_orders.show()

# ANTI JOIN - 沒有訂單的客戶
customers_without_orders = customers_df.join(
    order_customers,
    customers_df["客戶姓名"] == order_customers["客戶"],
    "leftanti"
)

customers_without_orders.show()


# %% [md]
### 📚 第五章：效能優化技巧
# 1️⃣1️⃣ Broadcast Join - 廣播小表

# 🎈 概念解釋：
# 當一個大表和一個小表進行 JOIN 時，可以將小表廣播到所有節點，
# 避免 shuffle 操作，大幅提升效能。
# 
# 🎯 適用場景：
# 小表 < 10MB，大表與維度表 JOIN



##### 📌 範例 11: Broadcast Join - 優化大小表連接

# %%

# 建立部門資訊（小表）
departments_data = [
    ("工程部", "技術大樓"),
    ("行銷部", "行政大樓"),
    ("人資部", "行政大樓"),
]

departments_df = spark.createDataFrame(departments_data, ["部門", "辦公地點"])

# 一般 JOIN
normal_join = employees_df.join(departments_df, "部門")

normal_join.select("姓名", "部門", "辦公地點").show(5)

# Broadcast JOIN
broadcast_join = employees_df.join(
    broadcast(departments_df),
    "部門"
)

broadcast_join.select("姓名", "部門", "辦公地點").show(5)

# 查看執行計畫
print("一般 JOIN 會有 SortMergeJoin")
print("Broadcast JOIN 會有 BroadcastHashJoin")


# 1️⃣2️⃣ 分區與分桶 - 資料組織優化

# 🎈 概念解釋：
# - Partitioning: 按欄位值將資料分割成多個目錄
# - Bucketing: 按 hash 值將資料分割成固定數量的檔案
# 
# 🎯 優勢：
# 減少掃描的資料量，提升查詢效能



##### 📌 範例 12: 分區與過濾優化

# %%

# 重新分區資料
employees_partitioned = employees_df.repartition(2, "部門")


# 使用分區欄位過濾（效能更好）
filtered = employees_partitioned.filter(col("部門") == "工程部")
filtered.show()


# 1️⃣3️⃣ 快取與持久化 - 避免重複計算

# 🎈 概念解釋：
# 將常用的 DataFrame 快取到記憶體中，避免重複計算。
# 
# 🎯 適用場景：
# 多次使用相同的中間結果、迭代計算



##### 📌 範例 13: 快取優化

# %%

# 建立一個複雜的計算
complex_df = employees_df.groupBy("部門") \
                        .agg(
                            count("*").alias("人數"),
                            avg("薪資").alias("平均薪資"),
                            max("薪資").alias("最高薪資")
                        )

# 快取結果
complex_df.cache()

# 第一次執行（會觸發計算並快取）
start = time.time()
complex_df.show()
print(f"執行時間: {time.time() - start:.4f} 秒")

# 第二次執行（從快取讀取）
start = time.time()
complex_df.show()
print(f"執行時間: {time.time() - start:.4f} 秒（應該更快）")

# 釋放快取
complex_df.unpersist()


# %% [md]
### 📚 第六章：複雜資料轉換
# 1️⃣4️⃣ PIVOT - 行轉列

# 🎈 概念解釋：
# PIVOT 將行資料轉換為列，常用於建立交叉表。
# 
# 🎯 應用場景：
# 銷售報表、資料透視表、趨勢分析



##### 📌 範例 14: PIVOT - 建立部門薪資透視表

# %%

# 使用 PIVOT 建立部門-統計指標透視表
employees_df.createOrReplaceTempView("employees")

pivot_result = spark.sql("""
    SELECT * FROM (
        SELECT `部門`, `薪資`
        FROM employees
    )
    PIVOT (
        COUNT(*) as `人數`,
        AVG(`薪資`) as `平均薪資`
        FOR `部門` IN ('工程部', '行銷部', '人資部')
    )
""")

pivot_result.show()


# 1️⃣5️⃣ UNPIVOT - 列轉行

# 🎈 概念解釋：
# UNPIVOT 將列資料轉換為行，是 PIVOT 的反向操作。
# 
# 🎯 應用場景：
# 將寬表轉換為長表、資料正規化



##### 📌 範例 15: UNPIVOT - 列轉行

# %%

# 建立寬表格式的季度銷售資料
quarterly_sales = spark.createDataFrame([
    ("產品A", 100, 120, 115, 130),
    ("產品B", 80, 90, 95, 100),
], ["產品", "Q1", "Q2", "Q3", "Q4"])

quarterly_sales.createOrReplaceTempView("quarterly_sales")

# 使用 UNPIVOT（Spark 3.4+）或使用 stack 函數
unpivot_result = quarterly_sales.selectExpr(
    "`產品`",
    "stack(4, 'Q1', `Q1`, 'Q2', `Q2`, 'Q3', `Q3`, 'Q4', `Q4`) as (`季度`, `銷售額`)"
)

unpivot_result.show()


# 1️⃣6️⃣ 陣列與結構處理

# 🎈 概念解釋：
# PySpark 支援複雜的資料型態如陣列、結構體、地圖等。
# 
# 🎯 應用場景：
# 處理 JSON 資料、巢狀結構、多值欄位



##### 📌 範例 16: 陣列操作

# %%

# 建立包含陣列的資料
students_data = [
    (1, "Alice", ["數學", "物理", "化學"]),
    (2, "Bob", ["英文", "歷史"]),
    (3, "Charlie", ["數學", "英文", "體育"]),
]

students_df = spark.createDataFrame(
    students_data,
    ["學號", "姓名", "選修課程"]
)

students_df.show(truncate=False)

# 展開陣列
exploded = students_df.select(
    "學號",
    "姓名",
    explode("選修課程").alias("課程")
)

exploded.show()

# 陣列相關函數
array_operations = students_df.select(
    "姓名",
    "選修課程",
    size("選修課程").alias("課程數"),
    array_contains("選修課程", "數學").alias("是否選修數學")
)

array_operations.show(truncate=False)


# %% [md]
### 📚 第七章：資料品質與清理
# 1️⃣7️⃣ 處理 NULL 值

# 🎈 概念解釋：
# NULL 值處理是資料清理的重要環節。
# 
# 🎯 常用方法：
# fillna, dropna, coalesce, nvl



##### 📌 範例 17: NULL 值處理

# %%

# 建立包含 NULL 的資料
data_with_null = [
    (1, "Alice", 75000, "工程部"),
    (2, "Bob", None, "行銷部"),
    (3, "Charlie", 68000, None),
    (4, None, 65000, "人資部"),
]

df_null = spark.createDataFrame(
    data_with_null,
    ["員工編號", "姓名", "薪資", "部門"]
)

df_null.show()

# 方法 1: 填充預設值
filled = df_null.fillna({
    "姓名": "未知",
    "薪資": 60000,
    "部門": "待分配"
})

filled.show()

# 方法 2: 刪除包含 NULL 的列
dropped = df_null.dropna()

dropped.show()

# 方法 3: 使用 COALESCE
coalesced = df_null.select(
    "員工編號",
    coalesce(col("姓名"), lit("未知")).alias("姓名"),
    coalesce(col("薪資"), lit(60000)).alias("薪資"),
    coalesce(col("部門"), lit("待分配")).alias("部門")
)

coalesced.show()


# 1️⃣8️⃣ 資料去重

# 🎈 概念解釋：
# 找出並移除重複的記錄。
# 
# 🎯 方法：
# distinct, dropDuplicates, 視窗函數



##### 📌 範例 18: 資料去重

# %%

# 建立包含重複資料
duplicate_data = [
    (1, "Alice", "工程部"),
    (2, "Bob", "行銷部"),
    (1, "Alice", "工程部"),  # 完全重複
    (3, "Alice", "人資部"),  # 姓名重複
]

df_dup = spark.createDataFrame(
    duplicate_data,
    ["員工編號", "姓名", "部門"]
)

df_dup.show()

# 方法 1: 完全去重
dedup_all = df_dup.distinct()
dedup_all.show()

# 方法 2: 按特定欄位去重（保留第一筆）
dedup_by_name = df_dup.dropDuplicates(["姓名"])
dedup_by_name.show()

# 方法 3: 使用視窗函數保留特定規則的記錄
windowSpec = Window.partitionBy("姓名").orderBy("員工編號")
dedup_window = df_dup.withColumn("rn", row_number().over(windowSpec)) \
                     .filter(col("rn") == 1) \
                     .drop("rn")

dedup_window.show()


# %% [md]
### 📚 第八章：進階分析函數
# 1️⃣9️⃣ NTILE - 資料分組

# 🎈 概念解釋：
# NTILE 將資料分成 N 個大致相等的組。
# 
# 🎯 應用場景：
# 客戶分層、ABC 分析、分位數分析



##### 📌 範例 19: NTILE - 薪資四分位數

# %%

windowSpec = Window.orderBy("薪資")

ntile_result = employees_df.withColumn(
    "薪資分組",
    ntile(4).over(windowSpec)
).withColumn(
    "分組說明",
    when(col("薪資分組") == 1, "低薪組")
    .when(col("薪資分組") == 2, "中低薪組")
    .when(col("薪資分組") == 3, "中高薪組")
    .otherwise("高薪組")
)

ntile_result.orderBy("薪資").show()


# 2️⃣0️⃣ PERCENT_RANK - 百分位排名

# 🎈 概念解釋：
# PERCENT_RANK 計算值在資料集中的相對位置（0 到 1 之間）。
# 
# 🎯 應用場景：
# 績效評估、成績分析、相對排名



##### 📌 範例 20: PERCENT_RANK - 薪資百分位

# %%

windowSpec = Window.orderBy("薪資")

percent_result = employees_df.withColumn(
    "薪資百分位",
    round(percent_rank().over(windowSpec) * 100, 2)
).withColumn(
    "說明",
    concat(
        lit("薪資超過 "),
        round(percent_rank().over(windowSpec) * 100, 0).cast("int"),
        lit("% 的員工")
    )
)

percent_result.orderBy("薪資").select("姓名", "薪資", "薪資百分位", "說明").show()


# 🎓 課程總結

# %% [md]
## 🎓 課程總結
### 🎊 恭喜！你已經完成了 SQL 進階課程！
# 📝 進階技能總結：
# 
# 1️⃣ 視窗函數：
#    - ROW_NUMBER, RANK, DENSE_RANK：排名與編號
#    - LEAD, LAG：訪問前後列資料
#    - SUM/AVG OVER：累計計算與移動平均
#    - NTILE, PERCENT_RANK：分組與百分位
# 
# 2️⃣ 通用表表達式 (CTE)：
#    - WITH 子句：提高查詢可讀性
#    - 多層 CTE：分解複雜邏輯
#    - 遞迴 CTE：處理層級關係
# 
# 3️⃣ 子查詢優化：
#    - 避免相關子查詢
#    - 使用 JOIN 替代
#    - 使用視窗函數優化
# 
# 4️⃣ 進階 JOIN：
#    - CROSS JOIN：笛卡爾積
#    - SELF JOIN：自我連接
#    - SEMI/ANTI JOIN：高效過濾
# 
# 5️⃣ 效能優化：
#    - Broadcast Join：廣播小表
#    - 分區與分桶：資料組織
#    - 快取與持久化：避免重複計算
# 
# 6️⃣ 複雜資料轉換：
#    - PIVOT/UNPIVOT：行列轉換
#    - 陣列與結構：處理複雜型態
#    - 資料展開與聚合
# 
# 7️⃣ 資料品質：
#    - NULL 值處理
#    - 資料去重
#    - 資料驗證
# 
# 💡 進階學習建議：
# - 理解執行計畫，學會效能分析
# - 掌握資料分區策略
# - 熟悉 Spark UI 的使用
# - 實踐 Delta Lake 進階功能
# - 學習 Spark Streaming
# 
# 🚀 下一步：
# - 深入研究 Spark 內部機制
# - 學習分散式系統原理
# - 實踐大規模資料處理專案
# - 探索機器學習與 Spark MLlib
# 
# 記住：進階技能需要大量實踐，多做專案、多解決實際問題才能真正掌握！
## 📚 進階教學文件結束 - 繼續精進你的技能！🚀

# 關閉 Spark Session

# %%
# spark.stop()  # 取消註解以關閉 Spark
