# %% [md]
## PySpark SQL 信用卡交易資料分析實戰
# 
# 作者：QChoice AI 教學團隊
# 日期：2025-01-16

# %%
# 環境設定與套件載入
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import random

# 建立 Spark Session
spark = SparkSession.builder \
    .appName("CreditCardAnalysis") \
    .config("spark.sql.warehouse.dir", "./spark-warehouse") \
    .getOrCreate()

print(f"✅ Spark版本: {spark.version}")
print(f"✅ 環境設定完成")

# %% [md]
### 📚 單元一：建立信用卡交易假資料

#### 1.1 使用者資料建立

# 🎈 概念解釋：
# 使用 Numpy 和 Pandas 建立使用者基本資料，包含使用者ID、姓名、年齡、職業等資訊

# %%
# 設定隨機種子以確保結果可重現
np.random.seed(42)
random.seed(42)

# 建立使用者資料
n_users = 1000

# 姓氏和名字列表（台灣常見姓名）
last_names = ['陳', '林', '黃', '張', '李', '王', '吳', '劉', '蔡', '楊', '許', '鄭', '謝', '郭', '洪']
first_names = ['怡君', '志明', '雅婷', '建國', '淑芬', '俊傑', '美玲', '家豪', '詩涵', '冠宇', '佳穎', '宗翰', '筱涵', '承恩', '雅筑']

users_data = {
    'user_id': [f'U{str(i).zfill(6)}' for i in range(1, n_users + 1)],
    'user_name': [random.choice(last_names) + random.choice(first_names) for _ in range(n_users)],
    'age': np.random.randint(20, 70, n_users),
    'gender': np.random.choice(['M', 'F'], n_users),
    'occupation': np.random.choice(['上班族', '學生', '自由業', '退休', '家管', '公務員', '醫療', '教育'], n_users),
    'city': np.random.choice(['台北', '新北', '桃園', '台中', '台南', '高雄', '新竹', '基隆'], n_users),
    'annual_income': np.random.randint(300000, 2000000, n_users)
}

users_df = pd.DataFrame(users_data)
print("✅ 使用者資料建立完成")
users_df.head()

# %% [md]
##### 📌 範例 1.1: 將 Pandas DataFrame 轉換為 PySpark DataFrame

# %%
# 轉換為 PySpark DataFrame
users_spark = spark.createDataFrame(users_df)

# 註冊為臨時表格
users_spark.createOrReplaceTempView("users")

print("📊 使用者資料表結構：")
users_spark.printSchema()
print(f"\n總使用者數：{users_spark.count()}")
users_spark.show(5)

# %% [md]
#### 1.2 信用卡持卡人資料建立

# 🎈 概念解釋：
# 每位使用者可能持有多張信用卡，建立信用卡基本資訊，包含卡號、卡別、額度等

# %%
# 建立信用卡資料
n_cards = 1500

card_types = ['金卡', '白金卡', '鈦金卡', '普卡', '商務卡']
card_brands = ['VISA', 'MasterCard', 'JCB', 'American Express']
banks = ['台新銀行', '國泰世華', '中信銀行', '玉山銀行', '富邦銀行', '第一銀行']

# 隨機分配信用卡給使用者（有些使用者有多張卡）
card_owners = np.random.choice(users_df['user_id'].values, n_cards)

cards_data = {
    'card_id': [f'C{str(i).zfill(8)}' for i in range(1, n_cards + 1)],
    'user_id': card_owners,
    'card_number': [f'{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}' for _ in range(n_cards)],
    'card_type': np.random.choice(card_types, n_cards, p=[0.15, 0.25, 0.10, 0.40, 0.10]),
    'card_brand': np.random.choice(card_brands, n_cards, p=[0.40, 0.35, 0.15, 0.10]),
    'bank': np.random.choice(banks, n_cards),
    'credit_limit': np.random.choice([50000, 100000, 200000, 300000, 500000, 800000], n_cards),
    'issue_date': pd.to_datetime([datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1825)) for _ in range(n_cards)]),
    'card_status': np.random.choice(['active', 'suspended', 'closed'], n_cards, p=[0.85, 0.10, 0.05])
}

cards_df = pd.DataFrame(cards_data)
print("✅ 信用卡資料建立完成")
cards_df.head()

# %% [md]
##### 📌 範例 1.2: 轉換信用卡資料為 Spark DataFrame

# %%
# 轉換為 PySpark DataFrame
cards_spark = spark.createDataFrame(cards_df)

# 註冊為臨時表格
cards_spark.createOrReplaceTempView("cards")

print("📊 信用卡資料表結構：")
cards_spark.printSchema()
print(f"\n總信用卡數：{cards_spark.count()}")
cards_spark.show(5)

# %% [md]
#### 1.3 信用卡刷卡交易紀錄建立

# 🎈 概念解釋：
# 建立信用卡交易明細，包含交易時間、金額、商店類別、交易狀態等資訊

# %%
# 建立交易資料
n_transactions = 50000

# 只使用狀態為 active 的信用卡
active_cards = cards_df[cards_df['card_status'] == 'active']['card_id'].values

# 商店類別
merchant_categories = [
    '超市', '餐廳', '加油站', '百貨公司', '網購', 
    '電影院', '書店', '藥局', '咖啡廳', '服飾店',
    '3C賣場', '便利商店', '旅遊', '飯店', '醫療'
]

# 交易地點
locations = ['台北', '新北', '桃園', '台中', '台南', '高雄', '新竹', '基隆', '國外']

# 生成交易紀錄
transactions_data = {
    'transaction_id': [f'T{str(i).zfill(10)}' for i in range(1, n_transactions + 1)],
    'card_id': np.random.choice(active_cards, n_transactions),
    'transaction_date': pd.to_datetime([
        datetime(2024, 1, 1) + timedelta(
            days=random.randint(0, 365),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        ) for _ in range(n_transactions)
    ]),
    'merchant_name': [f"{random.choice(merchant_categories)}{random.randint(1, 100)}號店" for _ in range(n_transactions)],
    'merchant_category': np.random.choice(merchant_categories, n_transactions),
    'amount': np.random.gamma(2, 500, n_transactions).astype(int),  # 使用 Gamma 分布模擬真實交易金額
    'location': np.random.choice(locations, n_transactions, p=[0.25, 0.20, 0.15, 0.15, 0.10, 0.08, 0.05, 0.01, 0.01]),
    'transaction_status': np.random.choice(['approved', 'declined', 'pending'], n_transactions, p=[0.90, 0.08, 0.02]),
    'is_online': np.random.choice([True, False], n_transactions, p=[0.35, 0.65])
}

transactions_df = pd.DataFrame(transactions_data)

# 確保金額為正數且合理
transactions_df['amount'] = transactions_df['amount'].clip(lower=10, upper=100000)

print("✅ 交易資料建立完成")
transactions_df.head()

# %% [md]
##### 📌 範例 1.3: 轉換交易資料為 Spark DataFrame

# %%
# 轉換為 PySpark DataFrame
transactions_spark = spark.createDataFrame(transactions_df)

# 註冊為臨時表格
transactions_spark.createOrReplaceTempView("transactions")

print("📊 交易資料表結構：")
transactions_spark.printSchema()
print(f"\n總交易筆數：{transactions_spark.count()}")
transactions_spark.show(5)

# %% [md]
### 📚 單元二：SQL 基礎查詢練習

#### 2.1 SELECT 與 WHERE 子句

# 🎈 概念解釋：
# 使用 SELECT 選擇欄位，WHERE 進行條件篩選

# %% [md]
##### 📌 範例 2.1: 查詢年齡大於40歲的使用者

# %%
query = """
SELECT user_id, user_name, age, city, occupation
FROM users
WHERE age > 40
ORDER BY age DESC
LIMIT 10
"""

result = spark.sql(query)
result.show()

# %% [md]
##### 📌 範例 2.2: 查詢台北地區的高收入使用者（年收入超過100萬）

# %%
query = """
SELECT user_id, user_name, age, city, annual_income
FROM users
WHERE city = '台北' AND annual_income > 1000000
ORDER BY annual_income DESC
"""

result = spark.sql(query)
result.show()

# %% [md]
#### 2.2 聚合函數與 GROUP BY

# 🎈 概念解釋：
# 使用聚合函數（COUNT, SUM, AVG, MAX, MIN）搭配 GROUP BY 進行資料統計

# %% [md]
##### 📌 範例 2.3: 統計各城市的使用者數量

# %%
query = """
SELECT 
    city,
    COUNT(*) as user_count,
    AVG(age) as avg_age,
    AVG(annual_income) as avg_income
FROM users
GROUP BY city
ORDER BY user_count DESC
"""

result = spark.sql(query)
result.show()

# %% [md]
##### 📌 範例 2.4: 統計各商店類別的交易總額

# %%
query = """
SELECT 
    merchant_category,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    MAX(amount) as max_amount
FROM transactions
WHERE transaction_status = 'approved'
GROUP BY merchant_category
ORDER BY total_amount DESC
"""

result = spark.sql(query)
result.show()

# %% [md]
### 📚 單元三：JOIN 連接查詢

#### 3.1 INNER JOIN

# 🎈 概念解釋：
# INNER JOIN 取得兩個表格的交集資料

# %% [md]
##### 📌 範例 3.1: 查詢使用者的信用卡資訊

# %%
query = """
SELECT 
    u.user_id,
    u.user_name,
    u.city,
    c.card_id,
    c.card_type,
    c.card_brand,
    c.credit_limit
FROM users u
INNER JOIN cards c ON u.user_id = c.user_id
WHERE c.card_status = 'active'
ORDER BY u.user_id
LIMIT 20
"""

result = spark.sql(query)
result.show()

# %% [md]
##### 📌 範例 3.2: 查詢交易紀錄與持卡人資訊

# %%
query = """
SELECT 
    t.transaction_id,
    t.transaction_date,
    t.amount,
    t.merchant_category,
    c.card_type,
    u.user_name,
    u.city
FROM transactions t
INNER JOIN cards c ON t.card_id = c.card_id
INNER JOIN users u ON c.user_id = u.user_id
WHERE t.transaction_status = 'approved'
ORDER BY t.transaction_date DESC
LIMIT 20
"""

result = spark.sql(query)
result.show(truncate=False)

# %% [md]
#### 3.2 LEFT JOIN

# 🎈 概念解釋：
# LEFT JOIN 保留左表所有資料，右表沒有對應則顯示 NULL

# %% [md]
##### 📌 範例 3.3: 查詢所有使用者及其信用卡數量（包含沒有信用卡的使用者）

# %%
query = """
SELECT 
    u.user_id,
    u.user_name,
    u.city,
    COUNT(c.card_id) as card_count
FROM users u
LEFT JOIN cards c ON u.user_id = c.user_id
GROUP BY u.user_id, u.user_name, u.city
ORDER BY card_count DESC, u.user_id
LIMIT 20
"""

result = spark.sql(query)
result.show()

# %% [md]
### 📚 單元四：進階 SQL 查詢

#### 4.1 子查詢 (Subquery)

# 🎈 概念解釋：
# 子查詢是在查詢內部再進行另一個查詢，可用於複雜的條件判斷

# %% [md]
##### 📌 範例 4.1: 查詢交易金額高於平均值的交易

# %%
query = """
SELECT 
    t.transaction_id,
    t.transaction_date,
    t.amount,
    t.merchant_category,
    u.user_name
FROM transactions t
INNER JOIN cards c ON t.card_id = c.card_id
INNER JOIN users u ON c.user_id = u.user_id
WHERE t.amount > (SELECT AVG(amount) FROM transactions)
    AND t.transaction_status = 'approved'
ORDER BY t.amount DESC
LIMIT 20
"""

result = spark.sql(query)
result.show()

# %% [md]
##### 📌 範例 4.2: 找出擁有最多信用卡的前10名使用者

# %%
query = """
SELECT 
    u.user_id,
    u.user_name,
    u.occupation,
    u.annual_income,
    card_stats.card_count
FROM users u
INNER JOIN (
    SELECT user_id, COUNT(*) as card_count
    FROM cards
    WHERE card_status = 'active'
    GROUP BY user_id
) card_stats ON u.user_id = card_stats.user_id
ORDER BY card_stats.card_count DESC
LIMIT 10
"""

result = spark.sql(query)
result.show()

# %% [md]
#### 4.2 Window Functions (窗口函數)

# 🎈 概念解釋：
# Window Functions 可以在不改變結果集行數的情況下進行聚合計算

# %% [md]
##### 📌 範例 4.3: 計算每位使用者的累積消費金額

# %%
query = """
SELECT 
    u.user_name,
    t.transaction_date,
    t.amount,
    t.merchant_category,
    SUM(t.amount) OVER (
        PARTITION BY u.user_id 
        ORDER BY t.transaction_date
    ) as cumulative_amount
FROM transactions t
INNER JOIN cards c ON t.card_id = c.card_id
INNER JOIN users u ON c.user_id = u.user_id
WHERE t.transaction_status = 'approved'
    AND u.user_id IN (SELECT user_id FROM users LIMIT 3)
ORDER BY u.user_name, t.transaction_date
"""

result = spark.sql(query)
result.show(30)

# %% [md]
##### 📌 範例 4.4: 為每位使用者的交易排名（依金額）

# %%
query = """
SELECT 
    user_name,
    transaction_date,
    amount,
    merchant_category,
    rank
FROM (
    SELECT 
        u.user_name,
        t.transaction_date,
        t.amount,
        t.merchant_category,
        RANK() OVER (
            PARTITION BY u.user_id 
            ORDER BY t.amount DESC
        ) as rank
    FROM transactions t
    INNER JOIN cards c ON t.card_id = c.card_id
    INNER JOIN users u ON c.user_id = u.user_id
    WHERE t.transaction_status = 'approved'
) ranked_transactions
WHERE rank <= 5
ORDER BY user_name, rank
LIMIT 30
"""

result = spark.sql(query)
result.show(30, truncate=False)

# %% [md]
### 📚 單元五：資料分析實戰

#### 5.1 月度消費分析

# 🎈 概念解釋：
# 使用日期函數進行時間序列分析

# %% [md]
##### 📌 範例 5.1: 每月交易統計

# %%
query = """
SELECT 
    YEAR(transaction_date) as year,
    MONTH(transaction_date) as month,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM transactions
WHERE transaction_status = 'approved'
GROUP BY YEAR(transaction_date), MONTH(transaction_date)
ORDER BY year, month
"""

result = spark.sql(query)
result.show(12)

# %% [md]
##### 📌 範例 5.2: 各月份熱門消費類別

# %%
query = """
SELECT 
    YEAR(transaction_date) as year,
    MONTH(transaction_date) as month,
    merchant_category,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount
FROM transactions
WHERE transaction_status = 'approved'
GROUP BY YEAR(transaction_date), MONTH(transaction_date), merchant_category
ORDER BY year, month, total_amount DESC
"""

result = spark.sql(query)
result.show(30)

# %% [md]
#### 5.2 使用者消費行為分析

# 🎈 概念解釋：
# 分析使用者的消費模式與偏好

# %% [md]
##### 📌 範例 5.3: 高消費使用者分析

# %%
query = """
SELECT 
    u.user_id,
    u.user_name,
    u.age,
    u.occupation,
    u.city,
    COUNT(DISTINCT t.transaction_id) as transaction_count,
    SUM(t.amount) as total_spending,
    AVG(t.amount) as avg_transaction_amount,
    COUNT(DISTINCT t.merchant_category) as category_diversity
FROM users u
INNER JOIN cards c ON u.user_id = c.user_id
INNER JOIN transactions t ON c.card_id = t.card_id
WHERE t.transaction_status = 'approved'
GROUP BY u.user_id, u.user_name, u.age, u.occupation, u.city
HAVING SUM(t.amount) > 100000
ORDER BY total_spending DESC
LIMIT 20
"""

result = spark.sql(query)
result.show()

# %% [md]
##### 📌 範例 5.4: 使用者最常消費的類別

# %%
query = """
WITH user_category_spending AS (
    SELECT 
        u.user_id,
        u.user_name,
        t.merchant_category,
        COUNT(*) as transaction_count,
        SUM(t.amount) as category_spending,
        RANK() OVER (
            PARTITION BY u.user_id 
            ORDER BY COUNT(*) DESC
        ) as category_rank
    FROM users u
    INNER JOIN cards c ON u.user_id = c.user_id
    INNER JOIN transactions t ON c.card_id = t.card_id
    WHERE t.transaction_status = 'approved'
    GROUP BY u.user_id, u.user_name, t.merchant_category
)
SELECT 
    user_id,
    user_name,
    merchant_category as favorite_category,
    transaction_count,
    category_spending
FROM user_category_spending
WHERE category_rank = 1
ORDER BY transaction_count DESC
LIMIT 20
"""

result = spark.sql(query)
result.show()

# %% [md]
#### 5.3 異常交易偵測

# 🎈 概念解釋：
# 使用統計方法找出可能的異常交易

# %% [md]
##### 📌 範例 5.5: 找出單筆金額異常高的交易

# %%
query = """
WITH transaction_stats AS (
    SELECT 
        merchant_category,
        AVG(amount) as avg_amount,
        STDDEV(amount) as stddev_amount
    FROM transactions
    WHERE transaction_status = 'approved'
    GROUP BY merchant_category
)
SELECT 
    t.transaction_id,
    t.transaction_date,
    t.merchant_category,
    t.amount,
    u.user_name,
    ROUND(ts.avg_amount, 2) as category_avg,
    ROUND((t.amount - ts.avg_amount) / ts.stddev_amount, 2) as z_score
FROM transactions t
INNER JOIN cards c ON t.card_id = c.card_id
INNER JOIN users u ON c.user_id = u.user_id
INNER JOIN transaction_stats ts ON t.merchant_category = ts.merchant_category
WHERE t.transaction_status = 'approved'
    AND ABS((t.amount - ts.avg_amount) / ts.stddev_amount) > 3
ORDER BY z_score DESC
LIMIT 20
"""

result = spark.sql(query)
result.show()

# %% [md]
##### 📌 範例 5.6: 偵測短時間內的多次交易

# %%
query = """
WITH transaction_intervals AS (
    SELECT 
        t1.transaction_id,
        t1.card_id,
        t1.transaction_date,
        t1.amount,
        t1.merchant_category,
        COUNT(t2.transaction_id) as transactions_within_hour
    FROM transactions t1
    LEFT JOIN transactions t2 
        ON t1.card_id = t2.card_id
        AND t2.transaction_date BETWEEN 
            (t1.transaction_date - INTERVAL 3 HOURS) 
            AND t1.transaction_date
        AND t2.transaction_id != t1.transaction_id
    WHERE t1.transaction_status = 'approved'
    GROUP BY t1.transaction_id, t1.card_id, t1.transaction_date, t1.amount, t1.merchant_category
)
SELECT 
    ti.transaction_id,
    ti.transaction_date,
    ti.amount,
    ti.merchant_category,
    ti.transactions_within_hour,
    u.user_name,
    c.card_type
FROM transaction_intervals ti
INNER JOIN cards c ON ti.card_id = c.card_id
INNER JOIN users u ON c.user_id = u.user_id
WHERE ti.transactions_within_hour >= 3
ORDER BY ti.transactions_within_hour DESC, ti.transaction_date DESC
LIMIT 20
"""

result = spark.sql(query)
result.show(truncate=False)

# %% [md]
### 📚 單元六：PySpark DataFrame API 操作

#### 6.1 DataFrame 基本操作

# 🎈 概念解釋：
# 使用 PySpark DataFrame API 進行資料操作，與 SQL 效果相同但語法不同

# %% [md]
##### 📌 範例 6.1: 使用 DataFrame API 進行篩選與聚合

# %%
# 統計各城市的平均年收入
result = users_spark \
    .groupBy('city') \
    .agg(
        count('*').alias('user_count'),
        avg('annual_income').alias('avg_income'),
        avg('age').alias('avg_age')
    ) \
    .orderBy(desc('avg_income'))

result.show()

# %% [md]
##### 📌 範例 6.2: 使用 DataFrame API 進行 JOIN 操作

# %%
# 查詢交易與使用者資訊
result = transactions_spark \
    .filter(col('transaction_status') == 'approved') \
    .join(cards_spark, 'card_id') \
    .join(users_spark, 'user_id') \
    .select(
        'transaction_id',
        'transaction_date',
        'amount',
        'merchant_category',
        'user_name',
        'city'
    ) \
    .orderBy(desc('transaction_date')) \
    .limit(20)

result.show(truncate=False)

# %% [md]
#### 6.2 Window Functions with DataFrame API

# 🎈 概念解釋：
# 使用 DataFrame API 實現窗口函數

# %% [md]
##### 📌 範例 6.3: 計算每位使用者的消費排名

# %%
# 定義窗口
windowSpec = Window.partitionBy('user_id').orderBy(desc('amount'))

# 計算排名
result = transactions_spark \
    .filter(col('transaction_status') == 'approved') \
    .join(cards_spark, 'card_id') \
    .join(users_spark, 'user_id') \
    .select(
        'user_name',
        'transaction_date',
        'amount',
        'merchant_category',
        rank().over(windowSpec).alias('rank')
    ) \
    .filter(col('rank') <= 5) \
    .orderBy('user_name', 'rank')

result.show(30)

# %% [md]
### 📚 單元七：資料匯出與儲存

#### 7.1 儲存為 Parquet 格式

# 🎈 概念解釋：
# Parquet 是一種列式存儲格式，適合大數據分析

# %% [md]
##### 📌 範例 7.1: 將處理後的資料儲存為 Parquet

# %%
# 建立彙整報表
summary_report = spark.sql("""
    SELECT 
        u.user_id,
        u.user_name,
        u.city,
        u.age,
        u.occupation,
        COUNT(DISTINCT c.card_id) as card_count,
        COUNT(DISTINCT t.transaction_id) as transaction_count,
        COALESCE(SUM(t.amount), 0) as total_spending,
        COALESCE(AVG(t.amount), 0) as avg_transaction_amount
    FROM users u
    LEFT JOIN cards c ON u.user_id = c.user_id AND c.card_status = 'active'
    LEFT JOIN transactions t ON c.card_id = t.card_id AND t.transaction_status = 'approved'
    GROUP BY u.user_id, u.user_name, u.city, u.age, u.occupation
    ORDER BY total_spending DESC
""")

# 顯示結果
summary_report.show(20)

# 儲存為 Parquet（註解掉以避免實際寫入）
# summary_report.write.mode('overwrite').parquet('./output/user_summary_report.parquet')
print("✅ 資料處理完成（儲存已註解）")

# %% [md]
#### 7.2 建立資料視圖

# 🎈 概念解釋：
# 建立可重複使用的資料視圖（View）

# %% [md]
##### 📌 範例 7.2: 建立交易彙總視圖

# %%
# 建立視圖
spark.sql("""
    CREATE OR REPLACE TEMP VIEW transaction_summary AS
    SELECT 
        DATE(t.transaction_date) as transaction_day,
        t.merchant_category,
        COUNT(*) as transaction_count,
        SUM(t.amount) as daily_total,
        AVG(t.amount) as daily_avg
    FROM transactions t
    WHERE t.transaction_status = 'approved'
    GROUP BY DATE(t.transaction_date), t.merchant_category
""")

# 使用視圖查詢
result = spark.sql("""
    SELECT *
    FROM transaction_summary
    WHERE daily_total > 10000
    ORDER BY daily_total DESC
    LIMIT 20
""")

result.show()

# %% [md]
### 📚 單元八：進階分析案例

#### 8.1 RFM 分析（Recency, Frequency, Monetary）

# 🎈 概念解釋：
# RFM 是衡量客戶價值的經典模型，分析最近消費、消費頻率、消費金額

# %% [md]
##### 📌 範例 8.1: 計算使用者 RFM 指標

# %%
query = """
WITH rfm_data AS (
    SELECT 
        u.user_id,
        u.user_name,
        u.city,
        DATEDIFF(CURRENT_DATE(), MAX(t.transaction_date)) as recency,
        COUNT(DISTINCT t.transaction_id) as frequency,
        SUM(t.amount) as monetary
    FROM users u
    INNER JOIN cards c ON u.user_id = c.user_id
    INNER JOIN transactions t ON c.card_id = t.card_id
    WHERE t.transaction_status = 'approved'
    GROUP BY u.user_id, u.user_name, u.city
),
rfm_scores AS (
    SELECT 
        *,
        NTILE(5) OVER (ORDER BY recency ASC) as r_score,
        NTILE(5) OVER (ORDER BY frequency DESC) as f_score,
        NTILE(5) OVER (ORDER BY monetary DESC) as m_score
    FROM rfm_data
)
SELECT 
    user_id,
    user_name,
    city,
    recency,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    (r_score + f_score + m_score) as rfm_total_score,
    CASE 
        WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN '重要價值客戶'
        WHEN r_score >= 4 AND f_score >= 3 THEN '重要發展客戶'
        WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN '重要保持客戶'
        WHEN r_score >= 4 AND m_score >= 4 THEN '重要挽留客戶'
        WHEN r_score <= 2 AND f_score <= 2 THEN '流失客戶'
        ELSE '一般客戶'
    END as customer_segment
FROM rfm_scores
ORDER BY rfm_total_score DESC
LIMIT 30
"""

result = spark.sql(query)
result.show(30, truncate=False)

# %% [md]
##### 📌 範例 8.2: 客戶分群統計

# %%
query = """
WITH rfm_data AS (
    SELECT 
        u.user_id,
        DATEDIFF(CURRENT_DATE(), MAX(t.transaction_date)) as recency,
        COUNT(DISTINCT t.transaction_id) as frequency,
        SUM(t.amount) as monetary
    FROM users u
    INNER JOIN cards c ON u.user_id = c.user_id
    INNER JOIN transactions t ON c.card_id = t.card_id
    WHERE t.transaction_status = 'approved'
    GROUP BY u.user_id
),
rfm_scores AS (
    SELECT 
        *,
        NTILE(5) OVER (ORDER BY recency ASC) as r_score,
        NTILE(5) OVER (ORDER BY frequency DESC) as f_score,
        NTILE(5) OVER (ORDER BY monetary DESC) as m_score
    FROM rfm_data
),
customer_segments AS (
    SELECT 
        user_id,
        CASE 
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN '重要價值客戶'
            WHEN r_score >= 4 AND f_score >= 3 THEN '重要發展客戶'
            WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN '重要保持客戶'
            WHEN r_score >= 4 AND m_score >= 4 THEN '重要挽留客戶'
            WHEN r_score <= 2 AND f_score <= 2 THEN '流失客戶'
            ELSE '一般客戶'
        END as customer_segment
    FROM rfm_scores
)
SELECT 
    customer_segment,
    COUNT(*) as customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM customer_segments
GROUP BY customer_segment
ORDER BY customer_count DESC
"""

result = spark.sql(query)
result.show()

# %% [md]
### 📚 總結與清理

#### 課程重點回顧

# 🎈 本課程涵蓋內容：
# 
# 1. **資料建立**：使用 Numpy、Pandas 建立模擬的信用卡交易資料
# 2. **SQL 基礎**：SELECT、WHERE、GROUP BY、聚合函數
# 3. **JOIN 操作**：INNER JOIN、LEFT JOIN 多表關聯查詢
# 4. **進階查詢**：子查詢、Window Functions、CTE (Common Table Expression)
# 5. **資料分析**：時間序列分析、使用者行為分析、異常偵測
# 6. **DataFrame API**：PySpark DataFrame 操作方法
# 7. **實戰案例**：RFM 客戶價值分析、客戶分群

# %%
# 清理資源
print("📊 資料統計總覽：")
print(f"使用者總數：{users_spark.count()}")
print(f"信用卡總數：{cards_spark.count()}")
print(f"交易總筆數：{transactions_spark.count()}")
print(f"核准交易筆數：{transactions_spark.filter(col('transaction_status') == 'approved').count()}")

# 顯示已建立的臨時表格
print("\n已建立的臨時表格：")
spark.sql("SHOW TABLES").show()

# %%
# 停止 Spark Session（可選）
# spark.stop()
print("\n✅ 課程結束！")
