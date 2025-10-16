# PySpark 中文欄位名稱使用指南

## 核心規則

### ✅ 正確用法

1. **在 spark.sql() 中使用反引號**
```python
# 正確 ✅
spark.sql("SELECT `名稱`, `顏色`, `價格` FROM toys")
spark.sql("SELECT * FROM toys WHERE `價格` > 400")
spark.sql("SELECT * FROM toys ORDER BY `價格` DESC")
```

2. **DataFrame API 不需要反引號**
```python
# 正確 ✅
df.select("名稱", "顏色", "價格")
df.filter(col("價格") > 400)
df.orderBy("價格", ascending=False)
```

### ❌ 錯誤用法

```python
# 錯誤 ❌ - 會產生 ParseException
spark.sql("SELECT 名稱, 顏色 FROM toys")
spark.sql("WHERE 價格 > 400")
```

## 常見場景

### SELECT 查詢
```python
# 基本查詢
spark.sql("SELECT `名稱`, `顏色` FROM toys")

# 使用別名
spark.sql("SELECT `名稱` as name, `顏色` as color FROM toys")

# 全部欄位
spark.sql("SELECT * FROM toys")  # 不需反引號
```

### WHERE 條件
```python
spark.sql("SELECT * FROM toys WHERE `價格` > 400")
spark.sql("SELECT * FROM toys WHERE `名稱` = '小熊'")
spark.sql("SELECT * FROM toys WHERE `顏色` IN ('紅色', '藍色')")
```

### ORDER BY 排序
```python
spark.sql("SELECT * FROM toys ORDER BY `價格` DESC")
spark.sql("SELECT * FROM toys ORDER BY `名稱` ASC, `價格` DESC")
```

### GROUP BY 分組
```python
spark.sql("""
    SELECT `顏色`, COUNT(*) as `數量`
    FROM toys
    GROUP BY `顏色`
""")
```

### 聚合函數
```python
spark.sql("SELECT COUNT(*) as `總數` FROM toys")
spark.sql("SELECT SUM(`價格`) as `總價` FROM toys")
spark.sql("SELECT AVG(`價格`) as `平均價` FROM toys")
spark.sql("SELECT MIN(`價格`) as `最低價` FROM toys")
spark.sql("SELECT MAX(`價格`) as `最高價` FROM toys")
```

### JOIN 查詢
```python
spark.sql("""
    SELECT 
        t.`名稱`, 
        m.`公司名稱`, 
        m.`地址`
    FROM toys t
    JOIN manufacturers m ON t.`製造商` = m.id
""")
```

### 子查詢
```python
spark.sql("""
    SELECT `名稱`, `價格`
    FROM toys
    WHERE `價格` > (SELECT AVG(`價格`) FROM toys)
""")
```

### CASE WHEN
```python
spark.sql("""
    SELECT 
        `名稱`,
        `價格`,
        CASE 
            WHEN `價格` > 500 THEN '貴'
            WHEN `價格` > 300 THEN '中等'
            ELSE '便宜'
        END as `價格等級`
    FROM toys
""")
```

## 混合使用範例

```python
# 方式 1：使用 SQL（需要反引號）
toys_df.createOrReplaceTempView("toys")
result = spark.sql("SELECT `名稱`, `顏色` FROM toys WHERE `價格` > 400")

# 方式 2：使用 DataFrame API（不需反引號）
result = toys_df.select("名稱", "顏色").filter(col("價格") > 400)

# 兩種方式結果相同
```

## 錯誤處理

### 常見錯誤訊息
```
ParseException: [PARSE_SYNTAX_ERROR] Syntax error at or near '名'
```

### 解決步驟
1. 找到錯誤的 SQL 語句
2. 確認所有中文欄位名稱
3. 在中文欄位名稱前後加上反引號 `` ` ``
4. 重新執行

### 檢查清單
- [ ] 檢查 SELECT 子句中的中文欄位
- [ ] 檢查 WHERE 條件中的中文欄位
- [ ] 檢查 ORDER BY 中的中文欄位
- [ ] 檢查 GROUP BY 中的中文欄位
- [ ] 檢查聚合函數中的中文欄位
- [ ] 檢查 JOIN 條件中的中文欄位
- [ ] 檢查別名（AS）中的中文名稱

## 最佳實踐建議

### 開發建議
1. **統一使用方式**：團隊內統一使用 SQL 或 DataFrame API
2. **命名規範**：考慮使用英文欄位名避免此問題
3. **程式碼審查**：檢查所有 `spark.sql()` 的中文欄位

### 測試建議
1. 測試前確認所有中文欄位都有反引號
2. 使用小數據集先測試 SQL 語法
3. 建立單元測試確保查詢正確

### 文件建議
1. 在教學文件中明確說明此規則
2. 提供正確和錯誤的對比範例
3. 建立快速參考指南

## 相關資源

- [Apache Spark SQL 文件](https://spark.apache.org/docs/latest/sql-ref.html)
- [PySpark SQL 函數](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- 專案修正紀錄：`worklog/REFACTOR_SUMMARY_20251016T02.md`
