# 檔案同步修正總結

## 日期時間
2025-10-16 16:00

## 任務目標
檢查並同步 `2025_pyspark_sql_advanced.py` 和 `2025_pyspark_sql_advanced.ipynb` 兩個檔案，確保 .ipynb 包含所有 .py 中的程式碼，使得測試 .py 檔案即可驗證 .ipynb 的正確性。

## 問題診斷

### 發現的問題
1. **SQL 程式碼被誤放入 Markdown cells**：有 5 個 SQL 查詢被放在 markdown cells 而非 code cells
2. **變數未定義錯誤**：因 SQL 在 markdown 中，導致以下變數未被定義：
   - `sql_correlated` (Cell 31)
   - `sql_join` (Cell 33)
   - `sql_multi_cte` (Cell 24)
   - `sql_self_join` (Cell 42)
   - `pivot_result` (Cell 59)

### 根本原因
在轉換 .py 檔案為 .ipynb 時，某些 `spark.sql()` 程式碼被錯誤地放入 markdown cells 作為範例說明，而不是可執行的 code cells。

## 修正內容

### Cell 31 - 範例 7 相關子查詢
- **問題**：SQL 查詢在 markdown cell 中
- **修正**：轉換為 code cell 並包裝為 `sql_correlated = spark.sql(...)`
- **程式碼**：定義相關子查詢範例

### Cell 33 - 範例 7 JOIN 優化
- **問題**：CTE + JOIN SQL 在 markdown cell 中
- **修正**：轉換為 code cell 並包裝為 `sql_join = spark.sql(...)`
- **程式碼**：使用 JOIN 優化子查詢

### Cell 24 - 範例 6 多層 CTE
- **問題**：多層 CTE SQL 在 markdown cell 中，且後續使用錯誤的變數名 `sql_with_cte`
- **修正**：
  - 轉換為 code cell 並定義 `sql_multi_cte = spark.sql(...)`
  - 修正 Cell 25 將 `sql_with_cte.show()` 改為 `sql_multi_cte.show()`
- **程式碼**：包含 monthly_sales, customer_summary, customer_level 三層 CTE

### Cell 42 - 範例 9 SELF JOIN
- **問題**：SELF JOIN SQL 在 markdown cell 中
- **修正**：轉換為 code cell 並包裝為 `sql_self_join = spark.sql(...)`
- **程式碼**：員工與主管關係查詢

### Cell 59 - 範例 14 PIVOT
- **問題**：PIVOT SQL 在 markdown cell 中
- **修正**：轉換為 code cell 並包裝為 `pivot_result = spark.sql(...)`
- **程式碼**：部門薪資透視表

## 驗證結果

### 變數定義檢查
✅ 所有 17 個關鍵變數都已正確定義：
- employees_df, sql_result, scores_df, sales_df
- orders_df, sql_with_cte, sql_multi_cte
- sql_correlated, sql_join, result_window
- sql_self_join, customers_df, pivot_result
- quarterly_sales, df_null, ntile_result, percent_result

### 範例完整性
✅ 全部 20 個範例都已完整同步

### 語法檢查
✅ .py 檔案編譯成功，無語法錯誤

### 檔案統計
- **.py 檔案**：1012 行，22186 字元
- **.ipynb 檔案**：82 cells（46 code + 36 markdown），程式碼 14311 字元

## 備份
已建立備份檔案：`2025_pyspark_sql_advanced.ipynb.backup`

## 結論
✅ **兩個檔案已完全同步**

現在可以：
1. 執行 `python 2025_pyspark_sql_advanced.py` 進行完整測試
2. 在 Jupyter Notebook 中執行 `2025_pyspark_sql_advanced.ipynb`
3. 測試 .py 檔案即等同於驗證 .ipynb 的正確性

所有程式碼區塊已正確分配到 code cells，變數定義完整，範例齊全。
