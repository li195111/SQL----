# PySpark SQL 教學檔案總覽

## 📚 教學檔案清單

### 基礎課程（適合零基礎學習者）
- **2025_pyspark_sql_course.py** (44K)
  - Python 腳本格式
  - 涵蓋 50 個 SQL 基礎概念
  - 適合直接執行或作為參考

- **2025_pyspark_sql_course.ipynb** (58K)
  - Jupyter Notebook 格式
  - 互動式學習體驗
  - 可逐步執行並查看結果

### 進階課程（適合已有基礎者）
- **2025_pyspark_sql_advanced.py** (29K)
  - Python 腳本格式
  - 涵蓋 20 個進階主題
  - 深入視窗函數、CTE、效能優化

- **2025_pyspark_sql_advanced.ipynb** (38K)
  - Jupyter Notebook 格式
  - 進階主題互動式學習
  - 包含實際應用案例

### 說明文件
- **README_ADVANCED.md** (4.4K)
  - 進階課程詳細說明
  - 學習路徑建議
  - 延伸資源推薦

---

## 🎯 學習路徑建議

### 第一階段：零基礎入門（2-3 週）
使用檔案：`2025_pyspark_sql_course.py/ipynb`

**學習重點：**
1. 基本查詢：SELECT、INSERT、UPDATE、DELETE
2. 資料表管理：CREATE、DROP、ALTER、TRUNCATE
3. 資料連接：INNER JOIN、LEFT JOIN、RIGHT JOIN、FULL OUTER JOIN
4. 資料篩選與排序：WHERE、ORDER BY、GROUP BY、HAVING、DISTINCT
5. 聚合函數：COUNT、SUM、AVG、MIN、MAX
6. 進階查詢：CASE、EXISTS、BETWEEN、LIKE、IN
7. 交易控制：TRANSACTION、COMMIT、ROLLBACK
8. 權限管理：GRANT、REVOKE

**每日學習計畫：**
- 第 1-5 天：基礎查詢操作（範例 1-4）
- 第 6-10 天：資料表管理（範例 5-11）
- 第 11-15 天：連接操作（範例 12-16）
- 第 16-20 天：資料篩選與聚合（範例 17-26）
- 第 21-25 天：進階查詢與函數（範例 27-41）
- 第 26-30 天：交易控制與權限（範例 42-50）

### 第二階段：進階應用（3-4 週）
使用檔案：`2025_pyspark_sql_advanced.py/ipynb`

**學習重點：**
1. 視窗函數精通（ROW_NUMBER、RANK、LEAD、LAG）
2. CTE 與複雜查詢優化
3. 進階 JOIN 技巧（CROSS、SELF、SEMI、ANTI）
4. 效能優化策略
5. 複雜資料轉換
6. 資料品質管控

**每週學習計畫：**
- Week 1：視窗函數（範例 1-4）
  - 掌握基本的視窗函數
  - 練習時間序列分析
  - 實作排名與累計計算

- Week 2：CTE 與子查詢（範例 5-7）
  - 學習 CTE 的使用
  - 理解子查詢優化
  - 重構複雜查詢

- Week 3：進階 JOIN 與優化（範例 8-13）
  - 掌握各種 JOIN 類型
  - 學習 Broadcast Join
  - 實踐快取策略

- Week 4：資料轉換與品質（範例 14-20）
  - 練習 PIVOT/UNPIVOT
  - 處理複雜資料型態
  - 建立資料清理流程

---

## 🚀 快速開始

### 1. 設定環境
```bash
# 啟動 conda 環境
source ~/.zshrc
conda activate fju

# 確認 PySpark 已安裝
python -c "import pyspark; print(pyspark.__version__)"
```

### 2. 執行 Python 腳本
```bash
# 基礎課程
python 2025_pyspark_sql_course.py

# 進階課程
python 2025_pyspark_sql_advanced.py
```

### 3. 使用 Jupyter Notebook
```bash
# 啟動 Jupyter
jupyter notebook

# 開啟對應的 .ipynb 檔案
```

---

## 📊 課程內容對照表

### 基礎課程 (50 個主題)

| 編號 | 主題 | 類別 | 難度 |
|------|------|------|------|
| 1-4 | SELECT、INSERT、UPDATE、DELETE | 基礎操作 | ⭐ |
| 5-6 | CREATE DATABASE、CREATE TABLE | 資料庫管理 | ⭐ |
| 7-11 | DROP、ALTER、TRUNCATE、INDEX | 資料表管理 | ⭐⭐ |
| 12-16 | INNER/LEFT/RIGHT/FULL JOIN、UNION | 資料連接 | ⭐⭐ |
| 17-21 | DISTINCT、WHERE、ORDER BY、GROUP BY、HAVING | 資料篩選 | ⭐⭐ |
| 22-26 | COUNT、SUM、AVG、MIN、MAX | 聚合函數 | ⭐⭐ |
| 27-36 | BETWEEN、LIKE、IN、NOT、NULL、CASE 等 | 進階查詢 | ⭐⭐⭐ |
| 37-41 | EXISTS、ANY、ALL、JOIN、PRIMARY KEY | 進階概念 | ⭐⭐⭐ |
| 42-48 | FOREIGN KEY、CONSTRAINT、TRANSACTION 等 | 資料完整性 | ⭐⭐⭐ |
| 49-50 | GRANT、REVOKE | 權限管理 | ⭐⭐ |

### 進階課程 (20 個主題)

| 編號 | 主題 | 類別 | 難度 |
|------|------|------|------|
| 1-4 | ROW_NUMBER、RANK、LEAD/LAG、累計計算 | 視窗函數 | ⭐⭐⭐ |
| 5-6 | WITH 子句、多層 CTE | CTE | ⭐⭐⭐ |
| 7 | 相關 vs 非相關子查詢 | 子查詢優化 | ⭐⭐⭐⭐ |
| 8-10 | CROSS、SELF、SEMI/ANTI JOIN | 進階 JOIN | ⭐⭐⭐ |
| 11-13 | Broadcast Join、分區、快取 | 效能優化 | ⭐⭐⭐⭐ |
| 14-16 | PIVOT、UNPIVOT、陣列處理 | 資料轉換 | ⭐⭐⭐⭐ |
| 17-18 | NULL 處理、資料去重 | 資料品質 | ⭐⭐⭐ |
| 19-20 | NTILE、PERCENT_RANK | 分析函數 | ⭐⭐⭐ |

---

## 💡 學習技巧

### 1. 實踐為主
- 不要只是閱讀程式碼，一定要自己動手執行
- 修改範例中的參數，觀察結果變化
- 嘗試用不同方法解決同一個問題

### 2. 建立知識地圖
- 記錄每個函數的用途和適用場景
- 整理常見問題的解決方案
- 建立個人的 SQL 速查表

### 3. 對比學習
- 比較 SQL 語法和 DataFrame API 的差異
- 理解何時使用哪種方法更合適
- 測試不同方法的效能差異

### 4. 實際應用
- 用學到的技巧分析真實資料
- 解決工作或專案中的實際問題
- 參與開源專案或資料分析競賽

---

## 🔗 相關資源

### 官方文件
- [PySpark SQL 官方文件](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [PySpark API 參考](https://spark.apache.org/docs/latest/api/python/)

### 推薦閱讀
- Spark: The Definitive Guide
- Learning Spark (2nd Edition)
- High Performance Spark

### 線上資源
- [Stack Overflow - PySpark 標籤](https://stackoverflow.com/questions/tagged/pyspark)
- [Databricks 學習資源](https://www.databricks.com/learn)
- [Spark 效能調校指南](https://spark.apache.org/docs/latest/tuning.html)

---

## 📞 支援與回饋

如有任何問題或建議，歡迎透過以下方式聯繫：
- 使用 AI 助手進行即時問答
- 參考範例中的 AI Prompt 學習如何提問
- 查閱官方文件和社群討論

**祝你學習愉快！** 🎉
