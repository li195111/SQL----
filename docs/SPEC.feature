# language: zh-TW
功能: PySpark SQL 教學文件與互動式 Notebook
  作為一個 SQL 學習者
  我想要有一個互動式的 PySpark SQL 教學 Notebook
  以便我能夠邊學習邊實作 SQL 的 50 個核心概念

  背景:
    假設 Spark Session 已經成功啟動
    並且 教學環境已經準備就緒

  場景: 建立互動式 Jupyter Notebook 教學文件
    假設 我有一個 Python 教學程式 "2025_pyspark_sql_course.py"
    當 我將其轉換為 Jupyter Notebook 格式
    那麼 應該生成 "2025_pyspark_sql_course.ipynb" 檔案
    並且 Notebook 應該包含所有 50 個 SQL 概念的教學單元

  場景: Notebook 包含基礎查詢操作教學
    假設 我開啟 "2025_pyspark_sql_course.ipynb"
    當 我執行基礎查詢操作的教學單元
    那麼 我應該能學習到 SELECT 操作
    並且 我應該能學習到 INSERT 操作
    並且 我應該能學習到 UPDATE 操作
    並且 我應該能學習到 DELETE 操作
    並且 每個操作都有清楚的範例和 AI Prompt

  場景: Notebook 包含資料庫管理教學
    假設 我開啟 "2025_pyspark_sql_course.ipynb"
    當 我執行資料庫管理的教學單元
    那麼 我應該能學習到 CREATE DATABASE 操作
    並且 我應該能學習到 CREATE TABLE 操作
    並且 我應該能學習到 DROP DATABASE 操作
    並且 我應該能學習到 DROP TABLE 操作
    並且 我應該能學習到 ALTER TABLE 操作
    並且 我應該能學習到 TRUNCATE TABLE 操作

  場景: Notebook 包含 JOIN 操作教學
    假設 我開啟 "2025_pyspark_sql_course.ipynb"
    當 我執行 JOIN 操作的教學單元
    那麼 我應該能學習到 INNER JOIN
    並且 我應該能學習到 LEFT JOIN
    並且 我應該能學習到 RIGHT JOIN
    並且 我應該能學習到 FULL OUTER JOIN
    並且 每個 JOIN 都有實際的學生成績範例

  場景: Notebook 包含聚合函數教學
    假設 我開啟 "2025_pyspark_sql_course.ipynb"
    當 我執行聚合函數的教學單元
    那麼 我應該能學習到 COUNT 函數
    並且 我應該能學習到 SUM 函數
    並且 我應該能學習到 AVG 函數
    並且 我應該能學習到 MIN 函數
    並且 我應該能學習到 MAX 函數
    並且 每個函數都有玩具價格的實際範例

  場景: Notebook 包含進階查詢教學
    假設 我開啟 "2025_pyspark_sql_course.ipynb"
    當 我執行進階查詢的教學單元
    那麼 我應該能學習到 WHERE 條件篩選
    並且 我應該能學習到 GROUP BY 分組
    並且 我應該能學習到 HAVING 分組篩選
    並且 我應該能學習到 ORDER BY 排序
    並且 我應該能學習到 DISTINCT 去重

  場景: Notebook 包含條件邏輯教學
    假設 我開啟 "2025_pyspark_sql_course.ipynb"
    當 我執行條件邏輯的教學單元
    那麼 我應該能學習到 CASE WHEN 語法
    並且 我應該能學習到 COALESCE 函數
    並且 我應該能學習到 BETWEEN 範圍查詢
    並且 我應該能學習到 LIKE 模糊搜尋
    並且 我應該能學習到 IN 清單比對

  場景: Notebook 單元可以獨立執行
    假設 我開啟 "2025_pyspark_sql_course.ipynb"
    當 我選擇任一個教學單元執行
    那麼 該單元應該能夠成功執行
    並且 應該顯示正確的輸出結果
    並且 不應該產生錯誤訊息

  場景: Notebook 包含 AI Prompt 範例
    假設 我開啟 "2025_pyspark_sql_course.ipynb"
    當 我查看任一個 SQL 概念的說明
    那麼 我應該看到該概念的生活化解釋
    並且 我應該看到一個可以提供給 AI 助手的 Prompt 範例
    並且 該 Prompt 應該清楚描述要完成的任務

  場景: 單元測試驗證 Notebook 結構
    假設 單元測試已經建立
    當 執行測試套件
    那麼 應該驗證 Notebook 檔案存在
    並且 應該驗證 Notebook 包含所有必要的單元格
    並且 應該驗證每個 SQL 概念都有對應的教學內容
    並且 應該驗證 AI Prompt 格式正確
    並且 所有測試應該通過

  場景: 教學內容適合初學者
    假設 我是一個 SQL 初學者
    當 我閱讀 Notebook 的任一章節
    那麼 我應該能夠理解概念的解釋
    並且 解釋應該使用生活化的比喻
    並且 程式碼範例應該清楚易懂
    並且 我應該能夠自己嘗試修改範例

  規則:
    * Notebook 必須包含完整的 50 個 SQL 概念
    * 每個概念必須包含：概念解釋、AI Prompt 範例、可執行程式碼
    * 使用繁體中文撰寫說明，程式碼註解使用英文
    * 使用生活化比喻（如玩具、盒子等）解釋 SQL 概念
    * 每個程式碼單元格應該可以獨立執行
    * 必須包含完整的測試覆蓋率
