# 工作記錄 - 2025/10/16 12:00

## 📋 任務摘要
為所有 Python 及 Notebook 教學文件添加練習題，並使用 HTML `<details>` 標籤將答案收合。

## 🔧 修改內容

### 1. 修改的文件
- `2024大數據情境處理介紹_SQL與AI.ipynb` - 新增 3 個練習題
- `2025_pyspark_sql_course.ipynb` - 新增 6 個練習題
- `2025_pyspark_sql_advanced.ipynb` - 新增 5 個練習題

### 2. 練習題設計原則
- 每個練習題包含：題目、提示、參考答案
- 使用 `<details>` 和 `<summary>` HTML 標籤實現答案收合功能
- 答案以程式碼區塊格式呈現，包含完整可執行的程式碼
- 練習題放置在相關章節的介紹之後

### 3. 練習題分布

#### 2025_pyspark_sql_course.ipynb
1. 書籍資料 DataFrame 建立與 SELECT 查詢
2. INSERT - 新增資料練習
3. WHERE - 條件篩選練習
4. 員工資料表建立與 Schema 定義
5. ALTER - 新增欄位與條件分級
6. JOIN - 訂單與客戶關聯查詢

#### 2025_pyspark_sql_advanced.ipynb
1. Window Functions - 產品銷售排名
2. LEAD & LAG - 月度銷售環比分析
3. CTE - 客戶消費行為分析
4. CTE - 組織架構遞迴查詢（含 Spark 替代方案）
5. 子查詢優化 - 效能比較練習

### 4. 技術實作細節
- 使用 Python 腳本自動化處理所有 notebook 檔案
- 確保 JSON 格式正確，每行包含適當的換行符號（`\n`）
- 練習題格式統一，使用 emoji 標記（💪）提高可讀性
- 答案收合使用標準 HTML 標籤，確保在 Jupyter Notebook 中正確顯示

### 5. 格式示例
```markdown
## 💪 練習題 1

**題目：**

[練習題描述]

**提示：[提示內容]**

<details>
<summary>📝 點擊查看參考答案</summary>

```python
[完整程式碼]
```

</details>

---
```

## ✅ 驗證結果
- 所有 notebook 成功添加練習題
- 格式正確，包含換行符號
- HTML 收合標籤功能正常
- 程式碼區塊格式完整

## 📊 統計
- 總計新增 14 個練習題
- 涵蓋基礎到進階的 SQL 與 PySpark 操作
- 每個練習題都有完整的參考答案

## 🎯 教學效果提升
- 學員可以在每個章節後立即練習
- 答案收合避免直接看到解答，鼓勵獨立思考
- 提示提供適當引導，降低學習門檻
- 完整的參考答案確保學員可以驗證結果
