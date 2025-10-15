# QChoice AI Collaboration Guidelines for SQL教學文件專案 (AI Agents Overview)

- 使用 `source ~/.zshrc` 來載入環境變數
- 使用 `conda activate fju` 來啟動 Python 環境

## 專案概述 (Project Overview)
本專案為 SQL 與 AI 大數據情境處理教學文件專案，旨在提供完整的 SQL 教學內容與 AI 整合示範。本文件說明各個 AI Agent 的角色分工與協作方式。

## AI Agents 角色與責任 (AI Agents Roles & Responsibilities)

### **GitHub Copilot**
**定位**：程式碼補全與快速開發助手

**核心能力**：
- 即時程式碼建議與自動補全
- 根據註解生成程式碼
- 重構與程式碼優化建議
- 單元測試生成

**適用場景**：
- 撰寫重複性高的程式碼
- 快速實作標準功能
- 產生測試案例
- 程式碼格式化與清理

**使用方式**：
```bash
copilot -p "產生 SQL JOIN 的範例程式碼"
copilot -p "為這個函數建立單元測試"
```

**限制**：
- 不擅長複雜邏輯設計
- 不具備網路搜尋能力
- 不應用於修改或刪除檔案

---

### **Claude**
**定位**：深度分析與架構設計專家

**核心能力**：
- 深度程式碼分析與審查
- 複雜系統架構設計
- 多步驟邏輯推理
- 技術決策權衡分析
- 安全性與效能評估

**適用場景**：
- 設計複雜的系統架構
- 進行全面的程式碼審查
- 分析效能瓶頸與優化方案
- 解決複雜的技術問題
- 評估技術選型

**使用方式**：
```bash
claude -p "分析這段 SQL 查詢的效能瓶頸"
claude -p "設計一個可擴展的資料庫架構"
claude -p "審查這個 API 設計的安全性"
```

**優勢**：
- 深入理解複雜邏輯
- 提供詳細的技術分析
- 整合多領域知識
- 識別潛在風險

---

### **Gemini**
**定位**：資訊搜尋與多模態處理專家

**核心能力**：
- 即時網路搜尋 (`google_web_search`)
- 網頁內容擷取 (`web_fetch`)
- 多模態內容處理（圖片、圖表分析）
- 最新技術資訊查詢

**適用場景**：
- 搜尋最新技術文件
- 查找最佳實踐與設計模式
- 擷取官方文件內容
- 分析資料庫架構圖
- 處理視覺化內容
- 查詢安全漏洞資訊

**使用方式**：
```bash
gemini -p "找出專案裡所有使用 SQL 的地方"
gemini -p 'google_web_search(query="Python SQLAlchemy best practices 2025")'
gemini -p 'web_fetch(prompt="總結 https://docs.sqlalchemy.org/en/20/tutorial/")'
```

**優勢**：
- 即時存取最新資訊
- 多模態內容理解
- 快速資訊整合
- 視覺化內容分析

---

### **Codex**
**定位**：程式碼生成引擎（目前不可用）

**狀態**：目前無法使用

---

## 協作流程 (Collaboration Workflow)

### **任務分配原則**

1. **資訊搜尋階段** → **Gemini**
   - 查詢最新技術文件
   - 搜尋最佳實踐
   - 擷取官方文件內容

2. **架構設計階段** → **Claude**
   - 系統架構設計
   - 技術選型評估
   - 安全性分析

3. **程式碼實作階段** → **Copilot**
   - 快速程式碼補全
   - 標準功能實作
   - 測試案例生成

4. **程式碼審查階段** → **Claude**
   - 深度程式碼審查
   - 效能分析
   - 安全漏洞檢查

5. **問題除錯階段** → **Gemini + Claude**
   - Gemini：搜尋錯誤訊息解決方案
   - Claude：分析根本原因與修正策略

### **協作範例**

#### 範例 1：實作新功能
```bash
# Step 1: Gemini 搜尋最佳實踐
gemini -p 'google_web_search(query="SQLAlchemy relationship best practices")'

# Step 2: Claude 設計架構
claude -p "設計一個支援多對多關係的資料庫模型"

# Step 3: Copilot 實作程式碼
copilot -p "根據上述設計實作 SQLAlchemy 模型"

# Step 4: Claude 審查程式碼
claude -p "審查這個模型的設計，檢查潛在問題"
```

#### 範例 2：效能優化
```bash
# Step 1: Claude 分析效能問題
claude -p "分析這個查詢的效能瓶頸"

# Step 2: Gemini 搜尋優化方案
gemini -p 'google_web_search(query="SQL query optimization techniques 2025")'

# Step 3: Copilot 實作優化
copilot -p "根據建議優化這個 SQL 查詢"
```

#### 範例 3：安全漏洞修補
```bash
# Step 1: Gemini 查詢漏洞資訊
gemini -p 'google_web_search(query="SQLAlchemy SQL injection prevention")'

# Step 2: Claude 評估風險
claude -p "評估我們的程式碼是否存在 SQL 注入風險"

# Step 3: Copilot 實作修正
copilot -p "將字串拼接改為參數化查詢"
```

---

## 開發工作流程 (Development Workflow)

### **核心原則：規格驅動開發 (Specification-Driven Development)**

所有 AI Agents 都必須遵循以下規格驅動的開發流程：

1. **規格更新**（`docs/SPEC.feature` + `api-spec/openapi.yaml`）
2. **實作**
3. **單元測試**
4. **文件更新**（`README.md`、AI 指引文件等）
5. **工作記錄**（`worklog/REFACTOR_SUMMARY_YYYYMMDDTHH.md`）

### **前置步驟：檢查交接事項**

每次開始工作前：
1. 檢查 `worklog/PROGRESS.md` 是否存在
2. 若存在，參考其內容接續開發
3. 完成後清理或標記為已完成

### **工作記錄規範**

- **檔案路徑**：`worklog/REFACTOR_SUMMARY_{YYYYMMDD}T{HH}.md`
- **內容**：簡潔總結規格、程式碼、測試與文件的變動
- **排除**：不記錄 `README.md` 與 AI 指引文件的修改
- **交接**：無法完成時寫入 `worklog/PROGRESS.md`

---

## 命令與輔助工具 (Essential Commands & Helper Tools)

### **開發命令**
```bash
# 啟動 Jupyter Notebook
jupyter notebook

# 執行測試
python -m pytest tests/

# 程式碼品質檢查
pylint *.py

# 格式化程式碼
black *.py
```

### **測試命令**
```bash
# 執行所有測試
python -m pytest

# 測試涵蓋率報告
pytest --cov=. --cov-report=html
```

### **AI 工具使用限制**

**嚴格禁止**：
- 使用任何 CLI 工具修改或刪除專案檔案
- 工具僅用於查詢與討論
- 所有檔案變更必須由 AI Agent 親自執行

---

## 整合模式 (Integration Patterns)

### **資料庫整合**
- 使用 SQLAlchemy ORM
- 連線字串使用環境變數
- 實作連線池優化效能

### **AI 服務整合**
- 統一 API 介面
- 錯誤重試機制
- API 呼叫日誌記錄

### **多 AI 協作模式**
- **串行模式**：依序使用不同 AI（搜尋 → 設計 → 實作 → 審查）
- **並行模式**：同時使用多個 AI 處理不同任務
- **互補模式**：結合不同 AI 的優勢解決複雜問題

---

## 測試要求與策略 (Testing Requirements & Strategy)

### **測試原則**
- 新功能必有單元測試（基於 `docs/SPEC.feature`）
- 測試涵蓋率 ≥ 80%
- 測試通過前不得合併
- 測試失敗必須回滾至實作階段

### **各 AI Agent 在測試中的角色**

- **Copilot**：快速生成測試案例骨架
- **Claude**：設計複雜的測試策略，識別邊界條件
- **Gemini**：搜尋測試最佳實踐，查找測試工具

---

## 文件更新與記錄 (Documentation Update & Work Logging)

### **文件類型**

1. **使用者文件** (`README.md`)
   - 安裝步驟
   - 使用方法
   - 範例程式碼

2. **AI 協作指引**
   - `.github/copilot-instructions.md`
   - `CLAUDE.md`
   - `GEMINI.md`
   - `AGENTS.md`

3. **安全政策** (`docs/SECURITY.md`)
   - 安全基線
   - 漏洞通報流程
   - 法規遵循

4. **工作記錄** (`worklog/`)
   - 每日變更總結
   - 不含 AI 文件摘要

### **各 AI Agent 在文件中的角色**

- **Copilot**：快速生成文件骨架，程式碼範例
- **Claude**：撰寫深度技術文件，架構說明
- **Gemini**：查找文件範本，最新文件標準

---

## 程式碼慣例與最佳實踐 (Coding Conventions & Best Practices)

### **通用原則**
- 規格先行
- 小型提交
- 完善錯誤處理
- 程式碼審查

### **常見陷阱**
- 勿硬編碼金鑰
- 勿雙寫資料庫
- 勿忽略錯誤
- 勿複製貼上

### **Python 規範**
- 遵循 PEP 8
- 使用 Type Hints
- Google Style Docstrings
- 有意義的命名

---

## 重要注意事項與除錯 (Important Notes & Debugging)

### **語言偏好**
- 溝通：繁體中文
- 程式碼：英文
- 文件：中英雙語

### **除錯策略**

1. **Gemini**：搜尋錯誤訊息與解決方案
2. **Claude**：分析根本原因
3. **Copilot**：快速實作修正

### **安全性**
- 避免提交敏感資料
- 使用 `.gitignore`
- 定期更新依賴套件
- Gemini 查詢最新漏洞資訊

---

## 狀態管理 (State Management)

### **應用程式狀態**
- 懶加載優化
- 自動偵測系統偏好
- 狀態變更日誌

### **資料庫狀態**
- Migration 管理
- 環境一致性
- 定期備份

### **快取策略**
- 適當快取機制
- 合理過期時間
- 手動清除機制

---

## AI Agents 協作最佳實踐 (Best Practices for AI Collaboration)

### **選擇合適的 AI Agent**

| 任務類型 | 建議 Agent | 理由 |
|---------|-----------|------|
| 資訊搜尋 | Gemini | 即時網路搜尋能力 |
| 架構設計 | Claude | 深度分析與推理 |
| 程式碼補全 | Copilot | 快速補全與建議 |
| 程式碼審查 | Claude | 全面分析與風險識別 |
| 最新技術 | Gemini | 擷取最新文件 |
| 複雜除錯 | Claude + Gemini | 結合搜尋與分析 |

### **協作溝通範例**

#### 跨 Agent 資訊傳遞
```bash
# Agent 1 (Gemini) 搜尋資訊
gemini -p 'google_web_search(query="SQLAlchemy 2.0 migration guide")'
# 輸出：找到官方遷移指南 URL

# Agent 2 (Claude) 分析並規劃
claude -p "根據 SQLAlchemy 2.0 遷移指南，規劃我們專案的升級步驟"
# 輸出：詳細的升級計畫

# Agent 3 (Copilot) 實作變更
copilot -p "實作 SQLAlchemy 2.0 的語法變更"
# 輸出：更新後的程式碼
```

### **效率最大化技巧**

1. **並行處理**：不同 AI 處理獨立任務
2. **串行優化**：資訊 → 設計 → 實作 → 審查
3. **快速迭代**：使用 Copilot 快速原型，Claude 深度優化
4. **知識共享**：在工作記錄中記錄 AI 協作過程

### **避免的反模式**

❌ 重複查詢相同資訊（浪費 token）  
✅ 在工作記錄中保存查詢結果

❌ 使用不適合的 AI 處理任務  
✅ 根據任務特性選擇合適的 AI

❌ 忽略前置交接檢查  
✅ 每次都檢查 `worklog/PROGRESS.md`

❌ 讓 AI 工具直接修改檔案  
✅ AI 提供建議，由開發者執行變更

---

## 參考文件位置 (Reference Document Locations)

- 規格文件：`docs/SPEC.feature`
- API 規格：`api-spec/openapi.yaml`
- 配置檔案：`config/settings.yaml`
- 測試檔案：`tests/`
- 工作記錄：`worklog/`
- AI 指引：`.github/copilot-instructions.md`, `CLAUDE.md`, `GEMINI.md`, `AGENTS.md`
- 安全政策：`docs/SECURITY.md`

---

## 總結 (Summary)

本專案採用多 AI 協作模式，每個 AI Agent 都有其專長領域：

- **Copilot**：快速開發與程式碼補全
- **Claude**：深度分析與架構設計  
- **Gemini**：資訊搜尋與多模態處理

透過合理的任務分配與協作流程，可以最大化開發效率，確保程式碼品質，並保持文件的完整性與一致性。記住，所有 AI 工具僅用於查詢與建議，實際的檔案變更必須由開發者親自執行。
