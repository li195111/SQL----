# QChoice AI Collaboration Guidelines for SQL教學文件專案 (Gemini Version)

## 專案概述 (Project Overview)
本專案為 SQL 與 AI 大數據情境處理教學文件專案，旨在提供完整的 SQL 教學內容與 AI 整合示範。Gemini 在此專案中擔任快速資訊搜尋、多模態內容處理與實時網路查詢的角色。

## 架構 (Architecture)
- **文件結構**：採用模組化設計，區分基礎教學、進階應用與實戰案例
- **配置管理**：使用 YAML 格式配置檔案，確保設定的可讀性與維護性
- **安全性**：教學範例中不包含真實敏感資料，使用環境變數管理所有配置
- **整合焦點**：聚焦於 SQL 與 AI 技術的整合應用，模組化設計便於擴展

## 開發工作流程 (Development Workflow)

### **核心原則：規格驅動開發 (Specification-Driven Development)**

你的首要任務是遵循一個嚴謹的、由規格驅動的開發流程。所有程式碼的修改、測試的建立，以及文件的更新，都必須以 `docs/SPEC.feature` 和 `api-spec/openapi.yaml` 這兩個規格文件為唯一的真相來源 (Single Source of Truth)。

### **主要工作流程 (Primary Workflow)**

每一次的調整請求都必須嚴格遵循以下五個階段的順序。在開始任何階段前，執行以下前置檢查。

#### **前置步驟：檢查交接事項 (Pre-check Handover)**

1. **檢查檔案**：每次開始工作前，應檢查是否有 `worklog/PROGRESS.md` 檔案。
2. **參考交接**：如果檔案存在，則參考其內容，包括目標需求、已完成階段的總結、目前卡住的問題點、下一步行動建議，以及任何必要的上下文資訊，然後接續開發。
3. **清理檔案**：若交接事項已完全處理，則刪除或標記該檔案為已完成，以避免重複處理。

#### **階段一：規格定義與更新 (Specification)**

1. **分析需求**：根據使用者的請求，判斷是新功能還是既有功能的調整。
2. **更新規格文件**：
    *   **功能規格 (`docs/SPEC.feature`)**：更新或新增 Gherkin 格式的功能描述，確保其清晰地定義了使用者場景與預期行為。
    *   **API 規格 (`api-spec/openapi.yaml`)**：同步更新或新增 API 的端點 (endpoints)、請求/回應模型 (schemas)、參數等，確保與 `SPEC.feature` 的描述完全一致。

#### **階段二：程式碼實作與調整 (Implementation)**

1. **實作程式碼**：根據階段一更新後的規格文件，調整應用程式的程式碼，以實現所需功能。
2. **參考既有慣例**：若專案中存在 `.github/copilot-instructions.md`, `CLAUDE.md`, `GEMINI.md`, `AGENTS.md` 檔案，應優先參考其內容，以了解專案的特定慣例與架構決策。

#### **階段三：測試與驗證 (Testing)**

1. **建立或修改單元測試**：
    *   嚴格依照 `docs/SPEC.feature` 及 `api-spec/openapi.yaml` 的規格撰寫單元測試。
    *   確保所有功能，包含成功路徑與邊界條件，都被測試案例完整涵蓋。
2. **執行測試**：**在每次修改完程式碼後，都必須立即執行所有單元測試。**
3. **迭代修正**：如果測試未通過，**必須回到階段二**，繼續調整程式碼，直到所有單元測試都成功通過為止。**在測試完全通過前，不得進入下一個階段。**

#### **階段四：文件產出與更新 (Documentation)**

在確認程式碼與測試都符合規格後，對以下文件進行全面評估與更新：

1. **AI 協作指令 (`.github/copilot-instructions.md`, `CLAUDE.md`, `GEMINI.md`, `AGENTS.md`)**：
    *   **評估**：檢查協作指令的**準確性**及**完整性**，確保其反映了最新的程式碼架構與 API 規格。
    *   **優化**：若有不足，則進行優化或產生新內容。專注於描述專案的宏觀架構、關鍵開發流程、以及 `api-spec/openapi.yaml` 中定義的 API 整合點。
2. **專案說明 (`README.md`)**：同步更新專案說明文件，確保使用者能理解最新的功能與 API 使用方式。
3. **安全政策 (`docs/SECURITY.md`)**：
    *   檢查 `docs/SECURITY.md` 是否存在。
    *   若不存在，則建立該檔案並包含以下內容。若已存在,請確保內容符合最新標準。

    ```markdown
    # 安全政策 (Security Policy)

    ## 範圍 (Scope)
    [請定義此政策涵蓋的專案範圍，例如：API 服務、前端應用程式等]

    ## 資料分級 (Data Classification)
    [請定義資料的敏感度等級，例如：公開、內部、機密]

    ## 威脅情境 (Threats)
    [請列舉主要的威脅情境，例如：未經授權的存取、資料外洩、服務中斷]

    ## 最低安全基線 (Minimum Security Baseline)
    [請定義開發時應遵循的最低安全要求]

    ## 漏洞通報 (Vulnerability Disclosure)
    我們非常重視並感謝所有來自外部研究員的安全回報。
    - **回報管道**：請優先使用 GitHub 的 **Security Advisories** 功能私下回報（若您有權限），或寄信至 **green07111@noreply.github.com**。
    - **我們的承諾**：
        - 我們承諾在 **72 小時內**回覆，確認已收到您的通報。
        - 我們承諾在 **7–30 天**內提供修補或緩解計畫（依據漏洞嚴重度）。
        - 我們採行「協調式揭露」（Coordinated Disclosure）；在修補完成後，我們將與您協調，共同決定公開漏洞細節的時機與方式，並在公告中向您致謝。
    - **快速指引**：為了方便研究員，我們建議在網站根目錄布署 `/.well-known/security.txt` 檔案，以指引通報流程（依據 RFC 9116）。

    ## 法規與準則
    - **法規遵循**：我們的服務設計遵循台灣《個人資料保護法（PDPA）》與其相關子法，確保僅蒐集業務所需資料、履行告知義務，並保障資料主體的權利。
    - **開發基準**：我們的研發流程參考 **OWASP Application Security Verification Standard (ASVS)** 與 **OWASP API/REST Cheat Sheets** 作為安全開發與程式碼撰寫的基準。

    ## 回報格式建議
    為了讓我們能更快地重現並處理問題，您的回報請盡可能包含以下資訊：
    - **影響範圍**：受影響的端點、功能或元件。
    - **重現步驟（POC）**：提供詳細的重現步驟，包含任何必要的設定、程式碼片段或請求範例。
    - **建議修補方向**：若您有任何修補建議，請一併提供。
    - **是否涉及個資**：說明此漏洞是否可能導致個人資料外洩。
    - **是否可被自動化濫用**：評估此漏洞被大規模利用的可能性。

    **提醒**：若您的回報涉及機敏資料，請使用加密附件，並在寄信時先向我們索取公鑰。
    ```

#### **階段五：工作記錄 (Work Log)**

在完成以上所有任務後，將本次對話與調整的過程總結至工作記錄檔案中。

1. **檔案路徑**：`worklog/REFACTOR_SUMMARY_{YYYYMMDD}T{HH}.md`，其中 `YYYYMMDD` 為當前日期（例如：`20251015`），`HH`為24小時制當前時刻（例如：`09`）。
2. **內容**：簡潔地總結本次修改的範圍，包含規格、程式碼、測試與文件的變動。
3. **操作**：如果該日期的檔案已存在，則在檔案末端追加內容；如果不存在，則建立新檔案。
4. **排除規則**：此工作記錄不應包含對 `README.md`、`.github/copilot-instructions.md`, `CLAUDE.md`, `GEMINI.md`, `AGENTS.md` 這些文件的修改摘要。
5. **無法一次完成工作時的交接**：如無法一次完成工作，需將目標需求以及目前階段性總結報告更新或創建寫入 `worklog/PROGRESS.md` 內交接，以便後續 Agents 可以繼續目前工作。內容應包含：原始需求描述、已完成階段的總結、目前卡住的問題點、下一步行動建議，以及任何必要的上下文資訊。

### **工作流程總結**

嚴格遵循 5 步驟規格驅動流程：
1. 規格更新（`docs/SPEC.feature` + `api-spec/openapi.yaml`）
2. 實作
3. 單元測試
4. 文件更新（`README.md`、`.github/copilot-instructions.md`, `CLAUDE.md`, `GEMINI.md`, `AGENTS.md` 等）
5. 工作記錄（`worklog/REFACTOR_SUMMARY_YYYYMMDDTHH.md`）

若無規格變更，從步驟 1 開始；測試失敗則回滾步驟 2。強調「規格先行」、測試必過與文件同步，適用所有變更。

## 命令與輔助工具 (Essential Commands & Helper Tools)

### **開發命令**
```bash
# 啟動 Jupyter Notebook
jupyter notebook

# 執行 Python 測試
python -m pytest tests/

# 檢查程式碼品質
pylint *.py

# 格式化程式碼
black *.py
```

### **測試命令**
```bash
# 執行所有測試
python -m pytest

# 執行特定測試檔案
python -m pytest tests/test_sql.py

# 產生測試涵蓋率報告
pytest --cov=. --cov-report=html
```

### **配置設定**
- 使用 YAML 作為配置檔案格式
- 配置檔案位置：`config/settings.yaml`
- 環境變數優先於配置檔案

### **輔助工具：Gemini 的特殊能力**

作為 Gemini，你具有以下獨特能力：

1. **網路搜尋整合** (`google_web_search`)
   ```python
   # 搜尋最新技術文件
   google_web_search(query="Python SQLAlchemy 2.0 新功能")
   google_web_search(query="SQL 效能優化最佳實踐 2025")
   ```

2. **網頁內容擷取** (`web_fetch`)
   ```python
   # 擷取特定網頁內容並總結
   web_fetch(prompt="總結 https://docs.sqlalchemy.org/en/20/tutorial/ 的重點")
   web_fetch(prompt="從 https://www.postgresql.org/docs/ 提取 JOIN 的說明")
   ```

3. **多模態處理**
   - 分析圖表與視覺化內容
   - 處理包含圖片的教學文件
   - 識別資料庫架構圖

4. **即時資訊查詢**
   - 查詢最新的技術文件
   - 搜尋當前的最佳實踐
   - 尋找最新的安全漏洞資訊

### **與其他 AI 工具協作**

- **Claude** (`claude -p "..."`): 適合深度分析與複雜邏輯設計
  - 範例：`claude -p "分析這段 SQL 查詢的效能瓶頸"`
  - 範例：`claude -p "設計一個可擴展的資料庫架構"`

- **GitHub Copilot** (`copilot -p "..."`): 適合快速程式碼建議與補全
  - 範例：`copilot -p "產生 SQL JOIN 的範例程式碼"`

- **Codex** (`codex -p "..."`): 目前無法使用

**使用限制：**
*   **嚴格禁止**使用任何 CLI 工具來**修改**或**刪除**任何專案檔案。這些工具僅作為查詢和討論的輔助，任何檔案的變更都必須由你親自完成，以避免潛在的嚴重錯誤。

## 整合模式 (Integration Patterns)

### **資料庫整合**
- 使用 SQLAlchemy 作為 ORM 框架
- 連線字串統一管理於環境變數
- 實作連線池以優化效能
- 使用查詢建構器避免 SQL 注入

### **AI 服務整合**
- 使用統一的 API 介面與 AI 服務溝通
- 實作錯誤重試機制
- 記錄所有 API 呼叫以便除錯
- 整合 Gemini API 進行智慧查詢與內容生成

### **Jupyter Notebook 整合**
- 將可重用的程式碼抽取為獨立模組
- 使用 `%load_ext` 載入自訂魔術命令
- 保持 Notebook 的可重現性
- 支援互動式資料視覺化

### **多模態內容處理**
- 整合圖表生成工具（matplotlib, seaborn）
- 支援資料庫架構圖的自動生成
- 處理包含視覺化的教學內容

## 測試要求與策略 (Testing Requirements & Strategy)

### **測試原則**
- 新功能必有單元測試，基於 `docs/SPEC.feature` 規格
- 測試涵蓋率須達 80% 以上
- 支援 watch 模式進行持續測試
- 測試通過前不得合併程式碼

### **測試類型**
1. **單元測試**：測試個別函數與類別
2. **整合測試**：測試資料庫操作與 API 呼叫
3. **範例驗證**：確保教學範例程式碼可正常執行
4. **視覺化測試**：驗證圖表與視覺化輸出的正確性

### **測試驅動原則**
- 確保所有變更後全測試通過
- 測試失敗時必須立即修正
- 不允許跳過或忽略測試
- 使用快照測試驗證複雜輸出

## 文件更新與記錄 (Documentation Update & Work Logging)

### **使用者文件**
- 更新使用者文件 `README.md`
- 確保安裝步驟、使用方法清晰明確
- 提供完整的範例程式碼
- 包含視覺化圖表與說明

### **AI 協作指引**
- 同步更新 AI 指引 `.github/copilot-instructions.md`, `CLAUDE.md`, `GEMINI.md`, `AGENTS.md`
- 反映最新的專案架構與開發流程
- 記錄重要的設計決策

### **工作記錄**
- 每日總結變更至 `worklog/REFACTOR_SUMMARY_YYYYMMDDTHH.md`
- 記錄不含 AI 文件摘要（排除 README.md 與 AI 指引文件）
- 保持記錄的簡潔與可讀性

### **檔案位置參考**
- 規格文件：`docs/SPEC.feature`
- API 規格：`api-spec/openapi.yaml`
- 配置檔案：`config/settings.yaml`
- 測試檔案：`tests/`
- 工作記錄：`worklog/`
- 多模態資源：`assets/images/`, `assets/diagrams/`

## 程式碼慣例與最佳實踐 (Coding Conventions & Best Practices)

### **最佳實踐**
- **規格先行**：任何變更都從更新規格文件開始
- **小型提交**：每次提交專注於單一功能或修正
- **錯誤處理**：所有外部呼叫都必須有適當的錯誤處理機制
- **程式碼審查**：重要變更需經過同儕審查
- **資訊搜尋**：利用 Gemini 的網路搜尋能力查找最新資訊

### **常見陷阱**
- **勿硬編碼金鑰**：所有敏感資訊使用環境變數
- **勿雙寫資料庫**：避免在多處定義相同的資料庫結構
- **勿忽略錯誤**：所有異常都應被捕獲並適當處理
- **勿複製貼上**：重複的程式碼應抽取為共用函數
- **勿過度依賴快取**：確保快取失效機制正確運作

### **程式設計原則**
- 採用功能程式設計風格，優先使用不可變資料結構
- 文件化所有公共 API 與函數
- 跨組件通訊使用明確定義的介面
- 保持函數的單一職責原則
- 使用設計模式解決常見問題

### **Python 編碼規範**
- 遵循 PEP 8 編碼風格
- 使用 Type Hints 增強程式碼可讀性
- Docstrings 使用 Google Style
- 變數命名使用有意義的英文名稱
- 適當使用 async/await 處理 I/O 密集操作

## 重要注意事項與除錯 (Important Notes & Debugging)

### **語言偏好**
- 使用繁體中文進行溝通與撰寫內部文件
- 程式碼註解與變數命名使用英文
- 使用者文件提供中英文雙語版本

### **除錯技巧（Gemini 優勢）**
- 使用 `logging` 模組記錄除錯資訊
- 利用 Python debugger (pdb) 進行互動式除錯
- 檢查 Jupyter Notebook 的 kernel 狀態
- 查看資料庫連線狀態與查詢日誌
- **利用網路搜尋查找錯誤訊息的解決方案**
- **搜尋最新的除錯技術與工具**

### **網路資源查詢範例**
```python
# 查詢錯誤訊息
google_web_search(query="SQLAlchemy IntegrityError 解決方法")

# 搜尋最佳實踐
google_web_search(query="Jupyter Notebook 效能優化 2025")

# 擷取官方文件
web_fetch(prompt="從 https://www.python.org/dev/peps/pep-0008/ 提取命名規範")
```

### **參考文件**
- 優先參考 `.github/copilot-instructions.md`, `CLAUDE.md`, `GEMINI.md`, `AGENTS.md` 了解專案慣例
- 參考 `docs/SPEC.feature` 了解功能需求
- 參考 `api-spec/openapi.yaml` 了解 API 介面
- 使用網路搜尋查找最新的官方文件

### **安全性注意事項**
- 避免敏感資料提交至版本控制系統
- 使用 `.gitignore` 排除敏感檔案
- 環境變數問題可透過 `.env.example` 範本解決
- 定期更新依賴套件以修補安全漏洞
- **使用網路搜尋查詢最新的安全漏洞資訊**

### **環境變數管理**
```bash
# 複製環境變數範本
cp .env.example .env

# 編輯環境變數
vim .env

# 載入環境變數
source .env
```

## 狀態管理 (State Management)

### **應用程式狀態**
- 使用懶加載 (Lazy Loading) 優化資源使用
- 自動偵測系統偏好設定（語言、時區等）
- 狀態變更應觸發相應的日誌記錄
- 使用 Redux 模式管理複雜前端狀態

### **資料庫狀態**
- 使用 Migration 管理資料庫結構變更
- 保持開發、測試、生產環境的一致性
- 定期備份重要資料
- 使用版本控制追蹤 schema 變更

### **快取策略**
- 實作適當的快取機制以提升效能
- 設定合理的快取過期時間
- 提供手動清除快取的機制
- 監控快取命中率

## Gemini 特殊能力與責任

### **即時資訊搜尋**
- 搜尋最新的技術文件與 API 更新
- 查找當前的最佳實踐與設計模式
- 尋找安全漏洞修補資訊
- 搜尋相關的開源專案與解決方案

### **多模態內容處理**
- 分析資料庫架構圖與 ER 圖
- 處理包含視覺化的教學內容
- 識別程式碼截圖中的問題
- 生成資料流程圖

### **內容擷取與總結**
- 從官方文件擷取重點資訊
- 總結技術部落格文章
- 提取程式碼範例與最佳實踐
- 整理多個來源的資訊

### **協作建議**
- 當需要深度分析時，建議使用 Claude
- 當需要快速程式碼補全時，建議使用 Copilot
- 當需要最新資訊時，使用 Gemini 的搜尋能力
- 整合多個 AI 工具的優勢完成複雜任務
