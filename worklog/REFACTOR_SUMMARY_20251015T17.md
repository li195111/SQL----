# 工作記錄 - 2025年10月15日 17:00

## 任務概述
依照規格更新 AI 協作指令檔案，包含完整的安全政策模板與輔助工具使用說明。

## 變更內容

### 1. 規格文件
- 無變更（本次任務為文件更新）

### 2. AI 協作指令檔案更新

#### `.github/copilot-instructions.md`
- 新增「整合焦點」說明至架構章節
- 更新階段四文件產出與更新章節，加入完整的安全政策模板
- 安全政策模板包含：
  - 範圍 (Scope)
  - 資料分級 (Data Classification)
  - 威脅情境 (Threats)
  - 最低安全基線 (Minimum Security Baseline)
  - 漏洞通報流程 (Vulnerability Disclosure)
  - 法規與準則（PDPA、OWASP ASVS）
  - 回報格式建議

#### `CLAUDE.md`
- 新增「整合焦點」說明至架構章節
- 同步更新完整的安全政策模板至階段四
- 確保與 Copilot 指令檔案內容一致

#### `GEMINI.md`
- 新增「整合焦點」說明至架構章節
- 同步更新完整的安全政策模板至階段四
- 確保與其他 AI 協作指令檔案一致

#### `AGENTS.md`
- 已包含完整的 AI Agents 協作指引
- 無需額外更新（內容已符合規格）

### 3. 測試
- 本次為文件更新，無程式碼變更
- 無需執行單元測試

### 4. 驗證
- 確認所有 AI 協作指令檔案已同步更新
- 確認安全政策模板格式正確且完整
- 確認輔助工具使用說明清晰明確

## 檔案變更清單
- `.github/copilot-instructions.md` - 更新
- `CLAUDE.md` - 更新
- `GEMINI.md` - 更新
- `AGENTS.md` - 已符合規格，無需更新

## 備註
- 所有 AI 協作指令檔案已完成同步
- 安全政策模板遵循 OWASP ASVS 與 RFC 9116 標準
- 工作記錄已建立於 `worklog/REFACTOR_SUMMARY_20251015T17.md`
