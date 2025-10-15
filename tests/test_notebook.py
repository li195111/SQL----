"""
測試 PySpark SQL 教學 Notebook 的結構和內容

根據 docs/SPEC.feature 和 api-spec/openapi.yaml 規格編寫的單元測試
"""

import unittest
import os
import json
import nbformat
from pathlib import Path


class TestNotebookStructure(unittest.TestCase):
    """測試 Notebook 檔案結構"""
    
    @classmethod
    def setUpClass(cls):
        """設定測試環境"""
        cls.project_root = Path(__file__).parent.parent
        cls.notebook_path = cls.project_root / "2025_pyspark_sql_course.ipynb"
        
        # 載入 Notebook
        if cls.notebook_path.exists():
            with open(cls.notebook_path, 'r', encoding='utf-8') as f:
                cls.notebook = nbformat.read(f, as_version=4)
        else:
            cls.notebook = None
    
    def test_notebook_file_exists(self):
        """測試：Notebook 檔案必須存在"""
        self.assertTrue(
            self.notebook_path.exists(),
            f"Notebook 檔案不存在：{self.notebook_path}"
        )
    
    def test_notebook_format_valid(self):
        """測試：Notebook 格式必須有效"""
        self.assertIsNotNone(self.notebook, "無法載入 Notebook")
        self.assertEqual(self.notebook.nbformat, 4, "Notebook 格式版本應為 4")
    
    def test_notebook_has_minimum_cells(self):
        """測試：Notebook 應該有足夠的單元格（至少 50 個）"""
        self.assertIsNotNone(self.notebook, "Notebook 未載入")
        cell_count = len(self.notebook.cells)
        self.assertGreaterEqual(
            cell_count, 
            50, 
            f"Notebook 單元格數量不足：{cell_count} < 50"
        )
    
    def test_notebook_has_title(self):
        """測試：Notebook 第一個單元格應該是標題"""
        self.assertIsNotNone(self.notebook, "Notebook 未載入")
        self.assertGreater(len(self.notebook.cells), 0, "Notebook 沒有單元格")
        
        first_cell = self.notebook.cells[0]
        self.assertEqual(
            first_cell.cell_type, 
            'markdown', 
            "第一個單元格應該是 markdown 類型"
        )
        
        content = ''.join(first_cell.source)
        self.assertIn('PySpark SQL', content, "標題應包含 'PySpark SQL'")
        self.assertIn('🎈', content, "標題應包含 emoji")
    
    def test_notebook_has_code_cells(self):
        """測試：Notebook 應該包含程式碼單元格"""
        self.assertIsNotNone(self.notebook, "Notebook 未載入")
        
        code_cells = [c for c in self.notebook.cells if c.cell_type == 'code']
        self.assertGreater(
            len(code_cells), 
            0, 
            "Notebook 應該包含至少一個程式碼單元格"
        )
    
    def test_notebook_has_markdown_cells(self):
        """測試：Notebook 應該包含說明文字單元格"""
        self.assertIsNotNone(self.notebook, "Notebook 未載入")
        
        markdown_cells = [c for c in self.notebook.cells if c.cell_type == 'markdown']
        self.assertGreater(
            len(markdown_cells), 
            0, 
            "Notebook 應該包含至少一個 markdown 單元格"
        )
    
    def test_notebook_metadata_exists(self):
        """測試：Notebook 應該有正確的 metadata"""
        self.assertIsNotNone(self.notebook, "Notebook 未載入")
        self.assertIn('metadata', self.notebook, "Notebook 應該有 metadata")
        self.assertIn('kernelspec', self.notebook.metadata, "應該有 kernelspec")
        self.assertIn('language_info', self.notebook.metadata, "應該有 language_info")


class TestSQLConcepts(unittest.TestCase):
    """測試 SQL 概念覆蓋度"""
    
    # 根據需求定義的 50 個 SQL 概念
    REQUIRED_SQL_CONCEPTS = [
        'SELECT', 'INSERT', 'UPDATE', 'DELETE',
        'CREATE DATABASE', 'CREATE TABLE', 'DROP DATABASE', 'DROP TABLE',
        'ALTER TABLE', 'TRUNCATE TABLE',
        'CREATE INDEX', 'DROP INDEX',
        'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL OUTER JOIN',
        'UNION', 'UNION ALL',
        'DISTINCT', 'WHERE', 'ORDER BY', 'GROUP BY', 'HAVING',
        'COUNT', 'SUM', 'AVG', 'MIN', 'MAX',
        'BETWEEN', 'LIKE', 'IN', 'NOT',
        'IS NULL', 'IS NOT NULL',
        'CASE', 'COALESCE',
        'EXISTS', 'ANY', 'SOME', 'ALL',
        'JOIN',
        'PRIMARY KEY', 'FOREIGN KEY', 'CONSTRAINT', 'INDEX',
        'TRANSACTION', 'COMMIT', 'ROLLBACK', 'SAVEPOINT',
        'GRANT', 'REVOKE'
    ]
    
    @classmethod
    def setUpClass(cls):
        """設定測試環境"""
        cls.project_root = Path(__file__).parent.parent
        cls.notebook_path = cls.project_root / "2025_pyspark_sql_course.ipynb"
        
        if cls.notebook_path.exists():
            with open(cls.notebook_path, 'r', encoding='utf-8') as f:
                cls.notebook = nbformat.read(f, as_version=4)
        else:
            cls.notebook = None
    
    def test_all_sql_concepts_covered(self):
        """測試：所有 50 個 SQL 概念都應該被涵蓋"""
        self.assertIsNotNone(self.notebook, "Notebook 未載入")
        
        # 收集所有單元格的內容
        all_content = ''
        for cell in self.notebook.cells:
            all_content += ''.join(cell.source) + '\n'
        
        # 檢查每個概念是否出現
        missing_concepts = []
        for concept in self.REQUIRED_SQL_CONCEPTS:
            # 使用更靈活的搜尋（考慮大小寫和部分匹配）
            if concept.upper() not in all_content.upper():
                missing_concepts.append(concept)
        
        self.assertEqual(
            len(missing_concepts), 
            0, 
            f"以下 SQL 概念未被涵蓋：{missing_concepts}"
        )
    
    def test_select_concept_exists(self):
        """測試：SELECT 概念必須存在"""
        self.assertIsNotNone(self.notebook, "Notebook 未載入")
        
        content = ''
        for cell in self.notebook.cells:
            content += ''.join(cell.source)
        
        self.assertIn('SELECT', content.upper(), "應該包含 SELECT 概念")
    
    def test_join_concepts_exist(self):
        """測試：所有 JOIN 類型都必須存在"""
        self.assertIsNotNone(self.notebook, "Notebook 未載入")
        
        content = ''
        for cell in self.notebook.cells:
            content += ''.join(cell.source)
        
        join_types = ['INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL OUTER JOIN']
        for join_type in join_types:
            self.assertIn(
                join_type, 
                content.upper(), 
                f"應該包含 {join_type} 概念"
            )
    
    def test_aggregate_functions_exist(self):
        """測試：聚合函數都必須存在"""
        self.assertIsNotNone(self.notebook, "Notebook 未載入")
        
        content = ''
        for cell in self.notebook.cells:
            content += ''.join(cell.source)
        
        functions = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']
        for func in functions:
            self.assertIn(
                func, 
                content.upper(), 
                f"應該包含 {func} 函數"
            )


class TestAIPrompts(unittest.TestCase):
    """測試 AI Prompt 的存在和格式"""
    
    @classmethod
    def setUpClass(cls):
        """設定測試環境"""
        cls.project_root = Path(__file__).parent.parent
        cls.notebook_path = cls.project_root / "2025_pyspark_sql_course.ipynb"
        
        if cls.notebook_path.exists():
            with open(cls.notebook_path, 'r', encoding='utf-8') as f:
                cls.notebook = nbformat.read(f, as_version=4)
        else:
            cls.notebook = None
    
    def test_ai_prompts_exist(self):
        """測試：應該包含 AI Prompt 範例"""
        self.assertIsNotNone(self.notebook, "Notebook 未載入")
        
        prompt_count = 0
        for cell in self.notebook.cells:
            content = ''.join(cell.source)
            if '🎯 AI Prompt' in content or 'AI Prompt 範例' in content:
                prompt_count += 1
        
        self.assertGreater(
            prompt_count, 
            10, 
            f"AI Prompt 數量不足：{prompt_count} < 10"
        )
    
    def test_ai_prompts_format(self):
        """測試：AI Prompt 應該有正確的格式"""
        self.assertIsNotNone(self.notebook, "Notebook 未載入")
        
        prompts = []
        for cell in self.notebook.cells:
            content = ''.join(cell.source)
            if '🎯 AI Prompt' in content or 'AI Prompt 範例' in content:
                # 檢查是否包含「請幫我」或類似的提示詞
                if '請幫我' in content or '請寫' in content:
                    prompts.append(content)
        
        self.assertGreater(
            len(prompts), 
            5, 
            "應該有至少 5 個格式正確的 AI Prompt"
        )


class TestExampleCode(unittest.TestCase):
    """測試範例程式碼的存在和基本語法"""
    
    @classmethod
    def setUpClass(cls):
        """設定測試環境"""
        cls.project_root = Path(__file__).parent.parent
        cls.notebook_path = cls.project_root / "2025_pyspark_sql_course.ipynb"
        
        if cls.notebook_path.exists():
            with open(cls.notebook_path, 'r', encoding='utf-8') as f:
                cls.notebook = nbformat.read(f, as_version=4)
        else:
            cls.notebook = None
    
    def test_code_cells_have_content(self):
        """測試：程式碼單元格應該有內容"""
        self.assertIsNotNone(self.notebook, "Notebook 未載入")
        
        code_cells = [c for c in self.notebook.cells if c.cell_type == 'code']
        
        for i, cell in enumerate(code_cells):
            content = ''.join(cell.source).strip()
            self.assertGreater(
                len(content), 
                0, 
                f"程式碼單元格 {i} 沒有內容"
            )
    
    def test_pyspark_imports_exist(self):
        """測試：應該包含 PySpark 的 import 語句"""
        self.assertIsNotNone(self.notebook, "Notebook 未載入")
        
        content = ''
        for cell in self.notebook.cells:
            if cell.cell_type == 'code':
                content += ''.join(cell.source)
        
        self.assertIn('from pyspark', content, "應該 import PySpark")
        self.assertIn('SparkSession', content, "應該使用 SparkSession")
    
    def test_spark_sql_usage(self):
        """測試：應該包含 Spark SQL 的使用"""
        self.assertIsNotNone(self.notebook, "Notebook 未載入")
        
        content = ''
        for cell in self.notebook.cells:
            if cell.cell_type == 'code':
                content += ''.join(cell.source)
        
        self.assertIn('spark.sql', content, "應該使用 spark.sql()")
        self.assertIn('createOrReplaceTempView', content, "應該建立臨時視圖")


class TestChapterStructure(unittest.TestCase):
    """測試章節結構"""
    
    EXPECTED_CHAPTERS = [
        '基礎查詢操作',
        '資料庫和資料表管理',
        '索引管理',
        '資料表連接',
        'JOIN',
        '集合操作',
        '資料篩選與排序',
        '聚合函數',
        '條件與範圍查詢',
        '進階條件邏輯',
        '子查詢',
        '資料表約束',
        '交易控制',
        '權限管理'
    ]
    
    @classmethod
    def setUpClass(cls):
        """設定測試環境"""
        cls.project_root = Path(__file__).parent.parent
        cls.notebook_path = cls.project_root / "2025_pyspark_sql_course.ipynb"
        
        if cls.notebook_path.exists():
            with open(cls.notebook_path, 'r', encoding='utf-8') as f:
                cls.notebook = nbformat.read(f, as_version=4)
        else:
            cls.notebook = None
    
    def test_chapters_exist(self):
        """測試：所有章節都應該存在"""
        self.assertIsNotNone(self.notebook, "Notebook 未載入")
        
        content = ''
        for cell in self.notebook.cells:
            if cell.cell_type == 'markdown':
                content += ''.join(cell.source)
        
        missing_chapters = []
        for chapter in self.EXPECTED_CHAPTERS:
            if chapter not in content:
                missing_chapters.append(chapter)
        
        # 允許某些章節名稱的變化
        self.assertLess(
            len(missing_chapters), 
            5, 
            f"缺少太多章節：{missing_chapters}"
        )


class TestEducationalContent(unittest.TestCase):
    """測試教學內容的品質"""
    
    @classmethod
    def setUpClass(cls):
        """設定測試環境"""
        cls.project_root = Path(__file__).parent.parent
        cls.notebook_path = cls.project_root / "2025_pyspark_sql_course.ipynb"
        
        if cls.notebook_path.exists():
            with open(cls.notebook_path, 'r', encoding='utf-8') as f:
                cls.notebook = nbformat.read(f, as_version=4)
        else:
            cls.notebook = None
    
    def test_has_explanations(self):
        """測試：應該有概念解釋"""
        self.assertIsNotNone(self.notebook, "Notebook 未載入")
        
        explanation_count = 0
        for cell in self.notebook.cells:
            content = ''.join(cell.source)
            if '🎈 概念解釋' in content or '概念解釋' in content:
                explanation_count += 1
        
        self.assertGreater(
            explanation_count, 
            10, 
            f"概念解釋數量不足：{explanation_count} < 10"
        )
    
    def test_uses_simple_metaphors(self):
        """測試：應該使用生活化比喻"""
        self.assertIsNotNone(self.notebook, "Notebook 未載入")
        
        content = ''
        for cell in self.notebook.cells:
            content += ''.join(cell.source)
        
        # 檢查是否使用生活化詞彙
        simple_words = ['玩具', '盒子', '倉庫', '糖果', '朋友']
        found_words = [word for word in simple_words if word in content]
        
        self.assertGreater(
            len(found_words), 
            2, 
            f"應該使用更多生活化比喻，目前只找到：{found_words}"
        )
    
    def test_has_emojis(self):
        """測試：應該使用 emoji 使內容生動"""
        self.assertIsNotNone(self.notebook, "Notebook 未載入")
        
        content = ''
        for cell in self.notebook.cells:
            content += ''.join(cell.source)
        
        # 檢查常見的 emoji
        emojis = ['🎈', '🎯', '📌', '✅', '📚', '🚀']
        found_emojis = [emoji for emoji in emojis if emoji in content]
        
        self.assertGreater(
            len(found_emojis), 
            3, 
            f"應該使用更多 emoji，目前只找到：{found_emojis}"
        )


class TestPythonFile(unittest.TestCase):
    """測試原始 Python 檔案"""
    
    @classmethod
    def setUpClass(cls):
        """設定測試環境"""
        cls.project_root = Path(__file__).parent.parent
        cls.python_path = cls.project_root / "2025_pyspark_sql_course.py"
    
    def test_python_file_exists(self):
        """測試：Python 檔案必須存在"""
        self.assertTrue(
            self.python_path.exists(),
            f"Python 檔案不存在：{self.python_path}"
        )
    
    def test_python_file_has_content(self):
        """測試：Python 檔案應該有內容"""
        self.assertTrue(self.python_path.exists())
        
        with open(self.python_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        self.assertGreater(
            len(content), 
            1000, 
            "Python 檔案內容太少"
        )
    
    def test_python_file_syntax(self):
        """測試：Python 檔案語法應該正確"""
        self.assertTrue(self.python_path.exists())
        
        # 嘗試編譯 Python 檔案
        with open(self.python_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        try:
            compile(content, str(self.python_path), 'exec')
        except SyntaxError as e:
            self.fail(f"Python 檔案有語法錯誤：{e}")


if __name__ == '__main__':
    unittest.main()
