from pathlib import Path


BASE_PATH = Path(__file__).resolve().parent.parent.parent
RESOURCE_PATH = BASE_PATH / "resource"
STOPWORDS_PATH = Path(__file__).resolve().parent / "stopwords.txt"
DOC_CNT_CAT_PATH = Path(__file__).resolve().parent.parent / "doc_cnt_cat.json"
