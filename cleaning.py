import re
import unicodedata
import pandas as pd

def clean_basic(val):
    """
    プログラム動作阻害要素だけを除去する最低限クリーニング。
    - タブ、CR、LF→半角スペース
    - 制御文字・不可視文字を除去
    - 前後スペースのみstrip
    - Unicode正規化（NFKC）
    """
    if pd.isnull(val):
        return ""
    s = str(val)
    # 制御文字・不可視文字除去
    s = re.sub(r"[\u0000-\u001F\u007F\u200B\uFEFF]", "", s)
    # タブ・CR・LFを半角スペース化
    s = re.sub(r"[\t\r\n]+", " ", s)
    # 前後スペース除去
    s = s.strip()
    # Unicode正規化（例：全角英数→半角、合成文字も統一）
    s = unicodedata.normalize("NFKC", s)
    return s
