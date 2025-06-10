"""
住所・電話番号・カラム名・DataFrameクリーニング等の共通ユーティリティ
"""
import re
import pandas as pd

ZIP_RE = re.compile(r"〒?\s*(\d{3})[-‐−]?(\d{4})")

def clean_address(text: str) -> tuple[str, str]:
    if not isinstance(text, str):
        return "", ""
    txt = text.replace("\r", "").replace("\n", " ").strip()
    m = ZIP_RE.search(txt)
    zipcode = f"〒{m.group(1)}-{m.group(2)}" if m else ""
    if m:
        txt = ZIP_RE.sub("", txt).strip()
    return zipcode, txt

def fix_tel(num: str) -> str:
    t = str(num).strip().replace("-", "")
    if t and t.isdigit() and not t.startswith("0") and len(t) in (10, 11):
        return "0" + t
    return t

def clean_basic(text: str) -> str:
    if not isinstance(text, str):
        return ""
    # 基本的な全角・半角変換、不要な空白・制御文字削除
    return text.replace("\t", "").replace("\r", "").replace("\n", "").strip()

def clean_colname(name):
    """カラム名もクリーニング（clean_basic + strip/tabs/newline除去）"""
    return clean_basic(str(name)).replace('\t', '').replace('\n', '').replace('\r', '').strip()

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        df = df.applymap(clean_basic)
        df.columns = [clean_colname(c) for c in df.columns]
        return df
