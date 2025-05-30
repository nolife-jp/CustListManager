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

def find_col(df: pd.DataFrame, cand: list[str]) -> str | None:
    for c in cand:
        if c in df.columns:
            return c
    return None

def transform(raw: pd.DataFrame, serial_gen, old_map: dict[str, str]) -> pd.DataFrame:
    from settings import CFG
    COL_MAP = CFG["columns"]
    c_url  = find_col(raw, COL_MAP["url"])
    c_name = find_col(raw, COL_MAP["name"])
    c_mail = find_col(raw, COL_MAP["email"])
    c_tel  = find_col(raw, COL_MAP["tel"])
    c_a1   = find_col(raw, COL_MAP["addr"])
    c_a2   = find_col(raw, COL_MAP["addr_id"])
    c_biko = "備考" if "備考" in raw.columns else None
    if c_mail is None:
        raise ValueError("メールアドレス列が見つかりません。")

    person_serial = old_map.copy()
    rows = []
    for _, r in raw.iterrows():
        # URL必須行のみ処理
        url_val = str(r[c_url]).strip() if c_url else ""
        if not url_val: continue

        name = str(r[c_name]).strip() if c_name else ""
        mail = str(r[c_mail]).strip()
        if not mail:
            continue
        key = f"{name}|{mail}"
        serial = person_serial.get(key) or serial_gen.next()
        person_serial.setdefault(key, serial)
        tel = fix_tel(r[c_tel]) if c_tel else ""
        zip1, addr1 = clean_address(r[c_a1]) if c_a1 else ("", "")
        zip2, addr2 = clean_address(r[c_a2]) if c_a2 else ("", "")
        biko = str(r[c_biko]).strip() if c_biko else ""
        rows.append(dict(
            管理番号=serial,
            氏名=name,
            メールアドレス=mail,
            電話番号=tel,
            郵便番号=zip1,
            登録住所=addr1,
            本人確認登録郵便番号=zip2,
            本人確認登録時住所=addr2,
            請求公演名=r["請求公演名"],
            閲覧用URL=url_val,
            備考=biko,
        ))
    df = pd.DataFrame(rows)
    # 件数集計は「氏名＋メール」のURLありレコードのみ
    df["件数"] = df.groupby(["氏名", "メールアドレス"])["閲覧用URL"].transform("count")
    return df
