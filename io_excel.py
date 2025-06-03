import datetime as dt
import shutil
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font, Border
from settings import CFG

def find_col(df: pd.DataFrame, cand: list[str]) -> str | None:
    for c in cand:
        if c in df.columns:
            return c
    return None

def extract_tables(df: pd.DataFrame) -> pd.DataFrame:
    from settings import CFG
    COL_MAP = CFG["columns"]

    # 1. 空行・空列の除去
    df = df.dropna(axis=0, how='all').dropna(axis=1, how='all')
    df = df.reset_index(drop=True)

    print("==== [DEBUG] 読み込んだDataFrame ====")
    print(df)
    print("==== [DEBUG] 各行のA列を順番に表示 ====")
    a_col = df.columns[0]
    for idx, row in df.iterrows():
        print(f"[{idx}] {a_col}: {row[a_col]!r} | 全行: {row.values}")

    blocks = []
    cur_title = ""
    cur_header = None

    for idx, row in df.iterrows():
        a_val = str(row[a_col]).strip() if pd.notna(row[a_col]) else ""
        # タイトル行判定（全カラムのうちNaNが多い場合＝タイトル行とみなす）
        if (
            isinstance(a_val, str)
            and a_val.startswith("【")
            and row.isnull().sum() >= len(row) - 1
        ):
            cur_title = a_val
            print(f"  -> タイトル認定: {cur_title!r}")
            continue
        # header（列名行）
        if a_val == "No.":
            cur_header = row
            print(f"  -> ヘッダー認定: {[str(cell) for cell in row.values]}")
            continue
        # データ行（headerを読んだあと、空でなければOK）
        if cur_header is not None and a_val != "" and a_val != "以上" and not isinstance(a_val, float):
            data_row = row.copy()
            data_row.index = cur_header
            data_row["請求公演名"] = cur_title
            print(f"  -> データ行: {data_row.to_dict()}")
            blocks.append(data_row)

    if not blocks:
        print("==== [DEBUG] blocksが空です ====")
        return pd.DataFrame()
    # 結合・再成形
    out_df = pd.DataFrame(blocks)
    for col in ["請求公演名", "備考"]:
        if col not in out_df.columns:
            out_df[col] = ""
    out_df = out_df.ffill().reset_index(drop=True)
    url_col = find_col(out_df, COL_MAP["url"])
    if url_col:
        out_df = out_df[
            (out_df[url_col].notna()) &
            (out_df[url_col].astype(str).str.strip() != "") &
            (out_df[url_col].astype(str).str.strip() != "以上")
        ]
    # 重複除去：氏名・メールアドレス・閲覧用URL
    key_cols = ["氏名", "メールアドレス", "閲覧用URL"]
    for col in key_cols:
        if col not in out_df.columns:
            out_df[col] = ""
    out_df = out_df.drop_duplicates(subset=key_cols, keep='first').reset_index(drop=True)
    print("==== [DEBUG] 最終出力DataFrame ====")
    print(out_df)
    return out_df

def load_input_excel(path: Path, logger=None) -> pd.DataFrame:
    tables = []
    for sheet, df in pd.read_excel(path, sheet_name=None, header=None, engine="openpyxl").items():
        t = extract_tables(df)
        if not t.empty:
            if logger: logger.info(f"  シート『{sheet}』から {len(t)} 行取り込み")
            tables.append(t)
    return pd.concat(tables, ignore_index=True) if tables else pd.DataFrame()

def style_excel(path: Path, font_name: str):
    wb = load_workbook(path)
    ft = Font(name=font_name)
    nob = Border()
    for ws in wb.worksheets:
        for row in ws.iter_rows():
            for c in row:
                c.font = ft
                c.border = nob
    wb.save(path)

def append_and_save(df_new: pd.DataFrame, serial_gen, logger=None):
    out_xlsx = Path(CFG["paths"]["output_excel"])
    bak_dir  = Path(CFG["paths"]["bak_dir"])
    bak_dir.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y-%m-%d_%H%M")
    if out_xlsx.exists():
        shutil.copy2(out_xlsx, bak_dir / f"bak_{out_xlsx.stem}_{ts}.xlsx")
    # Excel
    df_person = (
        df_new.drop(columns=["閲覧用URL"])
        .drop_duplicates(subset=["氏名", "メールアドレス"])
        .fillna("")
    )
    if "備考" not in df_person.columns:
        df_person["備考"] = ""
    col_order = [
        "管理番号","氏名","メールアドレス","電話番号","郵便番号",
        "登録住所","本人確認登録郵便番号","本人確認登録時住所",
        "請求公演名","件数","備考"
    ]
    for col in col_order:
        if col not in df_person.columns:
            df_person[col] = ""
    df_person = df_person[col_order]
    try:
        # 出力先ディレクトリが無ければ作成
        out_xlsx.parent.mkdir(parents=True, exist_ok=True)
        df_person.to_excel(out_xlsx, index=False)
    except PermissionError:
        if logger:
            logger.error("CustList.xlsx を開いているため書き込めません。閉じてから再実行してください。")
        return
    style_excel(out_xlsx, CFG["excel"]["font_name"])
    # CSV: 閲覧用URL が空でないレコードのみ
    df_csv = df_new.drop(columns=["件数"]) if "件数" in df_new.columns else df_new
    df_csv = df_csv[df_csv["閲覧用URL"].notna() & (df_csv["閲覧用URL"] != "")]
    csv_name = CFG["paths"]["csv_pattern"].replace(
        "{yyyymmdd}", dt.datetime.today().strftime("%Y%m%d_%H%M")
    )
    # CSV出力先ディレクトリも無ければ作成
    Path(csv_name).parent.mkdir(parents=True, exist_ok=True)
    df_csv.to_csv(csv_name, index=False, encoding=CFG["csv"]["encoding"])
    if logger:
        logger.info(f"追記完了：{len(df_person)} 人 / {len(df_csv)} URL")
        logger.info(f"Excel 保存: {out_xlsx}")
        logger.info(f"CSV 出力 : {csv_name}")

def load_person_map(logger=None) -> dict[str, str]:
    p = Path(CFG["paths"]["output_excel"])
    if not p.exists():
        return {}
    df = pd.read_excel(p, engine="openpyxl")
    df["key"] = df["氏名"].astype(str) + "|" + df["メールアドレス"].astype(str)
    return dict(zip(df["key"], df["管理番号"].astype(str)))
