import datetime as dt
import shutil
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font, Border

import cleaning
from settings import CFG

def clean_colname(name):
    """カラム名もクリーニング（clean_basic + strip/tabs/newline除去）"""
    return cleaning.clean_basic(str(name)).replace('\t', '').replace('\n', '').replace('\r', '').strip()

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        df = df.applymap(cleaning.clean_basic)
        df.columns = [clean_colname(c) for c in df.columns]
        return df

def extract_tables_multi(df: pd.DataFrame, logger=None):
    # 複数テーブル抽出用
    tables = []
    idx = 0
    nrows = df.shape[0]
    while idx < nrows:
        # タイトル行（「【」で始まるものを想定）を探す
        title_row = None
        for i in range(idx, nrows):
            row = df.iloc[i].astype(str).tolist()
            if any(cell.strip().startswith("【") for cell in row if isinstance(cell, str)):
                title_row = i
                break
        if title_row is None:
            break  # 残りにタイトルがなければ終了
        # ヘッダー行（No.がある行）を探す
        header_row = None
        for i in range(title_row + 1, nrows):
            row = df.iloc[i].astype(str).tolist()
            if any("No." in cell for cell in row):
                header_row = i
                break
        if header_row is None:
            idx = title_row + 1
            continue  # 次を探す
        # データ部分（次の空行または次のタイトル行まで）
        data_start = header_row + 1
        data_end = nrows
        for i in range(data_start, nrows):
            row = df.iloc[i].astype(str).tolist()
            # 空行またはタイトル行でデータ終了
            if all(cell == "" or cell.lower() == "nan" for cell in row) or any(cell.strip().startswith("【") for cell in row):
                data_end = i
                break
        # カラム名クリーニング
        headers = [clean_colname(h) for h in df.iloc[header_row]]
        # データ部を抽出
        df_data = df.iloc[data_start:data_end].reset_index(drop=True)
        df_data.columns = headers
        # ffill（前方補完）で結合セルの値を全行に展開
        df_data = df_data.ffill()
        # 請求公演名を付与
        title = next(cell.strip() for cell in df.iloc[title_row] if isinstance(cell, str) and cell.strip().startswith("【"))
        df_data["請求公演名"] = title
        # 必須カラムチェック
        must_have = ["氏名", "メールアドレス", "閲覧用URL"]
        if all(col in df_data.columns for col in must_have):
            df_data = clean_dataframe(df_data)
            # 空欄行を除外
            for col in ["氏名", "メールアドレス"]:
                df_data = df_data[~df_data[col].isin([col, "", None, pd.NA])]
            df_data = df_data[
                (df_data["氏名"].astype(str).str.strip() != "") &
                (df_data["メールアドレス"].astype(str).str.strip() != "") &
                (df_data["閲覧用URL"].astype(str).str.strip() != "")
            ]
            tables.append(df_data)
        idx = data_end + 1  # 次のタイトル以降へ進む
    # ログ
    if logger:
        logger.info(f"[extract_tables_multi] 抽出テーブル数: {len(tables)}")
        for i, t in enumerate(tables):
            logger.info(f"  テーブル{i+1}: {len(t)}件, カラム: {list(t.columns)}")
    return tables

def load_input_excel(path: Path, logger=None) -> pd.DataFrame:
    tables = []
    xls = pd.read_excel(path, sheet_name=None, header=None, dtype=str, engine="openpyxl")
    for sheet, df in xls.items():
        if logger:
            logger.info(f"[DEBUG] シート名: {sheet}")
        else:
            print(f"[DEBUG] シート名: {sheet}")
        ts = extract_tables_multi(df, logger)
        tables.extend(ts)
    df_all = pd.concat(tables, ignore_index=True) if tables else pd.DataFrame()
    # ---- ここで入力順を付与 ----
    df_all["_入力順"] = range(len(df_all))
    return df_all

def style_excel(path: Path, font_name: str):
    wb = load_workbook(path)
    ft = Font(name=font_name)
    nob = Border()
    for ws in wb.worksheets:
        for row in ws.iter_rows():
            for c in row:
                c.font = ft
                c.border = nob
                c.number_format = "@"
    wb.save(path)

def append_and_save(df_new: pd.DataFrame, serial_gen, logger=None, overwrite=False):
    out_xlsx = Path(CFG["paths"]["output_excel"])
    bak_dir  = Path(CFG["paths"]["bak_dir"])
    bak_dir.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y-%m-%d_%H%M")
    if out_xlsx.exists():
        shutil.copy2(out_xlsx, bak_dir / f"bak_{out_xlsx.stem}_{ts}.xlsx")

    key_cols = ["氏名", "メールアドレス"]

    if "閲覧用URL" not in df_new.columns:
        df_new["閲覧用URL"] = ""
    # ---- _入力順 が無ければここで付与（追加パッチ）----
    if "_入力順" not in df_new.columns:
        df_new["_入力順"] = range(len(df_new))

    # ★ 原始データ（ffill済）での人×URL件数を出力
    if logger:
        logger.info(f"==== [原始データ:人×URL] レコード数: {len(df_new)} ====")
        logger.info(df_new[[*key_cols, "閲覧用URL"]].head(30))
    else:
        print(f"==== [原始データ:人×URL] レコード数: {len(df_new)} ====")
        print(df_new[[*key_cols, "閲覧用URL"]].head(30))

    # 1. 「人」ごとのユニークなURLセット
    urlset_df = (
        df_new
        .groupby(key_cols)["閲覧用URL"]
        .apply(lambda x: set([v for v in x if pd.notnull(v) and str(v).strip() != ""]))
        .reset_index()
    )
    urlset_df["件数"] = urlset_df["閲覧用URL"].apply(len)
    if logger:
        logger.info("==== [groupbyでユニークURLカウント後] ====")
        logger.info(urlset_df.head(30))
    else:
        print("==== [groupbyでユニークURLカウント後] ====")
        print(urlset_df.head(30))

    # 2. 各「人」の代表情報抽出
    agg_dict = {
        "管理番号": "first",
        "電話番号": "first",
        "郵便番号": "first",
        "登録住所": "first",
        "本人確認登録郵便番号": "first",
        "本人確認登録時住所": "first",
        "請求公演名": "first",
        "備考": "first",
        "_入力順": "first"  # これが重要!!
    }
    df_person = df_new.groupby(key_cols, as_index=False).agg(agg_dict)
    df_person = pd.merge(df_person, urlset_df[[*key_cols, "件数"]], on=key_cols, how="left")
    df_person["件数"] = df_person["件数"].fillna(0).astype(int)

    # ---- ここで入力順でソート ----
    df_person = df_person.sort_values("_入力順").reset_index(drop=True)

    # 管理番号を採番（現行通り）
    for i, row in df_person.iterrows():
        # 既存管理番号が空欄なら新規採番
        if not row.get("管理番号") or str(row["管理番号"]).strip() == "":
            df_person.at[i, "管理番号"] = serial_gen.get_serial(row["氏名"], row["メールアドレス"])

    if logger:
        logger.info("==== [Excel書き出し直前: 個人ユニーク] ====")
        logger.info(df_person[["氏名", "メールアドレス", "件数"]].head(30))
    else:
        print("==== [Excel書き出し直前: 個人ユニーク] ====")
        print(df_person[["氏名", "メールアドレス", "件数"]].head(30))

    col_order = [
        "管理番号", "氏名", "メールアドレス", "電話番号", "郵便番号",
        "登録住所", "本人確認登録郵便番号", "本人確認登録時住所",
        "請求公演名", "件数", "備考"
    ]
    for col in col_order:
        if col not in df_person.columns:
            df_person[col] = ""
    df_person = df_person[col_order]
    df_person = clean_dataframe(df_person).astype(str)

    # 既存Excelとのマージ
    if out_xlsx.exists() and not overwrite:
        try:
            df_existing = pd.read_excel(out_xlsx, engine="openpyxl", dtype=str)
            df_existing = clean_dataframe(df_existing).fillna("").astype(str)
            df_person = pd.concat([df_existing, df_person], ignore_index=True)
            df_person = df_person.drop_duplicates(subset=["氏名", "メールアドレス"], keep='first')
        except Exception as e:
            if logger:
                logger.error(f"既存Excelの読み込みに失敗しました: {e}")

    try:
        out_xlsx.parent.mkdir(parents=True, exist_ok=True)
        df_person.to_excel(out_xlsx, index=False)
    except PermissionError:
        if logger:
            logger.error("CustList.xlsx を開いているため書き込めません。閉じてから再実行してください。")
        return
    style_excel(out_xlsx, CFG["excel"]["font_name"])

    # ===== CSV出力用 DataFrame作成 =====
    if "管理番号" not in df_new.columns:
        df_new["管理番号"] = ""
    person_to_no = dict(zip(
        zip(df_person["氏名"], df_person["メールアドレス"]),
        df_person["管理番号"]
    ))
    df_new["管理番号"] = df_new.apply(lambda row: person_to_no.get((row["氏名"], row["メールアドレス"]), ""), axis=1)
    # 不要なカラム除去
    csv_col_order = [
        "管理番号", "氏名", "メールアドレス", "電話番号", "郵便番号",
        "登録住所", "本人確認登録郵便番号", "本人確認登録時住所",
        "請求公演名", "備考", "閲覧用URL"
    ]
    for col in csv_col_order:
        if col not in df_new.columns:
            df_new[col] = ""
    csv_df = df_new[csv_col_order]

    # ファイル名例：CustList_2025-06-04_2020.csv
    csv_name = CFG["paths"]["csv_pattern"].replace(
        "{yyyymmdd}", dt.datetime.today().strftime("%Y-%m-%d_%H%M")
    )
    Path(csv_name).parent.mkdir(parents=True, exist_ok=True)
    csv_df = clean_dataframe(csv_df).astype(str)
    tmp_csv_name = Path(csv_name).with_name(Path(csv_name).stem + "_tmp.csv")
    try:
        csv_df.to_csv(tmp_csv_name, index=False, encoding=CFG["csv"]["encoding"])
        # 正常終了時のみリネーム
        tmp_csv_name.rename(csv_name)
    except Exception as e:
        if logger:
            logger.error(f"CSV一時ファイルの書き出しに失敗しました。元ファイルは壊れていません。: {e}")
        return

    if logger:
        logger.info(f"追記完了：{len(df_person)} 人 / {len(df_new)} URL")
        logger.info(f"Excel 保存: {out_xlsx}")
        logger.info(f"CSV 出力 : {csv_name}")

def load_person_map(logger=None) -> dict[str, str]:
    p = Path(CFG["paths"]["output_excel"])
    if not p.exists():
        return {}
    df = pd.read_excel(p, engine="openpyxl", dtype=str).fillna("").astype(str)
    df = clean_dataframe(df)
    df["key"] = df["氏名"].astype(str) + "|" + df["メールアドレス"].astype(str)
    return dict(zip(df["key"], df["管理番号"].astype(str)))
