import datetime as dt
import pandas as pd

def merge_person_records(df_person, df_existing, today_str, logger=None):
    """
    - 請求公演名を'|'で連結
    - 件数を加算
    - 更新履歴を追加
    - 退避用の既存レコードを返す
    """
    removed_records = []
    existing_map = {(row["氏名"], row["メールアドレス"]): i for i, row in df_existing.iterrows()}
    for idx, row in df_person.iterrows():
        key = (row["氏名"], row["メールアドレス"])
        if key in existing_map:
            i = existing_map[key]
            exist_row = df_existing.loc[i]
            # 件数加算
            new_cnt = int(exist_row["件数"]) + int(row["件数"])
            # 請求公演名追記（既存＋新規のユニーク和集合、順序維持）
            exist_titles = set(str(exist_row["請求公演名"]).split("|")) if exist_row["請求公演名"] else set()
            new_titles = set(str(row["請求公演名"]).split("|")) if row["請求公演名"] else set()
            all_titles = [t for t in (list(exist_titles) + list(new_titles)) if t]
            merged_titles = "|".join(sorted(set(all_titles)))
            # 更新履歴追加
            prev_upd = str(exist_row.get("更新") or "")
            upd_str = f"{today_str}:追記"
            upd_col = prev_upd + "|" + upd_str if prev_upd else upd_str
            # 更新
            for col in df_person.columns:
                if col == "件数":
                    df_existing.at[i, col] = new_cnt
                elif col == "請求公演名":
                    df_existing.at[i, col] = merged_titles
                elif col == "更新":
                    df_existing.at[i, col] = upd_col
            # 後発分は退避用に保存
            removed_records.append(row.to_dict())
            # 対象を空行にして残さない（後ほどdropで消せる）
            df_person.at[idx, "管理番号"] = None
    # 新規レコードのみ残す
    df_person = df_person[df_person["管理番号"].notnull()]
    return df_person, pd.DataFrame(removed_records)

def export_duplicate_records(df, base_dir="output", today_str=None):
    if df.empty:
        return
    if today_str is None:
        today_str = dt.datetime.today().strftime("%Y-%m-%d_%H%M")
    fname = f"CustList_{today_str}_removed.xlsx"
    out_path = base_dir / fname
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(out_path, index=False)
