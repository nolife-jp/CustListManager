"""
既存マスター突合、履歴付与、人単位の集約・副産物エクスポート
"""
import pandas as pd
import datetime as dt
from pathlib import Path

def dedupe_external(df_new: pd.DataFrame, df_existing: pd.DataFrame, today_str: str):
    """
    既存エクセルとの突合判定＆履歴追記
    ・氏名＋メール＋公演名（部分一致）なら、履歴カラムに「管理番号」記載し純粋に新規レコードとして追加
    ・氏名＋メールで一致、公演名部分一致なしなら、既存レコードの件数アップ＆公演名追記
    """
    existing_keys = []
    for idx, row in df_existing.iterrows():
        existing_keys.append({
            "name": row["氏名"],
            "mail": row["メールアドレス"],
            "events": set(str(row["請求公演名"]).split("|")),
            "kanri": str(row["管理番号"])
        })

    df_result = df_existing.copy()
    new_records = []
    for idx, row in df_new.iterrows():
        name, mail, event = row["氏名"], row["メールアドレス"], row["請求公演名"]
        event_set = set(str(event).split("|"))
        match_found = []
        no_event_found = []
        for k in existing_keys:
            if k["name"] == name and k["mail"] == mail:
                if k["events"] & event_set:
                    match_found.append(k["kanri"])
                else:
                    no_event_found.append(k)
        if match_found:
            hist = f"過去に同一人物・公演のレコード有り（管理番号:{'/'.join(match_found)}）"
            r = row.to_dict()
            r["履歴"] = hist
            new_records.append(r)
        elif no_event_found:
            tgt = no_event_found[0]
            for i, erow in df_result.iterrows():
                if erow["氏名"] == name and erow["メールアドレス"] == mail and erow["請求公演名"] == "|".join(tgt["events"]):
                    df_result.at[i, "件数"] = int(erow["件数"]) + int(row["件数"])
                    events_merged = sorted(tgt["events"] | event_set)
                    df_result.at[i, "請求公演名"] = "|".join(events_merged)
                    prev = erow.get("履歴", "")
                    uphist = f"{today_str}:追記"
                    df_result.at[i, "履歴"] = (prev + "|" + uphist) if prev else uphist
                    break
        else:
            new_records.append(row.to_dict())
    if new_records:
        df_result = pd.concat([df_result, pd.DataFrame(new_records)], ignore_index=True)
    return df_result

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
            new_cnt = int(exist_row["件数"]) + int(row["件数"])
            exist_titles = set(str(exist_row["請求公演名"]).split("|")) if exist_row["請求公演名"] else set()
            new_titles = set(str(row["請求公演名"]).split("|")) if row["請求公演名"] else set()
            all_titles = [t for t in (list(exist_titles) + list(new_titles)) if t]
            merged_titles = "|".join(sorted(set(all_titles)))
            prev_upd = str(exist_row.get("更新") or "")
            upd_str = f"{today_str}:追記"
            upd_col = prev_upd + "|" + upd_str if prev_upd else upd_str
            for col in df_person.columns:
                if col == "件数":
                    df_existing.at[i, col] = new_cnt
                elif col == "請求公演名":
                    df_existing.at[i, col] = merged_titles
                elif col == "更新":
                    df_existing.at[i, col] = upd_col
            removed_records.append(row.to_dict())
            df_person.at[idx, "管理番号"] = None
    df_person = df_person[df_person["管理番号"].notnull()]
    return df_person, pd.DataFrame(removed_records)

def export_duplicate_records(df, base_dir="output", today_str=None):
    if df.empty:
        return
    if today_str is None:
        today_str = dt.datetime.today().strftime("%Y-%m-%d_%H%M")
    fname = f"CustList_{today_str}_removed.xlsx"
    out_path = Path(base_dir) / fname
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(out_path, index=False)
