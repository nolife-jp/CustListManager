import pandas as pd

def dedupe_external(df_new: pd.DataFrame, df_existing: pd.DataFrame, today_str: str, logger=None):
    """
    既存エクセルとの突合判定＆履歴追記
    ・氏名＋メール＋公演名（部分一致）なら、履歴カラムに「管理番号」記載し純粋に新規レコードとして追加
    ・氏名＋メールで一致、公演名部分一致なしなら、既存レコードの件数アップ＆公演名追記
    """
    if logger:
        logger.info(f"[dedupe_external] 既存件数: {len(df_existing)} 新規件数: {len(df_new)}")

    # 既存の人物・公演名・管理番号一覧を生成
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
            # 部分一致あり＝履歴カラムに管理番号列記して追加
            hist = f"過去に同一人物・公演のレコード有り（管理番号:{'/'.join(match_found)}）"
            r = row.to_dict()
            r["履歴"] = hist
            new_records.append(r)
            if logger:
                logger.debug(f"[dedupe_external] 履歴付与: {name}/{mail}/{event} → {hist}")
        elif no_event_found:
            # 部分一致なし＝既存レコードに追記
            tgt = no_event_found[0]
            for i, erow in df_result.iterrows():
                if erow["氏名"] == name and erow["メールアドレス"] == mail and erow["請求公演名"] == "|".join(tgt["events"]):
                    # 件数加算
                    df_result.at[i, "件数"] = int(erow["件数"]) + int(row["件数"])
                    # 公演名追記（重複しないようマージ）
                    events_merged = sorted(tgt["events"] | event_set)
                    df_result.at[i, "請求公演名"] = "|".join(events_merged)
                    # 履歴追記
                    prev = erow.get("履歴", "")
                    uphist = f"{today_str}:追記"
                    df_result.at[i, "履歴"] = (prev + "|" + uphist) if prev else uphist
                    if logger:
                        logger.debug(f"[dedupe_external] 追記: {name}/{mail}/{event}")
                    break
        else:
            # 氏名＋メール該当なし→新規
            new_records.append(row.to_dict())
            if logger:
                logger.debug(f"[dedupe_external] 新規追加: {name}/{mail}/{event}")

    if new_records:
        df_result = pd.concat([df_result, pd.DataFrame(new_records)], ignore_index=True)
        if logger:
            logger.info(f"[dedupe_external] 新規追加レコード数: {len(new_records)}")

    return df_result
