# dedupe_internal.py

import pandas as pd

def dedupe_internal(df: pd.DataFrame):
    """
    同一ファイル（1回実行時）内での重複判定＆集約
    ・完全一致（氏名＋メール＋公演名＋URL）は除外
    ・氏名＋メール＋公演名が一致しURLが異なる場合、件数カウント＆公演名追記（順番維持）
    ・氏名＋メール＋公演名が異なる場合は普通に追加
    ※公演名は入力順を維持して連結（"A|B|C"）
    """
    key_cols = ["氏名", "メールアドレス"]
    event_col = "請求公演名"
    url_col = "閲覧用URL"
    
    seen_records = set()
    summary = {}  # (氏名, メール, 公演名) → {件数, 公演名(順序保持), 他カラム}
    order = []  # 入力順にkeyを記録

    for idx, row in df.iterrows():
        key = (row["氏名"], row["メールアドレス"], row["請求公演名"])
        key_noevent = (row["氏名"], row["メールアドレス"])
        url = row[url_col]
        # 完全重複（氏名＋メール＋公演名＋URLが全一致）は無視
        full_key = (row["氏名"], row["メールアドレス"], row["請求公演名"], url)
        if full_key in seen_records:
            continue
        seen_records.add(full_key)

        # 氏名＋メール＋公演名が一致してURLだけ違う場合（カウントアップ＆公演名追記）
        if key in summary:
            summary[key]["件数"] += 1
            # 公演名の連結は順番維持
            # （ただしここではkeyが同じなので公演名追記は不要）
            # 入力順でまとめるためorderも変わらず
        else:
            summary[key] = row.to_dict()
            summary[key]["件数"] = 1
            order.append(key)

    # さらに「氏名＋メール」ごとに公演名集約（AさんでA,B,C公演なら "A|B|C"、件数合算）
    person_event_map = {}
    for key in order:
        name, mail, event = key
        base = summary[key]
        base_events = set([event])
        if (name, mail) not in person_event_map:
            person_event_map[(name, mail)] = {
                "row": base.copy(),
                "event_list": [event],
                "件数": base["件数"]
            }
        else:
            # 別公演なのでイベント追記、件数加算
            person_event_map[(name, mail)]["event_list"].append(event)
            person_event_map[(name, mail)]["件数"] += base["件数"]

    # 出力整形
    output_rows = []
    for val in person_event_map.values():
        r = val["row"]
        r["請求公演名"] = "|".join(val["event_list"])
        r["件数"] = val["件数"]
        output_rows.append(r)
    return pd.DataFrame(output_rows)
