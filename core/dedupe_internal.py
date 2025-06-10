import pandas as pd

def dedupe_internal(df: pd.DataFrame, logger=None):
    key_cols = ["氏名", "メールアドレス"]
    event_col = "請求公演名"
    url_col = "閲覧用URL"
    
    seen_records = set()
    summary = {}
    order = []

    for idx, row in df.iterrows():
        key = (row["氏名"], row["メールアドレス"], row["請求公演名"])
        url = row[url_col]
        full_key = (row["氏名"], row["メールアドレス"], row["請求公演名"], url)
        if full_key in seen_records:
            continue
        seen_records.add(full_key)

        if key in summary:
            summary[key]["件数"] += 1
        else:
            summary[key] = row.to_dict()
            summary[key]["件数"] = 1
            order.append(key)

    person_event_map = {}
    for key in order:
        name, mail, event = key
        base = summary[key]
        if (name, mail) not in person_event_map:
            person_event_map[(name, mail)] = {
                "row": base.copy(),
                "event_list": [event],
                "件数": base["件数"]
            }
        else:
            person_event_map[(name, mail)]["event_list"].append(event)
            person_event_map[(name, mail)]["件数"] += base["件数"]

    output_rows = []
    for val in person_event_map.values():
        r = val["row"]
        r["請求公演名"] = "|".join(val["event_list"])
        r["件数"] = str(val["件数"])
        output_rows.append(r)
    df_out = pd.DataFrame(output_rows)
    if "件数" in df_out.columns:
        df_out["件数"] = df_out["件数"].astype(str)
    if logger:
        logger.debug(f"[dedupe_internal] {len(df)}→{len(df_out)}人")
    return df_out
