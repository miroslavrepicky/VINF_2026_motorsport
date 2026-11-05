
import json, collections as C
def rd(p):
    return (json.loads(l) for l in open(p, encoding="utf-8") if l.strip())
def null_bytes(r):                                   # approx. size of '"s__x__y": null, '
    return sum(len(k) + 10 for k, v in r.items() if k.startswith("s__") and v is None)
msm = {d["driver_id"]: d for d in rd("data/drivers.jsonl")}
wiki = {d["page_id"]: d for d in rd("data/wiki_drivers.jsonl")}
tot = nul = 0
for line in open("data/drivers.jsonl", encoding="utf-8"):
    tot += len(line.encode("utf-8")); nul += null_bytes(json.loads(line))
print(f"drivers.jsonl: {tot/1e6:.0f} MB, of that null s__ columns ~{nul/1e6:.0f} MB, {len(msm)} records")
n, size, nulls, bad = C.Counter(), C.Counter(), C.Counter(), C.Counter()
seen_m, seen_w, widths = C.Counter(), C.Counter(), C.Counter()
MSM_FIELDS = ["gender", "tagline", "last_race_slug", "last_race_name", "bio", "seasons"]
for line in open("data/joined_drivers.jsonl", encoding="utf-8"):
    r = json.loads(line); s = r["source"]
    n[s] += 1; size[s] += len(line.encode("utf-8")); widths[len(r)] += 1; nulls[s] += null_bytes(r)
    m = msm.get(r["driver_id"]) if r["driver_id"] else None
    w = wiki.get(r["page_id"]) if r["page_id"] else None
    if r["driver_id"]: seen_m[r["driver_id"]] += 1
    if r["page_id"]: seen_w[r["page_id"]] += 1
    if (r["driver_id"] and m is None) or (r["page_id"] and w is None):
        bad["id_not_in_source"] += 1; continue
    if (s == "wiki_only" and r["driver_id"]) or (s == "msm_only" and r["page_id"]):
        bad["wrong_source_label"] += 1
    if m:
        if any(r[k] != m.get(k) for k in MSM_FIELDS): bad["msm_field_differs"] += 1
        if any(v != m.get(k) for k, v in r.items() if k.startswith("s__")): bad["msm_series_stats_differ"] += 1
    if w and (r["wiki_title"] != w["title"] or r["born_place"] != w.get("born_place")
              or r["co_drivers"] != w.get("co_drivers")): bad["wiki_field_differs"] += 1
    if r["name"] not in (m and m["name"], w and w["name"]): bad["name_from_nowhere"] += 1
    if r["born_date"] not in (m and m["born_date"], w and w["born_date"]): bad["born_date_from_nowhere"] += 1
print("records:", dict(n), "| distinct key counts per line:", dict(widths))
for s in n:
    print(f"  {s}: {size[s]/1e6:.0f} MB, avg {size[s]/n[s]/1e3:.1f} KB/record, null s__ columns ~{nulls[s]/1e6:.0f} MB")
print("MSM drivers missing:", len(msm) - len(seen_m), "| wiki pages missing:", len(wiki) - len(seen_w))
print("duplicated driver_id:", sum(v > 1 for v in seen_m.values()), "| duplicated page_id:", sum(v > 1 for v in seen_w.values()))
print("problems:", dict(bad) or "none")