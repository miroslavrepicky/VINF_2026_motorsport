"""
doc_stats.py - collects the numbers for the wiki documentation (consultation 3) into ONE short text report.
Stdlib only, every file is read line by line (nothing big is loaded into memory).

    python doc_stats.py                 (run from the project root, where the data/ folder is)
    python doc_stats.py --root <dir>    (the folder that contains data/)

Reads (a missing file is just reported as missing):
    data/drivers.jsonl, data/wiki_drivers.jsonl, data/joined_drivers.jsonl, data/join_links.jsonl
    data/spark/join_stats.json, data/spark/wiki_extract_stats.json, data/spark/extract_stats.json
Writes: data/doc_stats.txt (the same text that is printed). Paste that text to the chat - it is a few KB.
It is a helper for the documentation, not a part of the pipeline: it can be deleted afterwards.
"""
import argparse
import json
import os
import re
import unicodedata
from collections import Counter

EXAMPLES = 4                      # examples per category
BIG_KEYS = {"problems", "error_examples"}   # parts of the stats files that are too long for the report
JOINED_FIELDS = [                 # merged fields whose fill rate is reported
    "full_name", "nationality", "born_date", "died_date", "bio", "gender", "tagline", "seasons", "series_names",
    "born_place", "died_place", "number", "titles", "awards", "relatives", "current_team", "lead", "categories",
    "categorisation", "co_drivers", "teams", "series", "fastest_laps", "championships", "top_tens", "best_pos"]
EXAMPLE_FIELDS = ["name", "full_name", "nationality", "born_date", "died_date", "born_place", "discipline",
                  "wiki_title", "url_wiki", "url_msm", "teams", "series", "co_drivers", "categories",
                  "fastest_laps", "championships", "best_pos", "lead", "bio"]


def read_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def present(v):
    return v is not None and v != "" and v != [] and v != {}


def short(v, n=110):
    s = json.dumps(v, ensure_ascii=False) if not isinstance(v, str) else v
    s = re.sub(r"\s+", " ", s)
    return s if len(s) <= n else s[:n - 1] + "…"


SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "junior", "senior"}
SUB_ARTICLE_RE = re.compile(r"\b(?:early life|career|seasons?|statistics|list of|results)\b", re.IGNORECASE)


def norm_tokens(text):
    t = unicodedata.normalize("NFKD", text or "")
    t = "".join(c for c in t if not unicodedata.combining(c)).lower()
    return re.findall(r"[a-z0-9]+", t)


def surname(text):
    toks = [t for t in norm_tokens(text) if t not in SUFFIXES]
    return toks[-1] if toks else None


def pct(a, b):
    return f"{100 * a / b:.1f} %" if b else "-"


def size_mb(path):
    return f"{os.path.getsize(path) / 1e6:.1f} MB"


def table(counter, total=None, top=None):
    """Counter -> lines 'key: n (x %)', biggest first."""
    total = total if total is not None else sum(counter.values())
    return [f"  {k}: {n} ({pct(n, total)})" for k, n in counter.most_common(top)]


class Report:
    def __init__(self):
        self.lines = []

    def h(self, title):
        self.lines += ["", f"=== {title} ==="]

    def add(self, *lines):
        self.lines += list(lines)


def stats_json(rep, path, title):
    rep.h(title)
    if not os.path.exists(path):
        rep.add("  (file missing)")
        return
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    for k, v in data.items():
        if k in BIG_KEYS:
            rep.add(f"  {k}: ({len(v) if hasattr(v, '__len__') else '?'} items, left out)")
        else:
            rep.add(f"  {k}: {short(v, 300)}")


def msm_section(rep, path):
    rep.h("MSM drivers.jsonl")
    if not os.path.exists(path):
        rep.add("  (file missing)")
        return
    n, c, series_cols, nat = 0, Counter(), set(), Counter()
    years = Counter()
    for d in read_jsonl(path):
        n += 1
        for k in ("nationality", "born_date", "died_date", "bio", "teams", "seasons", "series_names"):
            c[k] += present(d.get(k))
        nat[d.get("nationality")] += 1
        if not series_cols:
            series_cols = {k.split("__")[1] for k in d if k.startswith("s__")}
        if present(d.get("born_date")):
            years[str(d["born_date"])[:3] + "0s"] += 1
    rep.add(f"  size: {size_mb(path)}, drivers: {n}, series (column groups): {len(series_cols)}")
    rep.add("  filled: " + ", ".join(f"{k} {v} ({pct(v, n)})" for k, v in c.items()))
    rep.add("  top nationalities: " + ", ".join(f"{k} {v}" for k, v in nat.most_common(8)))
    rep.add("  born by decade: " + ", ".join(f"{k} {v}" for k, v in sorted(years.items())))


def wiki_section(rep, path):
    rep.h("Wiki wiki_drivers.jsonl")
    if not os.path.exists(path):
        rep.add("  (file missing)")
        return
    n = 0
    reason, disc, box, bprec = Counter(), Counter(), Counter(), Counter()
    fill = Counter()
    fields = ["lead", "series", "nationality", "born_date", "born_place", "died_date", "table_teams",
              "table_series", "co_drivers", "categorisation", "titles", "awards", "relatives", "current_team",
              "former_teams", "number", "birth_name"]
    for r in read_jsonl(path):
        n += 1
        reason[r.get("match_reason")] += 1
        disc[r.get("discipline")] += 1
        box[r.get("infobox_type")] += 1
        bprec[r.get("born_prec")] += 1
        for k in fields:
            fill[k] += present(r.get(k))
    rep.add(f"  size: {size_mb(path)}, records: {n}")
    rep.add("  match_reason:", *table(reason, n))
    rep.add("  discipline:", *table(disc, n))
    rep.add("  born_prec:", *table(bprec, n))
    rep.add("  top infobox_type:", *table(box, n, 8))
    rep.add("  filled: " + ", ".join(f"{k} {v} ({pct(v, n)})" for k, v in fill.items()))


def joined_section(rep, path):
    rep.h("Joined joined_drivers.jsonl")
    if not os.path.exists(path):
        rep.add("  (file missing)")
        return
    n = 0
    source, status, wkind_both, wkind_wo = Counter(), Counter(), Counter(), Counter()
    meth_conf, disc_both, disc_wo, nat_both = Counter(), Counter(), Counter(), Counter()
    pages, msm_ids, page_to_msm = set(), set(), Counter()
    conflict = 0
    fill_both, fill_all = Counter(), Counter()
    n_both = 0
    short_bio_with_lead = 0
    ex = {}                      # category -> examples
    richest = (-1, None)
    suspicious = {}                # (flag, method, confidence) -> [count, examples]

    def keep(cat, r):
        if len(ex.setdefault(cat, [])) < EXAMPLES:
            ex[cat].append(r)

    for r in read_jsonl(path):
        n += 1
        src = r.get("source")
        source[src] += 1
        status[(src, r.get("link_status"))] += 1
        for k in JOINED_FIELDS:
            fill_all[k] += present(r.get(k))
        if src == "both":
            n_both += 1
            meth_conf[(r.get("match_method"), r.get("confidence"))] += 1
            wkind_both[r.get("wiki_kind")] += 1
            disc_both[r.get("discipline")] += 1
            nat_both[r.get("nationality")] += 1
            pages.add(r.get("page_id"))
            msm_ids.add(r.get("driver_id"))
            page_to_msm[r.get("page_id")] += 1
            conflict += bool(r.get("conflict"))
            for k in JOINED_FIELDS:
                fill_both[k] += present(r.get(k))
            if len(r.get("bio") or "") < 150 and present(r.get("lead")):
                short_bio_with_lead += 1
            keep(f"both / {r.get('match_method')} / {r.get('confidence')}", r)
            title = re.sub(r"\s*\([^)]*\)\s*$", "", r.get("wiki_title") or "")
            flags = []
            if surname(title) != surname(r.get("name")):
                flags.append("surname_differs")
            if SUB_ARTICLE_RE.search(title) and not SUB_ARTICLE_RE.search(r.get("name") or ""):
                flags.append("sub_article_title")
            for fl in flags:
                slot = suspicious.setdefault((fl, r.get("match_method"), r.get("confidence")), [0, []])
                slot[0] += 1
                if len(slot[1]) < 8:
                    slot[1].append(r)
            score = sum(present(r.get(k)) for k in JOINED_FIELDS)
            if score > richest[0] and present(r.get("lead")) and present(r.get("co_drivers")):
                richest = (score, r)
        elif src == "wiki_only":
            wkind_wo[r.get("wiki_kind")] += 1
            disc_wo[r.get("discipline")] += 1
            keep("wiki_only", r)
        else:
            keep(f"msm_only / {r.get('link_status')}", r)

    rep.add(f"  size: {size_mb(path)}, records: {n}, fields per record: see JOINED_FIELDS below")
    rep.add("  source:", *table(source, n))
    rep.add("  (source, link_status):", *table(Counter({f"{a} / {b}": v for (a, b), v in status.items()}), n))
    rep.add(f"  unique MSM drivers linked: {len(msm_ids)}, unique wiki pages joined: {len(pages)}, "
            f"records with conflict flag: {conflict}, "
            f"wiki pages shared by >1 MSM driver: {sum(1 for v in page_to_msm.values() if v > 1)}")
    rep.add("  (match_method, confidence) of linked:",
            *table(Counter({f"{a} / {b}": v for (a, b), v in meth_conf.items()}), n_both))
    rep.add("  linked by wiki discipline:", *table(disc_both, n_both))
    rep.add("  linked by wiki_kind:", *table(wkind_both, n_both))
    rep.add("  wiki_only by discipline:", *table(disc_wo))
    rep.add("  wiki_only by wiki_kind:", *table(wkind_wo))
    rep.add("  top nationalities of linked: " + ", ".join(f"{k} {v}" for k, v in nat_both.most_common(8)))
    rep.add(f"  linked with a short bio (<150 chars, so basically only the born/died line) and a wiki lead: "
            f"{short_bio_with_lead}")
    rep.add("  fill rate of merged fields (linked records | all records):")
    for k in JOINED_FIELDS:
        rep.add(f"    {k}: {fill_both[k]} ({pct(fill_both[k], n_both)}) | {fill_all[k]} ({pct(fill_all[k], n)})")

    rep.h("Suspicious links (to review by hand; surname = last word of the MSM name vs. the wiki title)")
    if not suspicious:
        rep.add("  none")
    for (fl, meth, conf), (cnt, rows) in sorted(suspicious.items(), key=lambda kv: -kv[1][0]):
        rep.add(f"  {fl} / {meth} / {conf}: {cnt} of {meth_conf[(meth, conf)]}")
        for r in rows:
            rep.add(f"    {short(r.get('name'), 32)} | {r.get('born_date')} | {short(r.get('nationality'), 14)}"
                    f" -> {short(r.get('wiki_title'), 48)}")

    rep.h("Examples (for the 'Ukážky' tables)")
    for cat in sorted(ex):
        rep.add(f"  -- {cat}")
        for r in ex[cat]:
            rep.add("    " + " | ".join(f"{k}={short(r[k], 70)}" for k in EXAMPLE_FIELDS[:9] if present(r.get(k))))
    if richest[1]:
        rep.add("  -- the richest linked record (for the 'Ukážka' of attributes)")
        for k in EXAMPLE_FIELDS:
            if present(richest[1].get(k)):
                rep.add(f"    {k}: {short(richest[1][k], 160)}")


def links_section(rep, path):
    rep.h("Links join_links.jsonl")
    if not os.path.exists(path):
        rep.add("  (file missing)")
        return
    n = sum(1 for _ in read_jsonl(path))
    rep.add(f"  size: {size_mb(path)}, links: {n}")


def main():
    ap = argparse.ArgumentParser(description="Numbers for the wiki documentation -> data/doc_stats.txt")
    ap.add_argument("--root", default=".", help="folder that contains data/")
    args = ap.parse_args()
    data = os.path.join(args.root, "data")
    rep = Report()
    rep.add("DOC STATS (consultation 3)")
    stats_json(rep, os.path.join(data, "spark", "extract_stats.json"), "html_extractor stats")
    stats_json(rep, os.path.join(data, "spark", "wiki_extract_stats.json"), "wiki_extractor stats")
    stats_json(rep, os.path.join(data, "spark", "join_stats.json"), "join_drivers stats")
    msm_section(rep, os.path.join(data, "drivers.jsonl"))
    wiki_section(rep, os.path.join(data, "wiki_drivers.jsonl"))
    joined_section(rep, os.path.join(data, "joined_drivers.jsonl"))
    links_section(rep, os.path.join(data, "join_links.jsonl"))
    text = "\n".join(rep.lines) + "\n"
    out = os.path.join(data, "doc_stats.txt")
    os.makedirs(data, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        f.write(text)
    print(text)
    print(f"-> saved to {out} ({len(text) / 1024:.1f} KB)")


if __name__ == "__main__":
    main()