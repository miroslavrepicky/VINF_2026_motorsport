"""
Custom indexer: builds two on-disk inverted indexes from the joined file joined_drivers.jsonl (MSM x Wikipedia,
full outer join made by spark_jobs/join_drivers.py). No IR libraries, no databases, no lemmatization. Stdlib only.
Searching lives in searcher.py.

    python3 indexer.py --joined data/joined_drivers.jsonl --out data/index
    python3 indexer.py --msm-only --out data/index_msm      (the old MSM-only index, for the comparison of both)

One record of the joined file = one document (source = both | msm_only | wiki_only):
  both / msm_only   the MSM record is the base (statistics come from MSM, as decided); a wiki record linked to it adds
                    its own text fields (born_place, died_place, lead, titles, awards, relatives, categories, teams,
                    nationality, series labels, full name = birth name)
  wiki_only         built from the Wikipedia record (statistics from its infobox series blocks; the series id is a
                    synthetic 'w_<label>' because the infobox label is free text)
                    If an MSM driver has no series at all but its wiki page has some, the wiki series are used.

Output:
  <out>/drivers/   1 document = 1 record           (fields: name, full_name, nationality, teams, series, seasons,
                                                    tagline, last_race, bio, gender, born_place, died_place, lead,
                                                    titles, awards, relatives, categories, discipline)
  <out>/series/    1 document = 1 record + series  (fields: name, full_name, nationality, series, gender)

Every field has its OWN inverted index (own df, own idf, own vector lengths):
  postings_<field>.jsonl  {"t": term, "p": [[doc_id, tf], ...]}        tf = occurrences of the term in the field
  terms.tsv               field, term, df, idf_classic, idf_smooth     (all idf values in one place)
  docs.jsonl              document metadata + norms {field: vector length} + max_tf {field: max tf}
  filters.json            auxiliary indexes for filters (team, nationality, series, source, discipline, confidence,
                          gender, statistic ranges)
  meta.json               N, vocabulary sizes, field weights, stopword usage, documents per source, formulas

idf (both from the document frequency df in the given field, N = number of documents):
  classic : idf = log10(N / df)
  smooth  : idf = log10(1 + N / df)
Field weights are NOT applied at build time, only at search time (see meta.json), so they
can be changed without rebuilding the index.

Nationality: the wiki writes 'GBR', 'British (English)', 'AUS'..., MSM 'British'. The nationality filter therefore works
on normalized keys (matching.nat_set: England / UK / GBR -> british); the keys of a document are stored in nat_keys.
"""
import argparse
import datetime
import json
import math
import os
import re
import sys
from collections import Counter, defaultdict

from matching import nat_set
from tokenizer import normalize, tokenize

# field weight at search time: document score = sum of weight_of_field * score_in_field
DRIVER_WEIGHTS = {
    "name": 5.0, "full_name": 3.0, "nationality": 3.0, "teams": 2.0,
    "series": 2.0, "seasons": 2.0, "tagline": 2.0, "last_race": 1.0, "bio": 1.0, "gender": 2.0,
    "born_place": 2.0, "died_place": 1.0, "lead": 1.0, "titles": 2.0, "awards": 1.0, "relatives": 1.0,
    "categories": 1.0, "discipline": 2.0,
}
SERIES_WEIGHTS = {"name": 5.0, "full_name": 3.0, "nationality": 3.0, "series": 5.0, "gender": 2.0}
# gender / discipline are indexed as text together with common words, so a query like "female" / "motorcycle" finds them
GENDER_TEXT = {"male": "male man men", "female": "female woman women"}
DISCIPLINE_TEXT = {"car": "car cars", "motorcycle": "motorcycle motorcycles motorbike bike",
                   "both": "car cars motorcycle motorcycles motorbike bike"}
STATS = ("starts", "wins", "poles", "podiums")
COL_PREFIX = "s__"
YEAR_RE = re.compile(r"\b(1[89]\d\d|20\d\d)\b")
PRESENT_RE = re.compile(r"present|current", re.IGNORECASE)


def read_jsonl(path):
    """Yield one parsed JSON object per non-empty line of a JSONL file."""
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def idf_classic(n, df):
    """Classic idf: log10(N / df)."""
    return math.log10(n / df)


def idf_smooth(n, df):
    """Smoothed idf: log10(1 + N / df); always positive, even for terms present in every document."""
    return math.log10(1 + n / df)


# ---------- series of a record ----------
def msm_series(msm):
    """(series_names, {series_id: {starts, wins, poles, podiums, years}}) from the wide columns of an MSM record.
    In the joined file series_names has a field for EVERY series (null where the driver did not take part)."""
    names = {sid: n for sid, n in (msm.get("series_names") or {}).items() if n}
    rows = {}
    for sid in names:
        row = {st: msm.get(f"{COL_PREFIX}{sid}__{st}") for st in STATS}
        row["years"] = msm.get(f"{COL_PREFIX}{sid}__years") or []
        rows[sid] = row
    return names, rows


def parse_years(text):
    """'1950-1958' -> [1950..1958]; '1960' -> [1960]; 'since 1995 - present' -> [1995..this year]; no year -> []."""
    text = str(text or "")
    ys = [int(y) for y in YEAR_RE.findall(text)]
    if ys and PRESENT_RE.search(text):
        ys.append(datetime.date.today().year)
    if not ys:
        return []
    return list(range(min(ys), max(ys) + 1))


def wiki_series(wiki):
    """Series blocks of the infobox -> (names, rows) in the same shape as msm_series; the id is 'w_<label>'."""
    names, rows = {}, {}
    for b in wiki.get("series") or []:
        label = (b.get("series") or "").strip()
        if not label:
            continue
        sid = "w_" + re.sub(r"[^a-z0-9]+", "_", normalize(label)).strip("_")
        row = rows.setdefault(sid, {**{st: None for st in STATS}, "years": []})
        names.setdefault(sid, label)
        for st in STATS:
            if row[st] is None and isinstance(b.get(st), int):
                row[st] = b[st]
        if not row["years"]:
            row["years"] = parse_years(b.get("years"))
    return names, rows


def series_stats(row):
    """Pick the statistics and the list of years of one series row."""
    return {**{k: row.get(k) for k in STATS}, "years": row.get("years") or []}


def career_totals(rows):
    """Sum of the statistics across all series. If no series has a value, the total is null."""
    out = {}
    for stat in STATS:
        vals = [r[stat] for r in rows if r.get(stat) is not None]
        out[stat] = sum(vals) if vals else None
    return out


# ---------- teams ----------
def wiki_teams(wiki):
    """current_team / former_teams are texts like 'Ferrari; McLaren (1990-1993)' -> ['Ferrari', 'McLaren']."""
    out = []
    for key in ("current_team", "former_teams"):
        for t in re.split(r";", wiki.get(key) or ""):
            t = re.sub(r"\s*\([^)]*\)", "", t).strip(" ,")
            if t:
                out.append(t)
    return out


def merge_teams(*lists):
    """Union of team names; a team that is already there (the same normalized name) is not added again."""
    seen, out = set(), []
    for lst in lists:
        for t in lst:
            k = normalize(t).strip()
            if k and k not in seen:
                seen.add(k)
                out.append(t)
    return out


# ---------- documents ----------
def field_counters(fields, use_stopwords):
    """{field: Counter(term -> tf)}; empty fields are left out."""
    out = {}
    for field, text in fields.items():
        c = Counter(tokenize(text, use_stopwords))
        if c:
            out[field] = c
    return out


def build_record(rec, msm_only):
    """One joined record -> (base metadata, series names, series rows, driver fields) or None when it is skipped."""
    msm = rec.get("msm")
    wiki = None if msm_only else rec.get("wiki")
    if msm is None and wiki is None:
        return None
    m, w = msm or {}, wiki or {}

    # statistics: MSM (as decided); only an MSM driver without any series takes the wiki series
    m_names, m_rows = msm_series(m) if msm else ({}, {})
    w_names, w_rows = wiki_series(w) if wiki else ({}, {})
    names, rows = (m_names, m_rows) if m_rows else (w_names, w_rows)

    teams = merge_teams(m.get("teams") or [], wiki_teams(w) if wiki else [])
    nat_keys = sorted(nat_set(m.get("nationality")) | nat_set(w.get("nationality")))
    source = ("both" if wiki else "msm_only") if msm else "wiki_only"
    discipline = w.get("discipline") if wiki else None
    base = {
        "driver_id": m["driver_id"] if msm else f"wiki-{w['page_id']}",
        "record_id": rec.get("record_id"),
        "source": source,
        "name": m.get("name") or w.get("name"),
        "url": rec.get("url_msm") or rec.get("url_wiki"),
        "wiki_url": rec.get("url_wiki") if wiki else None,
        "nationality": m.get("nationality") or w.get("nationality"),
        "gender": m.get("gender"),
        "born_date": rec.get("born_date") if wiki else m.get("born_date"),
        "died_date": rec.get("died_date") if wiki else m.get("died_date"),
        "teams": teams,
        "discipline": discipline,
        "confidence": rec.get("confidence") if wiki and msm else None,
        "match_method": rec.get("match_method") if wiki and msm else None,
        "nat_keys": nat_keys,
    }
    full_names = [x for x in (m.get("full_name"), w.get("birth_name")) if x]
    all_series = list(names.values()) + [n for sid, n in w_names.items() if sid not in names]
    fields = {
        "name": base["name"] or "",
        "full_name": " ".join(dict.fromkeys(full_names)),
        "nationality": " ".join(x for x in dict.fromkeys([m.get("nationality"), w.get("nationality")]) if x),
        "teams": " ".join(teams),
        "series": " ".join(all_series),
        "seasons": " ".join(m.get("seasons") or []),
        "tagline": m.get("tagline") or "",
        "last_race": m.get("last_race_name") or "",
        "bio": m.get("bio") or "",
        "gender": GENDER_TEXT.get((m.get("gender") or "").lower(), ""),
        "born_place": w.get("born_place") or "",
        "died_place": w.get("died_place") or "",
        "lead": w.get("lead") or "",
        "titles": w.get("titles") or "",
        "awards": w.get("awards") or "",
        "relatives": w.get("relatives") or "",
        "categories": " ".join(w.get("categories") or []),
        "discipline": DISCIPLINE_TEXT.get(discipline or "", ""),
    }
    return base, names, rows, fields


# ---------- filters ----------
def build_filters(docs):
    """
    Auxiliary inverted indexes for filters (so filtering does not have to scan all documents):
      teams / series_tokens : word -> [doc_id]
      nationality           : normalized nationality key (british, american...) -> [doc_id]
      series_id / gender / source / discipline / confidence : value -> [doc_id]
      stats : "<career|series_id>.<stat>" -> [[value, doc_id], ...] sorted by value
              (a range query > >= < <= = is answered with binary search)
    """
    teams, nat, sid_idx, stok, gender, source, discipline, confidence = (defaultdict(list) for _ in range(8))
    stats = defaultdict(list)
    for d in docs:
        i = d["id"]
        for tok in sorted({t for team in d["teams"] for t in tokenize(team, False)}):
            teams[tok].append(i)
        for k in d["nat_keys"]:
            nat[k].append(i)
        if d.get("gender"):
            gender[d["gender"].lower()].append(i)
        source[d["source"]].append(i)
        if d.get("discipline"):
            discipline[d["discipline"]].append(i)
        if d.get("confidence"):
            confidence[d["confidence"]].append(i)
        for sid in d["series_names"]:
            sid_idx[sid].append(i)
        for tok in sorted({t for n in d["series_names"].values() for t in tokenize(n, False)}):
            stok[tok].append(i)
        for stat in STATS:
            if d["career"].get(stat) is not None:
                stats[f"career.{stat}"].append((d["career"][stat], i))
            for sid, row in d["series"].items():
                if row.get(stat) is not None:
                    stats[f"{sid}.{stat}"].append((row[stat], i))
    for k in stats:
        stats[k].sort()
    return {"teams": teams, "nationality": nat, "gender": gender, "series_id": sid_idx,
            "series_tokens": stok, "source": source, "discipline": discipline, "confidence": confidence,
            "stats": stats}


def write_index(entries, weights, out_dir, use_stopwords):
    """entries: [(doc_meta_dict, {field: Counter})] -> index files in out_dir. Returns the meta dict."""
    n = len(entries)
    os.makedirs(out_dir, exist_ok=True)
    # per-field postings: field -> term -> [(doc_id, tf), ...]
    postings = {f: defaultdict(list) for f in weights}
    for doc_id, (_, fields) in enumerate(entries):
        for field, tf in fields.items():
            for term, c in tf.items():
                postings[field][term].append((doc_id, c))

    # vector lengths (cosine, idf_classic) and max tf per field
    norm_sq = {f: [0.0] * n for f in weights}
    for field, post in postings.items():
        for plist in post.values():
            idf = idf_classic(n, len(plist))
            for d, tf in plist:
                w = (1 + math.log10(tf)) * idf
                norm_sq[field][d] += w * w

    docs = []
    for doc_id, (meta, fields) in enumerate(entries):
        docs.append({
            "id": doc_id, **meta,
            "norms": {f: math.sqrt(norm_sq[f][doc_id]) for f in fields},
            "max_tf": {f: max(tf.values()) for f, tf in fields.items()},
        })
    with open(os.path.join(out_dir, "docs.jsonl"), "w", encoding="utf-8") as f:
        for d in docs:
            f.write(json.dumps(d, ensure_ascii=False) + "\n")

    vocab = {}
    with open(os.path.join(out_dir, "terms.tsv"), "w", encoding="utf-8") as tf_out:
        tf_out.write("field\tterm\tdf\tidf_classic\tidf_smooth\n")
        for field in weights:
            post = postings[field]
            vocab[field] = len(post)
            with open(os.path.join(out_dir, f"postings_{field}.jsonl"), "w", encoding="utf-8") as f:
                for term in sorted(post):
                    f.write(json.dumps({"t": term, "p": post[term]}, ensure_ascii=False) + "\n")
                    df = len(post[term])
                    tf_out.write(f"{field}\t{term}\t{df}\t{idf_classic(n, df)!r}\t{idf_smooth(n, df)!r}\n")

    with open(os.path.join(out_dir, "filters.json"), "w", encoding="utf-8") as f:
        json.dump(build_filters(docs), f, ensure_ascii=False)
    meta = {"N": n, "vocabulary": vocab, "field_weights": weights, "stopwords": use_stopwords,
            "documents_per_source": dict(Counter(d["source"] for d in docs)),
            "idf_classic": "log10(N/df)", "idf_smooth": "log10(1+N/df)"}
    with open(os.path.join(out_dir, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    return meta


def main():
    ap = argparse.ArgumentParser(description="Custom indexer (per-field inverted index, TF) over the joined MSM + wiki file")
    ap.add_argument("--joined", default="data/joined_drivers.jsonl", help="input: joined_drivers.jsonl")
    ap.add_argument("--out", default="data/index", help="output folder of the index")
    ap.add_argument("--msm-only", action="store_true",
                    help="index only the MSM side (wiki-only records and wiki fields are ignored) = the old index, "
                         "for the comparison with the joined one")
    ap.add_argument("--no-stopwords", action="store_true", help="do not remove stopwords while tokenizing")
    args = ap.parse_args()
    use_sw = not args.no_stopwords

    driver_entries, series_entries = [], []
    for rec in read_jsonl(args.joined):
        built = build_record(rec, args.msm_only)
        if built is None:
            continue
        base, names, rows, fields = built
        driver_entries.append((
            {**base, "series_names": names, "series": {sid: series_stats(r) for sid, r in rows.items()},
             "career": career_totals(rows.values())},
            field_counters(fields, use_sw)))

        # --- (record, series)-level documents ---
        for sid, r in rows.items():
            sfields = {"name": fields["name"], "full_name": fields["full_name"], "nationality": fields["nationality"],
                       "series": names[sid], "gender": fields["gender"]}
            stats = series_stats(r)
            series_entries.append((
                {**base, "driver_series_id": f"{base['driver_id']}/{sid}",
                 "series_names": {sid: names[sid]}, "series": {sid: stats},
                 "career": {k: stats[k] for k in STATS}},
                field_counters(sfields, use_sw)))

    if not driver_entries:
        sys.exit("No documents to index.")

    out = {"drivers": write_index(driver_entries, DRIVER_WEIGHTS, os.path.join(args.out, "drivers"), use_sw)}
    if series_entries:
        out["series"] = write_index(series_entries, SERIES_WEIGHTS, os.path.join(args.out, "series"), use_sw)
    print(json.dumps({k: {"N": v["N"], "documents_per_source": v["documents_per_source"],
                          "vocabulary": v["vocabulary"]} for k, v in out.items()}, indent=2))


if __name__ == "__main__":
    main()