"""
Custom indexer over joined_drivers.jsonl (MSM x Wikipedia). Full-text only, stdlib only.

    python3 indexer.py --joined data/joined_drivers.jsonl --out data/index

One joined record = one document. Every field has its OWN inverted index (own df, own idf, own vector lengths):
  postings.jsonl  {"f": field, "t": term, "p": [[doc_id, tf], ...]}
  docs.jsonl      metadata for display + norms {field: vector length} + max_tf {field: max tf}
  meta.json       N
idf_classic = log10(N / df), idf_smooth = log10(1 + N / df) are computed from df when the searcher loads the index.
Field weights are applied at search time only.
"""
import argparse
import json
import math
import os
from collections import Counter, defaultdict

from tokenizer import tokenize

# field -> weight (document score = sum of weight * score in the field)
FIELDS = {
    "name": 5.0, "full_name": 3.0, "nationality": 3.0, "teams": 2.0, "series": 2.0, "seasons": 2.0,
    "tagline": 2.0, "gender": 2.0, "discipline": 2.0, "born_place": 2.0, "titles": 2.0,
    "died_place": 1.0, "bio": 1.0, "lead": 1.0, "awards": 1.0, "relatives": 1.0, "categories": 1.0,
    "co_drivers": 1.0,
}
# gender / discipline are only codes in the data; synonyms make "female" / "motorbike" findable
GENDER_TEXT = {"male": "male man men", "female": "female woman women"}
DISCIPLINE_TEXT = {"car": "car cars", "motorcycle": "motorcycle motorcycles motorbike bike",
                   "both": "car cars motorcycle motorcycles motorbike bike"}


def read_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def text(v):
    """str | list | None -> str"""
    return " ".join(map(str, v)) if isinstance(v, list) else (v or "")


def fields_of(rec):
    """Joined record -> {field: text}. Shared with pylucene_indexer.py, so both indexes see the same text."""
    f = {k: text(rec.get(k)) for k in FIELDS}
    f["teams"] += " " + text(rec.get("current_team"))
    f["categories"] += " " + text(rec.get("categorisation"))
    f["gender"] = GENDER_TEXT.get(rec.get("gender") or "", "")
    f["discipline"] = DISCIPLINE_TEXT.get(rec.get("discipline") or "", "")
    return f


def meta_of(rec):
    """What the searcher shows (stored, not searched)."""
    return {"record_id": rec["record_id"], "source": rec["source"], "name": rec.get("name") or "",
            "nationality": rec.get("nationality") or "", "born_date": rec.get("born_date") or "",
            "url": rec.get("url_msm") or rec.get("url_wiki") or ""}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--joined", default="data/joined_drivers.jsonl")
    ap.add_argument("--out", default="data/index")
    args = ap.parse_args()

    metas, counters = [], []
    for rec in read_jsonl(args.joined):
        metas.append(meta_of(rec))
        counters.append({f: c for f, t in fields_of(rec).items() if (c := Counter(tokenize(t)))})
    n = len(metas)

    postings = {f: defaultdict(list) for f in FIELDS}            # field -> term -> [(doc_id, tf)]
    for d, fields in enumerate(counters):
        for f, c in fields.items():
            for term, tf in c.items():
                postings[f][term].append((d, tf))

    norm_sq = [defaultdict(float) for _ in range(n)]              # cosine vector length per document and field
    for f, post in postings.items():
        for plist in post.values():
            idf = math.log10(n / len(plist))
            for d, tf in plist:
                norm_sq[d][f] += ((1 + math.log10(tf)) * idf) ** 2

    os.makedirs(args.out, exist_ok=True)
    with open(os.path.join(args.out, "docs.jsonl"), "w", encoding="utf-8") as out:
        for d, meta in enumerate(metas):
            meta["norms"] = {f: math.sqrt(norm_sq[d][f]) for f in counters[d]}
            meta["max_tf"] = {f: max(c.values()) for f, c in counters[d].items()}
            out.write(json.dumps(meta, ensure_ascii=False) + "\n")
    with open(os.path.join(args.out, "postings.jsonl"), "w", encoding="utf-8") as out:
        for f, post in postings.items():
            for term in sorted(post):
                out.write(json.dumps({"f": f, "t": term, "p": post[term]}) + "\n")
    with open(os.path.join(args.out, "meta.json"), "w") as out:
        json.dump({"N": n}, out)
    print(f"{n} documents, {sum(len(p) for p in postings.values())} (field, term) pairs -> {args.out}")


if __name__ == "__main__":
    main()