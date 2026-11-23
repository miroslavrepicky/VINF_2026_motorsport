"""
Search over the index built by indexer.py (whole index in memory). Full text only.

    python3 searcher.py "ferrari monaco"
    python3 searcher.py "french AND lourdes OR greek AND athens" --method smooth
    python3 searcher.py                                     (interactive mode)

  ferrari monaco   soft OR: one matching word is enough, the score is multiplied by the coverage
                   (matched query words / all query words)
  AND / OR         UPPERCASE only; a document must satisfy at least one OR group, all words of the group (AND).
                   AND binds tighter than OR, no parentheses. The coverage is not applied.

Per field f (weights in indexer.FIELDS), final score = sum_f weight_f * score_f:
  cosine : w(t,d) = (1 + log10 tf) * log10(N/df);  score_f = sum_t w(t,q) w(t,d) / (|q_f| |d_f|)
  smooth : score_f = sum_t (0.5 + 0.5 * tf / max_tf_f(d)) * log10(1 + N/df)
"""
import argparse
import json
import math
import os
from collections import Counter, defaultdict

from indexer import FIELDS, read_jsonl
from tokenizer import tokenize


def parse(query):
    """-> (groups of terms, has_operators). Groups are separated by OR, AND is inside a group."""
    for stop in (True, False):                                    # a query of stopwords only: keep them
        groups, has_ops = [[]], False
        for w in query.split():
            if w in ("AND", "OR"):
                has_ops = True
                if w == "OR":
                    groups.append([])
            else:
                groups[-1] += tokenize(w, stop)
        groups = [list(dict.fromkeys(g)) for g in groups if g]
        if groups:
            break
    return groups, has_ops


class Index:
    def __init__(self, path):
        with open(os.path.join(path, "meta.json")) as f:
            self.N = json.load(f)["N"]
        self.docs = list(read_jsonl(os.path.join(path, "docs.jsonl")))
        self.post = {f: {} for f in FIELDS}                       # field -> term -> [[doc_id, tf]]
        for r in read_jsonl(os.path.join(path, "postings.jsonl")):
            self.post[r["f"]][r["t"]] = r["p"]

    def rank(self, terms, method):
        q, total = Counter(terms), defaultdict(float)
        for f, weight in FIELDS.items():
            post = self.post[f]
            known = [t for t in q if t in post]
            if not known:
                continue
            if method == "cosine":
                idf = {t: math.log10(self.N / len(post[t])) for t in known}
                wq = {t: (1 + math.log10(q[t])) * idf[t] for t in known}
                qn = math.sqrt(sum(w * w for w in wq.values()))
                acc = defaultdict(float)
                for t in known:
                    for d, tf in post[t]:
                        acc[d] += wq[t] * (1 + math.log10(tf)) * idf[t]
                for d, s in acc.items():
                    norm = qn * self.docs[d]["norms"][f]
                    total[d] += weight * s / norm if norm > 0 else 0.0
            else:
                for t in known:
                    idf = math.log10(1 + self.N / len(post[t]))
                    for d, tf in post[t]:
                        total[d] += weight * (0.5 + 0.5 * tf / self.docs[d]["max_tf"][f]) * idf
        return total

    def docs_of(self, term):
        return {d for f in FIELDS for d, _ in self.post[f].get(term, ())}

    def search(self, query, method="cosine", top=10):
        groups, has_ops = parse(query)
        terms = list(dict.fromkeys(t for g in groups for t in g))
        if not terms:
            return []
        raw = self.rank(terms, method)
        if has_ops:
            hit = set()
            for g in groups:
                hit |= set.intersection(*(self.docs_of(t) for t in g))
            scores = {d: raw.get(d, 0.0) for d in hit}
        else:
            cnt = Counter(d for t in terms for d in self.docs_of(t))
            scores = {d: raw[d] * c / len(terms) for d, c in cnt.items() if raw.get(d, 0) > 0}
        best = sorted(scores.items(), key=lambda x: (-x[1], self.docs[x[0]]["name"]))[:top]
        return [(self.docs[d], s) for d, s in best]


def show(results):
    if not results:
        print("(no results)")
    for i, (d, s) in enumerate(results, 1):
        print(f"{i:>2}. {s:8.4f}  {d['name']}  [{d['nationality'] or '?'}, {d['born_date'] or '?'}]  <{d['source']}>")
        print(f"{'':14}{d['url']}")


def main():
    ap = argparse.ArgumentParser(description="Search the index (per-field TF-IDF)")
    ap.add_argument("query", nargs="?")
    ap.add_argument("--index", default="data/index")
    ap.add_argument("--method", choices=["cosine", "smooth"], default="cosine")
    ap.add_argument("--top", type=int, default=10)
    args = ap.parse_args()
    idx = Index(args.index)
    if args.query:
        show(idx.search(args.query, args.method, args.top))
        return
    print(f"Index: {idx.N} documents. Empty line = quit.")
    while (q := input("> ").strip()):
        show(idx.search(q, args.method, args.top))


if __name__ == "__main__":
    main()