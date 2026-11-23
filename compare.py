"""
Old (custom TF-IDF) index vs new (PyLucene) index on the SAME Boolean queries (only AND / OR).
Every term is joined by an explicit AND or OR, so both indexes answer the same logical question and the
top of the rankings can be compared.

    python3 compare.py --old data/index --new data/index_lucene [--top 5]
"""
import argparse

from pylucene_searcher import LuceneIndex
from searcher import Index

# (what the query is about, query for the old index, query for PyLucene)
QUERIES = [
    ("women + nationality (AND)", "female AND danish", "female AND danish"),
    ("motorcycles + nationality (AND)", "motorcycle AND italian", "motorcycle AND italian"),
    ("champions of a team (AND)", "champion AND ferrari", "champion AND ferrari"),
    ("rivals (OR)", "senna OR prost OR lauda", "senna OR prost OR lauda"),
]


def label(d):
    return f"{d['name']} ({d['nationality'] or '?'})"[:38]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", default="data/index")
    ap.add_argument("--new", default="data/index_lucene")
    ap.add_argument("--top", type=int, default=5)
    args = ap.parse_args()
    old, new = Index(args.old), LuceneIndex(args.new)
    for kind, q_old, q_new in QUERIES:
        a = [d for d, _ in old.search(q_old, "cosine", args.top)]
        b = [d for d, _ in new.search(q_new, args.top)]
        print(f"\n=== {kind}\n  old: {q_old}\n  new: {q_new}")
        print(f"  {'OLD (cosine TF-IDF)':<40}PYLUCENE (BM25)")
        for i in range(args.top):
            left = label(a[i]) if i < len(a) else "-"
            right = label(b[i]) if i < len(b) else "-"
            print(f"  {i + 1}. {left:<37} {right}")


if __name__ == "__main__":
    main()