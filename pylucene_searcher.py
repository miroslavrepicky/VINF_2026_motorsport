"""
PyLucene search (full text, Lucene query syntax, BM25). Fields and weights = indexer.FIELDS (as query boosts).

    python3 pylucene_searcher.py "ferrari monaco"                    soft OR (default operator)
    python3 pylucene_searcher.py "ferrari AND monaco"                Boolean AND
    python3 pylucene_searcher.py "french AND lourdes OR greek"       Boolean AND / OR
    python3 pylucene_searcher.py '"red bull"'                        Phrase
    python3 pylucene_searcher.py "hamliton~"                         Fuzzy (edit distance <= 2; hamliton~1)
    python3 pylucene_searcher.py "ferrari born_year:[1950 TO 1960]"  Range on the birth year
    python3 pylucene_searcher.py                                     (interactive mode)
"""
import argparse

import lucene
from java.lang import Float
from java.nio.file import Paths
from java.util import HashMap
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.queryparser.classic import MultiFieldQueryParser, QueryParserBase
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.store import FSDirectory

from indexer import FIELDS
from pylucene_indexer import STORED, fold, make_analyzer


class LuceneIndex:
    def __init__(self, path):
        lucene.initVM()
        self.searcher = IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(path))))
        boosts = HashMap()
        for f, w in FIELDS.items():
            boosts.put(f, Float(w))
        self.parser = MultiFieldQueryParser(list(FIELDS), make_analyzer(), boosts)

    def search(self, query, top=10):
        """-> [(doc dict, score)]; a syntactically wrong query gives []."""
        try:
            q = QueryParserBase.parse(self.parser, fold(query))
        except lucene.JavaError as e:
            print("bad query:", e.getJavaException().getMessage())
            return []
        stored = self.searcher.storedFields()
        return [({k: stored.document(h.doc).get("s_" + k) for k in STORED}, h.score)
                for h in self.searcher.search(q, top).scoreDocs]


def show(results):
    if not results:
        print("(no results)")
    for i, (d, s) in enumerate(results, 1):
        print(f"{i:>2}. {s:8.4f}  {d['name']}  [{d['nationality'] or '?'}, {d['born_date'] or '?'}]  <{d['source']}>")
        print(f"{'':14}{d['url']}")


def main():
    ap = argparse.ArgumentParser(description="PyLucene search")
    ap.add_argument("query", nargs="?")
    ap.add_argument("--index", default="data/index_lucene")
    ap.add_argument("--top", type=int, default=10)
    args = ap.parse_args()
    idx = LuceneIndex(args.index)
    if args.query:
        show(idx.search(args.query, args.top))
        return
    print("PyLucene index. Empty line = quit.")
    while (q := input("> ").strip()):
        show(idx.search(q, args.top))


if __name__ == "__main__":
    main()
