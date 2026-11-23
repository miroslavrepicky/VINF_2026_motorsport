"""
PyLucene indexer over joined_drivers.jsonl - the same documents and fields as indexer.py (shared fields_of()).

    python3 pylucene_indexer.py --joined data/joined_drivers.jsonl --out data/index_lucene

  - every text field of indexer.FIELDS is a TextField (tokenized, positions -> phrase queries, not stored)
  - born_year is a StringField (one untokenized 4-digit term -> Range query born_year:[1950 TO 1960])
  - record_id, source, name, nationality, born_date, url are stored only to be shown in the results
  - analyzer: StandardAnalyzer + English stopwords, lowercase; the accents are removed by fold() before
    indexing AND before querying (same normalization as tokenizer.py)
"""
import argparse
import unicodedata

import lucene
from java.nio.file import Paths
from org.apache.lucene.analysis.en import EnglishAnalyzer
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, StoredField, StringField, TextField
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.store import FSDirectory

from indexer import fields_of, meta_of, read_jsonl

STORED = ("record_id", "source", "name", "nationality", "born_date", "url")


def fold(s):
    """Strip diacritics (case is kept: AND / OR in a query must stay uppercase)."""
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def make_analyzer():
    return StandardAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--joined", default="data/joined_drivers.jsonl")
    ap.add_argument("--out", default="data/index_lucene")
    args = ap.parse_args()

    lucene.initVM()
    cfg = IndexWriterConfig(make_analyzer())
    cfg.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
    writer = IndexWriter(FSDirectory.open(Paths.get(args.out)), cfg)
    n = 0
    for rec in read_jsonl(args.joined):
        doc, meta = Document(), meta_of(rec)
        for k in STORED:
            doc.add(StoredField("s_" + k, meta[k]))
        for field, txt in fields_of(rec).items():
            if txt.strip():
                doc.add(TextField(field, fold(txt), Field.Store.NO))
        if meta["born_date"]:
            doc.add(StringField("born_year", meta["born_date"][:4], Field.Store.NO))
        writer.addDocument(doc)
        n += 1
    writer.close()
    print(f"{n} documents -> {args.out}")


if __name__ == "__main__":
    main()
