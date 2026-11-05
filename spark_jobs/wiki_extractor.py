"""
Spark Wikipedia extractor: enwiki-latest-pages-articles.xml -> ONE JSONL with a record per driver page.

    spark-submit spark_jobs/wiki_extractor.py
    python spark_jobs/wiki_extractor.py --limit-mb 2000      (a test run on the first 2 GB)

Paths are fixed (constants below): data/wiki/enwiki-latest-pages-articles.xml (+ data/drivers.jsonl for the
msm_rescue path) -> data/spark/wiki -> data/wiki_drivers.jsonl. The output is always written by Spark (df.write).

The regexes are NOT here: Spark only distributes the work and calls wiki_page.extract_wiki_page() on every page
(see wiki_page.py for the filter - infobox / category / msm_rescue - and for the record layout).

Plan:
  1. reading the dump (--reader):
       hadoop  DEFAULT. Hadoop TextInputFormat with the record delimiter '</page>': Spark/Hadoop make the splits and
               read them where the data is (works for hdfs:// as well as for a local file)
       ranges  FALLBACK (prints a WARNING) for environments where the Hadoop file system does not work (Windows without
               winutils.exe / hadoop.dll): the dump is cut into byte ranges (--part-mb each); a range owns the pages
               whose '<page>' tag starts in it. Every Python worker opens the file itself - no Hadoop; on a cluster
               the file has to be on a folder every node sees
  2. mapPartitions -> DataFrame(kind, payload): kind = rec (a driver record as JSON) | stat (counters of the range)
     | err (a page that raised an exception - one broken page must not kill the job)
  3. persist -> the dump is read once; the records and the statistics come from the same cached rows
  4. the records are ordered by page_id and written by Spark as one file (df.write into OUT, then moved to SINGLE_FILE)
The Motor Sport Magazine drivers (MSM_FILE) are only used for the msm_rescue path: {name key: [birth years]} is
broadcast to the workers.
"""
import argparse
import glob
import json
import os
import shutil
import sys
import tempfile
import time
from collections import Counter

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F

HERE = os.path.dirname(os.path.abspath(__file__))


def find_root():
    """Folder that contains wiki_page.py: this script's folder or one of the folders above it."""
    d = HERE
    for _ in range(4):
        if os.path.exists(os.path.join(d, "wiki_page.py")):
            return d
        d = os.path.dirname(d)
    sys.exit(f"wiki_page.py was not found in {HERE} or in the folders above it.")


ROOT = find_root()
sys.path.insert(0, ROOT)
import wiki_page  # noqa: E402  (driver side: plan_ranges)

# ---------- fixed locations ----------
# (relative to ROOT = the folder with wiki_page.py, NOT to the current working directory, so the job behaves the same
#  from a terminal and from an IDE)
DATA = os.path.join(ROOT, "data")
DUMP = "C:/Users/PC/Downloads/enwiki-latest-pages-articles.xml"   # the XML dump
MSM_FILE = os.path.join(DATA, "drivers.jsonl")           # MSM drivers (enables the msm_rescue path)
OUT = os.path.join(DATA, "spark", "wiki")                # Spark writes part-* into this folder
SINGLE_FILE = os.path.join(DATA, "wiki_drivers.jsonl")   # the final single JSONL file
STATS_FILE = os.path.join(DATA, "spark", "wiki_extract_stats.json")     # statistics


def hadoop_io_ok(spark):
    """Can Spark use the Hadoop file system here? A real probe (write + list + read of a tiny file), the same as in
    html_extractor.py: on Windows it needs winutils.exe + hadoop.dll, Linux/macOS need no native library."""
    if os.name != "nt":
        return True
    tmp = tempfile.mkdtemp(prefix="vinf-hadoop-probe-")
    try:
        spark.range(1).coalesce(1).write.json(os.path.join(tmp, "p"))
        spark.read.format("binaryFile").load(os.path.join(tmp, "p")).count()
        return True
    except Exception:
        return False
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def load_msm_index(path):
    """{name key: [birth years]} from drivers.jsonl (name and full_name of every driver)."""
    out = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            d = json.loads(line)
            bd = d.get("born_date") or ""
            year = int(bd[:4]) if bd[:4].isdigit() else None
            for nm in (d.get("name"), d.get("full_name")):
                key = wiki_page.norm_key(nm or "")
                if key and year:
                    out.setdefault(key, set()).add(year)
    return {k: sorted(v) for k, v in out.items()}


def _process(wp, raw, msm, stats):
    """One <page> block -> zero or one output row; the counters of the partition are updated in `stats`."""
    stats["pages"] += 1
    try:
        rec, reason = wp.extract_wiki_page(raw, msm)
    except Exception as e:                          # a broken page must not kill the whole job
        stats["errors"] += 1
        if stats["errors"] <= 5:
            title, *_ = wp.split_page(raw)
            yield ("err", json.dumps({"title": title, "error": f"{type(e).__name__}: {e}"}, ensure_ascii=False))
        return
    stats[reason] += 1
    if rec:
        stats["match_" + rec["match_reason"]] += 1
        stats["discipline_" + rec["discipline"]] += 1
        stats["born_" + rec["born_prec"]] += 1
        stats["died_" + rec["died_prec"]] += 1
        stats["with_lead"] += 1 if rec["lead"] else 0
        stats["with_series"] += 1 if rec["series"] else 0
        stats["with_nationality"] += 1 if rec["nationality"] else 0
        stats["with_categorisation"] += 1 if rec["categorisation"] else 0
        stats["with_table_teams"] += 1 if rec["table_teams"] else 0
        stats["with_table_series"] += 1 if rec["table_series"] else 0
        stats["with_co_drivers"] += 1 if rec["co_drivers"] else 0
        stats["infobox_" + (rec["infobox_type"] or "none")] += 1
        yield ("rec", json.dumps(rec, ensure_ascii=False))


def make_range_parser(path, msm_bc):
    """FALLBACK --reader ranges, run on the executors: one call per partition = one byte range of the local file."""
    def parse_ranges(ranges):
        import wiki_page as wp                      # shipped with addPyFile / PYTHONPATH
        msm = msm_bc.value if msm_bc is not None else None
        for start, end in ranges:
            stats = Counter()
            for raw in wp.iter_pages_in_range(path, start, end):
                yield from _process(wp, raw, msm, stats)
            yield ("stat", json.dumps(stats))
    return parse_ranges


def make_record_parser(msm_bc):
    """--reader hadoop: the records are the texts between two '</page>' delimiters (the first one starts with
    <siteinfo>, the last one is '</mediawiki>'), so the page is cut out at '<page>' and the delimiter is put back."""
    def parse_records(texts):
        import wiki_page as wp
        msm = msm_bc.value if msm_bc is not None else None
        stats = Counter()
        for text in texts:
            i = text.find("<page>")
            if i != -1:
                yield from _process(wp, text[i:] + "</page>", msm, stats)
        yield ("stat", json.dumps(stats))
    return parse_records


def main():
    ap = argparse.ArgumentParser(
        description="Spark Wikipedia extractor (dump -> driver records as JSONL). Default: pure Spark + Hadoop.")
    ap.add_argument("--reader", choices=["hadoop", "ranges"], default="hadoop",
                    help="hadoop = Hadoop TextInputFormat with the '</page>' delimiter (default); ranges = FALLBACK "
                         "without Hadoop: Python workers read byte ranges of a local/shared file")
    ap.add_argument("--part-mb", type=float, default=128, help="size of one range / split = one task, in MB")
    ap.add_argument("--partitions", type=int, default=None, help="number of ranges (--reader ranges only)")
    ap.add_argument("--limit-mb", type=float, default=None, help="process only the first N MB of the dump (test run)")
    ap.add_argument("--cores", type=int, default=None,
                    help="number of parallel tasks = Python worker processes (local[N]); default: all cores. Lower it "
                         "if the machine runs out of memory (only with plain `python`; spark-submit has --master)")
    ap.add_argument("--driver-memory", default="4g",
                    help="driver JVM memory when started with plain `python` (spark-submit uses its own)")
    args = ap.parse_args()
    reader = args.reader

    t0 = time.time()
    if not os.path.exists(DUMP):
        sys.exit(f"The dump was not found: {DUMP}")
    # the python workers must find wiki_page.py and use the same interpreter (set BEFORE Spark starts)
    os.environ["PYTHONPATH"] = ROOT + os.pathsep + os.environ.get("PYTHONPATH", "")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    builder = (SparkSession.builder.appName("vinf-wiki-extractor")
               .config("spark.sql.shuffle.partitions", "8")
               # a crashed Python worker then reports WHERE it died instead of just "exited unexpectedly"
               .config("spark.python.worker.faulthandler.enabled", "true"))
    if "PYSPARK_GATEWAY_PORT" not in os.environ:
        builder = builder.config("spark.driver.memory", args.driver_memory)
        if args.cores:
            builder = builder.master(f"local[{args.cores}]")
    spark = builder.getOrCreate()
    sc = spark.sparkContext

    # ---- the fallback is always announced; Spark/Hadoop that does not work is NOT silently replaced ----
    if reader == "ranges":
        print("WARNING: FALLBACK reader 'ranges' - the Python workers read the dump without Spark/Hadoop "
              "(works only for a local / shared file).")
    # the output is always written by Spark (and the hadoop reader needs Hadoop as well)
    if not hadoop_io_ok(spark):
        spark.stop()
        sys.exit("ERROR: Spark cannot use the Hadoop file system here (winutils.exe / hadoop.dll missing?). "
                 "Fix the environment (diagnose with: python spark_jobs/check_spark_env.py).")
    try:
        sc.addPyFile(os.path.join(ROOT, "wiki_page.py"))
    except Exception as e:
        print(f"addPyFile skipped ({type(e).__name__}); local workers use PYTHONPATH={ROOT}")

    msm_bc = None
    if os.path.exists(MSM_FILE):
        msm = load_msm_index(MSM_FILE)
        msm_bc = sc.broadcast(msm)
        print(f"msm_rescue enabled: {len(msm)} name keys from {MSM_FILE}")
    else:
        print(f"WARNING: {MSM_FILE} not found, the msm_rescue path is disabled.")

    part_bytes = int(args.part_mb * 1024 * 1024)
    if reader == "ranges":
        size = os.path.getsize(DUMP)
        total_gb = size / 1e9
        if args.limit_mb:
            size = min(size, int(args.limit_mb * 1024 * 1024))
        n_parts = args.partitions or max(sc.defaultParallelism * 2, -(-size // part_bytes))
        ranges = wiki_page.plan_ranges(size, n_parts)
        print(f"reader=ranges: {total_gb:.1f} GB dump, processing {size / 1e9:.1f} GB in {len(ranges)} ranges")
        rows = (sc.parallelize(ranges, len(ranges))
                  .mapPartitions(make_range_parser(os.path.abspath(DUMP), msm_bc)))
    else:
        # one split = --part-mb (min = max size forces it even where the block size is smaller)
        conf = {"textinputformat.record.delimiter": "</page>",
                "mapreduce.input.fileinputformat.split.minsize": str(part_bytes),
                "mapreduce.input.fileinputformat.split.maxsize": str(part_bytes)}
        texts = (sc.newAPIHadoopFile(DUMP, "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                                     "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text", conf=conf)
                   .map(lambda kv: kv[1]))
        rows = texts.mapPartitions(make_record_parser(msm_bc))
        if args.limit_mb:
            # test run: only the first splits are scheduled at all (the other splits are not even read); the rows
            # (driver records + counters, a few MB) are collected and turned into a DataFrame below
            keep = min(-(-int(args.limit_mb * 1024 * 1024) // part_bytes), rows.getNumPartitions())
            rows = [tuple(r) for r in sc.runJob(rows, lambda it: list(it), list(range(keep)))]   # flat list of rows
        size = int(args.limit_mb * 1024 * 1024) if args.limit_mb else None
        ranges = []
        print(f"reader=hadoop: {DUMP}, splits of {args.part_mb} MB"
              + (f", only the first {keep} splits" if args.limit_mb else ""))
    df = spark.createDataFrame(rows, "kind string, payload string", verifySchema=False)
    df = df.persist(StorageLevel.MEMORY_AND_DISK)               # the dump is read once

    # ---- records: ordered by page_id, one JSONL file written by Spark ----
    recs = (df.filter("kind = 'rec'")
              .withColumn("pid", F.get_json_object("payload", "$.page_id").cast("long"))
              .orderBy("pid").select("payload"))
    os.makedirs(os.path.dirname(SINGLE_FILE) or ".", exist_ok=True)
    # the JSON lines are written as text: the layout (explicit nulls, nested series) stays exactly as made by
    # wiki_page.py; coalesce(1) after orderBy keeps the order
    recs.coalesce(1).write.mode("overwrite").text(OUT)
    part = glob.glob(os.path.join(OUT, "part-*"))
    if len(part) != 1:
        spark.stop()
        sys.exit(f"Expected exactly one part file in {OUT}, found {len(part)}.")
    shutil.move(part[0], SINGLE_FILE)
    with open(SINGLE_FILE, "r", encoding="utf-8") as f:
        n_rec = sum(1 for _ in f)

    # ---- statistics (the data is cached) ----
    total = Counter()
    for r in df.filter("kind = 'stat'").collect():
        total.update(json.loads(r["payload"]))
    errors = [json.loads(r["payload"]) for r in df.filter("kind = 'err'").limit(20).collect()]
    stats = dict(total)
    stats.update({"records": n_rec, "ranges": len(ranges) or None, "error_examples": errors,
                  "reader": reader, "writer": "spark", "fallback_used": reader != "hadoop",
                  "msm_rescue": msm_bc is not None, "single_file": SINGLE_FILE,
                  "bytes_processed": size, "seconds": round(time.time() - t0, 1)})
    os.makedirs(os.path.dirname(STATS_FILE) or ".", exist_ok=True)
    with open(STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()