"""
Spark HTML extractor: data/html/*.html -> ONE wide JSONL (same format as extractor.py produces locally).

    spark-submit spark_jobs/html_extractor.py --input data/html --out data/spark/drivers
    spark-submit spark_jobs/html_extractor.py --input data/html --out data/spark/drivers \
                 --single-file data/drivers.jsonl --stats data/spark/extract_stats.json

The regexes are NOT re-implemented: Spark only distributes the work and calls extractor.extract_page()
on every file (each partition = a group of files processed by one task).

Plan (DataFrame API, nothing is executed until the write / the actions):
  1. binaryFile source        -> DataFrame(path, content)                     one row per HTML file
  2. mapPartitions            -> parsed DataFrame: one row per page (+ nested array of series)
  3. persist                  -> parsed once, reused by the writer and by the statistics
  4. explode + pivot          -> wide table: columns s__<series>__<stat>, NULL where the driver did not race
  5. join back to base fields -> one row per driver
  6. coalesce(1).write.json   -> one output file (nulls are written explicitly)
"""
import argparse
import glob
import json
import os
import shutil
import sys
import time

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F, types as T

HERE = os.path.dirname(os.path.abspath(__file__))


def find_root():
    """Folder that contains extractor.py: this script's folder or one of the folders above it."""
    d = HERE
    for _ in range(4):
        if os.path.exists(os.path.join(d, "extractor.py")):
            return d
        d = os.path.dirname(d)
    sys.exit(f"extractor.py was not found in {HERE} or in the folders above it.")


ROOT = find_root()
STATS = ("starts", "wins", "poles", "podiums")
STAT_COLS = STATS + ("years",)
COL_PREFIX = "s__"          # must be the same as in extractor.py

S, I, L, B = T.StringType(), T.IntegerType(), T.LongType(), T.BooleanType()
SERIES_T = T.StructType([
    T.StructField("series_id", S), T.StructField("series_name", S),
    T.StructField("starts", I), T.StructField("wins", I),
    T.StructField("poles", I), T.StructField("podiums", I),
    T.StructField("years", T.ArrayType(I)),
])
# (name, type) of the plain driver fields, in the order of the output record
BASE_FIELDS = [
    ("driver_id", S), ("wp_id", L), ("url", S), ("name", S), ("full_name", S), ("gender", S),
    ("nationality", S), ("born_date", S), ("died_date", S), ("last_race_slug", S),
    ("last_race_name", S), ("tagline", S), ("bio", S),
    ("teams", T.ArrayType(S)), ("seasons", T.ArrayType(S)),
]
# helper values of extractor.extract_page() used only for the statistics (not written to the output)
DIAG_FIELDS = [("born_prec", S), ("died_prec", S), ("real_bio", B)]
PARSED_SCHEMA = T.StructType(
    [T.StructField("file", S)]
    + [T.StructField(n, t) for n, t in BASE_FIELDS]
    + [T.StructField("series", T.ArrayType(SERIES_T))]
    + [T.StructField(n, t) for n, t in DIAG_FIELDS]
    + [T.StructField("error", S)])


def parse_partition(rows):
    """Runs on the executors: one call per partition. Yields one tuple (PARSED_SCHEMA) per HTML file."""
    import extractor                                   # shipped with addPyFile, imported on the executor
    n_base, n_diag = len(BASE_FIELDS), len(DIAG_FIELDS)
    for r in rows:
        try:
            text = bytes(r.content).decode("utf-8", errors="replace")
            drv, series = extractor.extract_page(text)
        except Exception as e:                         # a broken file must not kill the whole job
            yield (r.path,) + (None,) * (n_base + 1 + n_diag) + (f"exception: {type(e).__name__}: {e}",)
            continue
        if drv is None:
            yield (r.path,) + (None,) * (n_base + 1 + n_diag) + ("not_driver_page",)
            continue
        base = tuple(drv.get(n) for n, _ in BASE_FIELDS)
        srows = [(s["series_id"], s["series_name"] or s["series_id"], s["starts"], s["wins"],
                  s["poles"], s["podiums"], s["years"]) for s in series]
        diag = (drv["_born_prec"], drv["_died_prec"], drv["_real_bio"])
        yield (r.path,) + base + (srows,) + diag + (None,)


def build_wide(ok):
    """ok: parsed rows without errors -> one row per driver with the columns of every series."""
    base = ok.select(
        *[n for n, _ in BASE_FIELDS],
        # {series_id: name} only for the series the driver took part in
        F.map_from_entries(F.transform("series", lambda x: F.struct(x["series_id"], x["series_name"]))
                           ).alias("series_names"))
    long = ok.select("driver_id", F.explode("series").alias("s")).select("driver_id", "s.*")
    sids = sorted(r[0] for r in long.select("series_id").distinct().collect())   # the only extra job: series list
    wide = (long.groupBy("driver_id")
                .pivot("series_id", sids)                                        # a column group per series
                .agg(*[F.first(c).alias(c) for c in STAT_COLS]))
    # pivot names the columns <series>_<stat>; rename them all at once to s__<series>__<stat>
    renamed = []
    for c in wide.columns:
        if c == "driver_id":
            renamed.append(c)
            continue
        sid, stat = next((c[:-len(st) - 1], st) for st in STAT_COLS if c.endswith("_" + st))
        renamed.append(f"{COL_PREFIX}{sid}__{stat}")
    wide = wide.toDF(*renamed)
    # LEFT join: drivers without any series stay in the output (all their series columns are NULL)
    return base.join(wide, "driver_id", "left").orderBy("driver_id"), sids


def main():
    ap = argparse.ArgumentParser(description="Spark HTML extractor (driver pages -> one wide JSONL)")
    ap.add_argument("--input", default="data/html", help="folder with the HTML files (local, hdfs://, ...)")
    ap.add_argument("--out", default="data/spark/drivers", help="output folder (Spark writes part-*.json into it)")
    ap.add_argument("--single-file", default=None,
                    help="move the part file to this path (local filesystem only), e.g. data/drivers.jsonl")
    ap.add_argument("--stats", default=None, help="write the statistics to this local JSON file")
    ap.add_argument("--glob", default="*.html", help="file name filter")
    ap.add_argument("--partitions", type=int, default=None, help="repartition the input (default: Spark decides)")
    args = ap.parse_args()

    t0 = time.time()
    # master / memory come from spark-submit (cluster) - local[*] is only the fallback for a plain `python`
    spark = SparkSession.builder.appName("vinf-html-extractor").getOrCreate()
    spark.sparkContext.addPyFile(os.path.join(ROOT, "extractor.py"))   # code for the executors

    files = (spark.read.format("binaryFile").option("pathGlobFilter", args.glob)
             .load(args.input).select("path", "content"))
    if args.partitions:
        files = files.repartition(args.partitions)
    parsed = spark.createDataFrame(files.rdd.mapPartitions(parse_partition), PARSED_SCHEMA)
    parsed = parsed.persist(StorageLevel.MEMORY_AND_DISK)      # parsed once, used by the output and the statistics

    ok = parsed.filter("error IS NULL").dropDuplicates(["driver_id"])
    out, sids = build_wide(ok)
    (out.coalesce(1).write.mode("overwrite")
        .option("ignoreNullFields", "false")                   # keep "s__f2__wins": null explicitly
        .json(args.out))

    # ---- statistics (cheap: the data is already cached) ----
    agg = ok.agg(
        F.count("*").alias("drivers"),
        *[F.sum(cond.cast("int")).alias(name) for name, cond in [
            ("with_bio", F.col("real_bio")), ("with_series", F.size("series") > 0),
            ("with_teams", F.size("teams") > 0), ("with_seasons", F.size("seasons") > 0),
            ("with_nationality", F.col("nationality").isNotNull()),
            ("with_born_date", F.col("born_date").isNotNull())]]).first().asDict()
    stats = {k: (v or 0) for k, v in agg.items()}
    stats["files"] = parsed.count()
    stats["errors"] = {r["error"]: r["count"] for r in
                       parsed.filter("error IS NOT NULL").groupBy("error").count().collect()}
    stats["series_groups"] = len(sids)
    stats["columns"] = len(out.columns)
    stats["partitions"] = parsed.rdd.getNumPartitions()
    stats["seconds"] = round(time.time() - t0, 1)

    if args.single_file and "://" not in args.out:             # local filesystem only
        part = glob.glob(os.path.join(args.out, "part-*.json"))
        if len(part) == 1:
            os.makedirs(os.path.dirname(args.single_file) or ".", exist_ok=True)
            shutil.move(part[0], args.single_file)
            stats["single_file"] = args.single_file
    if args.stats:
        os.makedirs(os.path.dirname(args.stats) or ".", exist_ok=True)
        with open(args.stats, "w", encoding="utf-8") as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    spark.stop()


if __name__ == "__main__":
    sys.path.insert(0, ROOT)
    main()