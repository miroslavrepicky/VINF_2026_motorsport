"""
Spark HTML extractor: data/html/*.html -> ONE wide JSONL (same format as extractor.py produces locally).

    spark-submit spark_jobs/html_extractor.py
    python spark_jobs/html_extractor.py                      (plain python = local[*] Spark)

Paths are fixed (constants below): data/html -> data/spark/drivers -> data/drivers.jsonl, statistics in
data/spark/extract_stats.json.

The default is pure Spark + Hadoop: files are read by the Spark `binaryFile` source and the output is written
by `df.write.json`. The only options are two EXPLICIT fallbacks for environments where the Hadoop file system
does not work (typically Windows without winutils.exe / hadoop.dll); each of them prints a WARNING:
    --reader paths     the Python workers read the files themselves (no Hadoop; local/shared folders only)
    --writer python    the driver writes the output file itself (no Hadoop; not suitable for big outputs)

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
import tempfile
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

# ---------- fixed locations ----------
# (relative to ROOT = the folder with extractor.py, NOT to the current working directory, so the job behaves the same
#  from a terminal and from an IDE)
DATA = os.path.join(ROOT, "data")
INPUT = os.path.join(DATA, "html")                   # folder with the HTML files
GLOB = "*.html"                                      # only HTML files are crawled
OUT = os.path.join(DATA, "spark", "drivers")         # Spark writes part-*.json into this folder
SINGLE_FILE = os.path.join(DATA, "drivers.jsonl")    # the final single JSONL file
STATS_FILE = os.path.join(DATA, "spark", "extract_stats.json")   # statistics

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
    for path, content in rows:
        try:
            text = bytes(content).decode("utf-8", errors="replace")
            drv, series = extractor.extract_page(text)
        except Exception as e:                         # a broken file must not kill the whole job
            yield (path,) + (None,) * (n_base + 1 + n_diag) + (f"exception: {type(e).__name__}: {e}",)
            continue
        if drv is None:
            yield (path,) + (None,) * (n_base + 1 + n_diag) + ("not_driver_page",)
            continue
        base = tuple(drv.get(n) for n, _ in BASE_FIELDS)
        srows = [(s["series_id"], s["series_name"] or s["series_id"], s["starts"], s["wins"],
                  s["poles"], s["podiums"], s["years"]) for s in series]
        diag = (drv["_born_prec"], drv["_died_prec"], drv["_real_bio"])
        yield (path,) + base + (srows,) + diag + (None,)


def hadoop_io_ok(spark):
    """Can Spark use the Hadoop file system here? A real probe (write + list + read of a tiny file) instead of
    guessing from the presence of winutils.exe / hadoop.dll. Linux/macOS need no native library."""
    if os.name != "nt":
        return True
    tmp = tempfile.mkdtemp(prefix="vinf-hadoop-probe-")
    try:
        out = os.path.join(tmp, "p")
        spark.range(1).coalesce(1).write.json(out)
        spark.read.format("binaryFile").load(out).count()
        return True
    except Exception:
        return False
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def read_partition(paths):
    """Executor side of the FALLBACK 'paths' reader: read the files with plain Python (no Hadoop file system)."""
    for path in paths:
        with open(path, "rb") as f:
            yield path, f.read()


def write_jsonl_local(df, path):
    """FALLBACK writer: write the rows from the driver with plain Python (no Hadoop file system): one JSONL file."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for row in df.toLocalIterator():                 # streams one partition at a time
            f.write(json.dumps(row.asDict(recursive=True), ensure_ascii=False) + "\n")
    os.replace(tmp, path)


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
    ap = argparse.ArgumentParser(
        description="Spark HTML extractor (driver pages -> one wide JSONL). Default: pure Spark + Hadoop.")
    ap.add_argument("--reader", choices=["spark", "paths"], default="spark",
                    help="spark = Spark binaryFile source (default); paths = FALLBACK without Hadoop: the Python "
                         "workers read the files themselves (local/shared folders only)")
    ap.add_argument("--writer", choices=["spark", "python"], default="spark",
                    help="spark = df.write.json (default); python = FALLBACK without Hadoop: the driver writes the "
                         "output file itself")
    ap.add_argument("--driver-memory", default="4g",
                    help="driver JVM memory when started with plain `python` (spark-submit uses its own --driver-memory)")
    ap.add_argument("--partitions", type=int, default=None,
                    help="number of partitions = tasks = Python worker processes (default: 4 x cores)")
    args = ap.parse_args()
    reader, writer = args.reader, args.writer

    t0 = time.time()
    # the python workers must find extractor.py and use the same interpreter (set BEFORE Spark starts)
    os.environ["PYTHONPATH"] = ROOT + os.pathsep + os.environ.get("PYTHONPATH", "")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    # master / memory come from spark-submit (cluster) - local[*] is only the fallback for a plain `python`
    builder = (SparkSession.builder.appName("vinf-html-extractor")
               .config("spark.sql.shuffle.partitions", "8"))       # tiny data: 200 default shuffle tasks are pure overhead
    if "PYSPARK_GATEWAY_PORT" not in os.environ:                   # started by plain `python`, not by spark-submit
        builder = builder.config("spark.driver.memory", args.driver_memory)
    spark = builder.getOrCreate()
    sc = spark.sparkContext

    # ---- fallbacks are always announced; Spark/Hadoop that does not work is NOT silently replaced ----
    if reader == "paths":
        print("WARNING: FALLBACK reader 'paths' - the Python workers read the files without Spark/Hadoop "
              "(works only for local / shared folders).")
    if writer == "python":
        print("WARNING: FALLBACK writer 'python' - the driver writes the output without Spark/Hadoop "
              "(not suitable for big outputs).")
    if (reader == "spark" or writer == "spark") and not hadoop_io_ok(spark):
        spark.stop()
        sys.exit("ERROR: Spark cannot use the Hadoop file system here (winutils.exe / hadoop.dll missing?). "
                 "Fix the environment, or explicitly choose the fallback: --reader paths --writer python")
    try:
        sc.addPyFile(os.path.join(ROOT, "extractor.py"))   # ships the code to the executors (cluster)
    except Exception as e:                                  # e.g. Windows without winutils: PYTHONPATH is enough locally
        print(f"addPyFile skipped ({type(e).__name__}); local workers use PYTHONPATH={ROOT}")

    # one Python worker process per partition (on Windows PySpark starts a NEW interpreter for every task),
    # so use few large partitions: a few per core, not hundreds
    n_parts = args.partitions or sc.defaultParallelism * 4
    if reader == "spark":
        files = (spark.read.format("binaryFile").option("pathGlobFilter", GLOB)
                 .load(INPUT).select("path", "content").coalesce(n_parts))
        raw = files.rdd.map(lambda r: (r.path, r.content))
    else:
        paths = sorted(glob.glob(os.path.join(INPUT, GLOB)))
        if not paths:
            spark.stop()
            sys.exit(f"No files {GLOB} in {INPUT}")
        raw = sc.parallelize(paths, min(len(paths), n_parts)).mapPartitions(read_partition)
    parsed = spark.createDataFrame(raw.mapPartitions(parse_partition), PARSED_SCHEMA,
                                  verifySchema=False)
    parsed = parsed.persist(StorageLevel.MEMORY_AND_DISK)      # parsed once, used by the output and the statistics

    ok = parsed.filter("error IS NULL").dropDuplicates(["driver_id"])
    out, sids = build_wide(ok)
    if writer == "spark":
        (out.coalesce(1).write.mode("overwrite")
            .option("ignoreNullFields", "false")                   # keep "s__f2__wins": null explicitly
            .json(OUT))
        part = glob.glob(os.path.join(OUT, "part-*.json"))
        if len(part) != 1:
            spark.stop()
            sys.exit(f"Expected exactly one part file in {OUT}, found {len(part)}.")
        os.makedirs(os.path.dirname(SINGLE_FILE) or ".", exist_ok=True)
        shutil.move(part[0], SINGLE_FILE)
    else:
        write_jsonl_local(out, SINGLE_FILE)

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
    stats["reader"], stats["writer"] = reader, writer
    stats["fallback_used"] = reader != "spark" or writer != "spark"
    stats["single_file"] = SINGLE_FILE

    os.makedirs(os.path.dirname(STATS_FILE) or ".", exist_ok=True)
    with open(STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    spark.stop()


if __name__ == "__main__":
    sys.path.insert(0, ROOT)
    main()