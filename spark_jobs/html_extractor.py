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
import posixpath
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
DATA = os.environ.get("VINF_DATA", os.path.join(ROOT, "data"))   # local folder (default) or hdfs://... on a cluster


def dp(*parts):
    """Path inside DATA; works for a local folder as well as for an hdfs:// / s3a:// URL."""
    return posixpath.join(DATA, *parts) if "://" in DATA else os.path.join(DATA, *parts)


INPUT = dp("html")                   # folder with the HTML files
GLOB = "*.html"                                      # only HTML files are crawled
OUT = dp("spark", "drivers")         # Spark writes part-*.json into this folder
SINGLE_FILE = dp("drivers.jsonl")    # the final single JSONL file
STATS_FILE = dp("spark", "extract_stats.json")   # statistics

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


# ---------- environment independence: local disk or hdfs:// (the code is the same, only VINF_DATA differs) ----------
def is_local(path):
    """A plain path = local disk; anything with a scheme (hdfs://, s3a://, file://) goes through the Hadoop file system."""
    return "://" not in path


def _hfs(spark, path):
    p = spark._jvm.org.apache.hadoop.fs.Path(path)
    return p, p.getFileSystem(spark._jsc.hadoopConfiguration())


def exists(spark, path):
    if is_local(path):
        return os.path.exists(path)
    p, fs = _hfs(spark, path)
    return fs.exists(p)


def write_text(spark, path, text):
    """Small text file written from the driver: local disk or through the Hadoop file system."""
    if is_local(path):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp, path)
        return
    p, fs = _hfs(spark, path)
    out = fs.create(p, True)
    try:
        out.write(bytearray(text.encode("utf-8")))
    finally:
        out.close()


def finish_single_file(spark, out_dir, single_file):
    """Spark wrote exactly ONE part file into out_dir; move it to single_file.
    Local master + local path: glob + move. Cluster / hdfs://: the same move through the Hadoop file system API
    (on a cluster the part file is not on the driver's disk, so glob would find nothing)."""
    if is_local(out_dir) and spark.sparkContext.master.startswith("local"):
        part = glob.glob(os.path.join(out_dir, "part-*"))
        if len(part) != 1:
            spark.stop()
            sys.exit(f"Expected exactly one part file in {out_dir}, found {len(part)}.")
        os.makedirs(os.path.dirname(single_file) or ".", exist_ok=True)
        shutil.move(part[0], single_file)
        return
    src, fs = _hfs(spark, out_dir)
    parts = [s.getPath() for s in fs.listStatus(src) if s.getPath().getName().startswith("part-")]
    if len(parts) != 1:
        spark.stop()
        sys.exit(f"Expected exactly one part file in {out_dir}, found {len(parts)}.")
    dst = spark._jvm.org.apache.hadoop.fs.Path(single_file)
    fs.mkdirs(dst.getParent())
    fs.delete(dst, False)
    if not fs.rename(parts[0], dst):
        spark.stop()
        sys.exit(f"Could not move {parts[0]} to {single_file}.")


def ship_code(sc, *names):
    """Ship the modules to the executors. On a real cluster a failure is fatal (otherwise it shows up later as
    ModuleNotFoundError on the workers); with a local master PYTHONPATH is enough."""
    for name in names:
        try:
            sc.addPyFile(os.path.join(ROOT, name))
        except Exception as e:
            if not sc.master.startswith("local"):
                raise
            print(f"addPyFile {name} skipped ({type(e).__name__}); local workers use PYTHONPATH={ROOT}")


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
    if (reader == "paths" or writer == "python") and not (is_local(INPUT) and is_local(SINGLE_FILE)):
        spark.stop()
        sys.exit("The fallbacks --reader paths / --writer python work only with local folders (VINF_DATA is a URL).")
    ship_code(sc, "extractor.py")

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
        finish_single_file(spark, OUT, SINGLE_FILE)
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

    write_text(spark, STATS_FILE, json.dumps(stats, ensure_ascii=False, indent=2))
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    spark.stop()


if __name__ == "__main__":
    sys.path.insert(0, ROOT)
    main()