"""
Spark join: Motor Sport Magazine drivers (drivers.jsonl) x Wikipedia drivers (wiki_drivers.jsonl)
-> ONE JSONL with a FULL OUTER join (+ the list of links + join statistics).

    spark-submit spark_jobs/join_drivers.py
    python spark_jobs/join_drivers.py                           (plain python = local[*] Spark)

Paths are fixed (constants below): data/drivers.jsonl x data/wiki_drivers.jsonl -> data/spark/joined
-> data/joined_drivers.jsonl.

The default is pure Spark + Hadoop (spark.read.json, df.write.json). The only option is an EXPLICIT fallback for
environments where the Hadoop file system does not work (typically Windows without winutils.exe / hadoop.dll):
    --writer python    the files are read and written with plain Python (no Hadoop); prints a WARNING

The matching rules are implemented here as DataFrame joins; the shared helper functions (nat_set, name_similar,
wiki_span, series_kind) live in matching.py and are called from UDFs:
  1. candidates      MSM x wiki pairs that share a name key (norm_key of name / full_name  x  title_key / name / birth_name)
  2. tier per pair   0 = same birth year, 1 = year missing on one side (but NOT when the nationality differs AND the career
                     years do not overlap), 2 = years differ by exactly 1 (and the nationality does not contradict),
                     3 = rejected. Per MSM driver only the best tier counts; among the pairs of the best tier a page whose
                     TITLE is the driver's name beats pages that only carry the name in the infobox (sub-articles):
                       tier 0 -> name_year (high) | tier 1 -> name_only (medium, low if no year at all) |
                       tier 2 -> name_year_fuzzy (medium / low) | tier 3 -> rejected_by_year
                     several pairs in the best tier -> the career years (MSM series x wiki infobox) must overlap in exactly
                     one of them ('+years'), otherwise 'ambiguous'
  3. date_name       drivers WITHOUT any name candidate: same FULL birth date + the same surname / an almost equal name
                     (high; medium when the nationality or the career years contradict)
  Discipline check: every pair except an exact name+year match (and a high date_name) must be consistent: a wiki
                     motorcycle page only for MSM drivers that raced a motorcycle series (or have unknown / mixed series), a wiki
                     car page not for MSM drivers that raced only motorcycle series. The removed pairs are counted in the stats.
  4. conflicts       a wiki page claimed by several MSM drivers: the strongest claim wins, the others are 'superseded';
                     an equal medium/low claim -> 'ambiguous_tie'; an equal high claim is kept and flagged `conflict`

Output record (one line per MSM driver or wiki page; the two original records are kept untouched in `msm` / `wiki`):
  record_id    msm:<driver_id> | wiki:<page_id>
  source       both | msm_only | wiki_only
  link_status  linked | unmatched | ambiguous | rejected_by_year | superseded | ambiguous_tie   (wiki_only: unmatched)
  match_method, confidence, conflict           (only for source=both)
  wiki_kind    core | category | msm_rescue | likely_noise   (a flag only, nothing is dropped)
  name, nationality, born_date (the more precise one), died_date, url_msm, url_wiki
  msm          the whole MSM record (wide format) or null
  wiki         the whole Wikipedia record or null

Statistics (--stats): record counts per source, unique MSM drivers / wiki pages linked, methods and confidences, why the
rest was not linked, nationality agreement, how many wiki attributes were attached and which gaps of MSM they filled.
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
from pyspark.sql import SparkSession, Window, functions as F, types as T

HERE = os.path.dirname(os.path.abspath(__file__))


def find_root():
    """Folder that contains wiki_page.py and matching.py: this script's folder or one of the folders above it."""
    d = HERE
    for _ in range(4):
        if all(os.path.exists(os.path.join(d, f)) for f in ("wiki_page.py", "matching.py")):
            return d
        d = os.path.dirname(d)
    sys.exit(f"wiki_page.py + matching.py were not found in {HERE} or in the folders above it.")


ROOT = find_root()
sys.path.insert(0, ROOT)
import matching as je  # noqa: E402  (driver side: TOLERANCE)

# ---------- fixed locations ----------
# (relative to ROOT = the folder with wiki_page.py + matching.py, NOT to the current working directory, so the job
#  behaves the same from a terminal and from an IDE)
DATA = os.path.join(ROOT, "data")
MSM_FILE = os.path.join(DATA, "drivers.jsonl")           # drivers.jsonl of the Motor Sport Magazine drivers
WIKI_FILE = os.path.join(DATA, "wiki_drivers.jsonl")     # wiki_drivers.jsonl of the Wikipedia drivers
OUT = os.path.join(DATA, "spark", "joined")              # Spark writes part-*.json into this folder
SINGLE_FILE = os.path.join(DATA, "joined_drivers.jsonl") # the final single JSONL file
STAT_ORDER = ("starts", "wins", "poles", "podiums", "years")

NOISE_TITLE_RE = r"^(List of |Formula One drivers from |.+ drivers from )"
NOISE_INFOBOXES = ["f1 nationality", "horseracing personality", "officeholder", "office holder", "football biography",
                   "musical artist", "film", "video game", "video game player", "monument", "television",
                   "youtube channel", "social media personality", "nfl biography", "afl biography",
                   "rugby biography", "baseball biography", "basketball biography", "gridiron football biography",
                   "cricketer", "tennis biography", "boxer", "golfer", "swimmer", "field hockey player",
                   "professional wrestler", "sport wrestler", "comics creator", "chef", "architect", "scientist",
                   "writer", "serial killer", "criminal", "adult biography", "religious biography", "minister"]
METHOD_RANK = {"name_year": 0, "date_name": 1, "name_year_fuzzy": 2, "name_only": 3}


# ---------- Hadoop probe / IO helpers (the same idea as in html_extractor.py) ----------
def hadoop_io_ok(spark):
    """Can Spark use the Hadoop file system here? On Windows it needs winutils.exe + hadoop.dll; Linux/macOS need nothing."""
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


def load_jsonl(spark, path, python_fallback):
    """JSONL -> DataFrame (schema inferred). Default: spark.read.json. FALLBACK (no Hadoop): the lines are read by plain Python."""
    if not python_fallback:
        return spark.read.json(path)
    with open(path, "r", encoding="utf-8") as f:
        lines = [ln for ln in f if ln.strip()]
    return spark.read.json(spark.sparkContext.parallelize(lines, max(1, min(len(lines) // 2000 + 1, 32))))


def write_jsonl_local(df, path):
    """FALLBACK writer: write the rows from the driver with plain Python (no Hadoop file system): one JSONL file."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for row in df.toLocalIterator():
            f.write(json.dumps(row.asDict(recursive=True), ensure_ascii=False) + "\n")
    os.replace(tmp, path)


# ---------- UDFs (they only call the shared helpers; the modules are shipped to the executors) ----------
def make_udfs():
    str_arr = T.ArrayType(T.StringType())

    def _keys(*vals):
        from wiki_page import norm_key
        return sorted({k for k in (norm_key(v or "") for v in vals) if k})

    def _nat(s):
        import matching as j
        return sorted(j.nat_set(s))

    def _wspan(js):
        import json as js_
        import matching as j
        sp = j.wiki_span({"series": js_.loads(js) if js else []})
        return list(sp) if sp else None

    def _kind(js):
        import json as js_
        import matching as j
        return j.series_kind(js_.loads(js) if js else None)

    def _similar(a, b):
        import matching as j
        return j.name_similar(a or [], b or [])

    return {
        "keys2": F.udf(lambda a, b: _keys(a, b), str_arr),
        "keys3": F.udf(lambda a, b, c: _keys(a, b, c), str_arr),
        "nat": F.udf(_nat, str_arr),
        "wspan": F.udf(_wspan, T.ArrayType(T.IntegerType())),
        "similar": F.udf(_similar, T.StringType()),
        "kind": F.udf(_kind, T.StringType()),
    }


def overlap_expr(m_lo, m_hi, w_lo, w_hi):
    """Career ranges overlap (with the tolerance of matching.py); NULL when one of them is unknown."""
    return (F.col(m_lo) - je.TOLERANCE <= F.col(w_hi)) & (F.col(w_lo) - je.TOLERANCE <= F.col(m_hi))


def discipline_ok_expr():
    """The discipline check: kind of the MSM series x discipline of the wiki page (needs m_kind and w_disc)."""
    return (F.when(F.col("w_disc") == "motorcycle", F.col("m_kind") != "car")
             .when(F.col("w_disc") == "car", F.col("m_kind") != "moto")
             .otherwise(True))


def nat_agree_expr():
    """true / false when both nationalities are known (after normalization), NULL otherwise."""
    return F.when((F.size("m_nat") > 0) & (F.size("w_nat") > 0), F.arrays_overlap("m_nat", "w_nat"))


# ---------- merge of one MSM + one wiki record (the logic lives in merge_records.py) ----------
def make_merge_udf(series_cols):
    """UDF: (meta columns, msm struct, wiki struct) of ONE row -> the merged record as one JSON line. Works only on the
    two structs of the same row (no second join, no explode / regroup), so two drivers cannot be mixed."""
    def _merge(record_id, source, link_status, method, confidence, conflict, wiki_kind, msm, wiki):
        import json as js_
        import matching as j
        rec = j.merge_records(
            record_id, source, link_status, method, confidence, conflict, wiki_kind,
            msm.asDict(recursive=True) if msm is not None else None,
            wiki.asDict(recursive=True) if wiki is not None else None, series_cols)
        return js_.dumps(rec, ensure_ascii=False)
    return F.udf(_merge, T.StringType())


def write_lines_local(df, path):
    """FALLBACK writer (no Hadoop): the column `line` (one JSON per row) is written by plain Python."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for row in df.select("line").toLocalIterator():
            f.write(row["line"] + "\n")
    os.replace(tmp, path)


def merged_stats(out):
    """How many merged records have the merged fields filled (read from the JSON line)."""
    keys = ["teams", "series", "co_drivers", "categorisation", "seasons", "born_place", "lead", "bio", "fastest_laps", "championships", "top_tens", "best_pos"]
    res = {}
    for scope, df in (("all", out), ("both", out.filter(F.col("source") == "both"))):
        exprs = []
        for k in keys:
            v = F.get_json_object("line", "$." + k)
            exprs.append(F.sum((v.isNotNull() & ~v.isin("[]", "")).cast("int")).alias(k))
        row = df.agg(*exprs).first().asDict()
        res[scope] = {k: (v or 0) for k, v in row.items()}
    return res


# ---------- matching ----------
def name_candidates(m, w):
    """Steps 1 + 2: one decision per MSM driver that has at least one name candidate."""
    # t_exact = 1 when the shared key is the page TITLE (not only the name written in the infobox)
    pairs = (m.select("m_id", F.explode("m_keys").alias("key"))
              .join(w.select("w_id", "w_tkey", F.explode("w_keys").alias("key")), "key")
              .groupBy("m_id", "w_id").agg(F.max((F.col("key") == F.col("w_tkey")).cast("int")).alias("t_exact")))
    c = (pairs.join(m.select("m_id", "m_year", "m_nat", "m_lo", "m_hi", "m_kind"), "m_id")
              .join(w.select("w_id", "w_year", "w_nat", "w_lo", "w_hi", "w_disc"), "w_id"))
    # discipline check: only an exact name+year pair is exempt
    keep = F.coalesce(F.col("m_year") == F.col("w_year"), F.lit(False)) | discipline_ok_expr()
    removed = c.filter(~keep).select("m_id", "w_id")
    c = c.filter(keep)
    agree = nat_agree_expr()
    ov_known = overlap_expr("m_lo", "m_hi", "w_lo", "w_hi")                 # NULL when a career is unknown
    ov = F.coalesce(ov_known, F.lit(False))
    exact = F.coalesce(F.col("m_year") == F.col("w_year"), F.lit(False))
    compat = F.col("m_year").isNull() | F.col("w_year").isNull()
    diff1 = F.coalesce(F.abs(F.col("m_year") - F.col("w_year")) == 1, F.lit(False))
    # a year-less pair is only a namesake when BOTH are known and contradict: other nationality AND no overlapping career
    contra = F.coalesce(~agree, F.lit(False)) & F.coalesce(~ov_known, F.lit(False))
    tier = (F.when(exact, 0).when(compat, F.when(contra, 3).otherwise(1))
             .when(diff1 & (agree.isNull() | agree), 2).otherwise(3))
    c = (c.withColumn("tier", tier)
          .withColumn("ov", ov)
          .withColumn("support", F.coalesce(agree, F.lit(False)) | ov)
          .withColumn("best", F.min("tier").over(Window.partitionBy("m_id"))))
    pool = c.filter(F.col("tier") == F.col("best"))
    # a page titled exactly like the driver beats pages that only carry the name in the infobox ('Early life and career of X')
    pool = (pool.withColumn("has_title", F.max("t_exact").over(Window.partitionBy("m_id")))
                .filter((F.col("has_title") == 0) | (F.col("t_exact") == 1)))
    a = pool.groupBy("m_id").agg(
        F.first("best").alias("tier"), F.count("*").alias("n"),
        F.sum(F.col("ov").cast("int")).alias("n_ov"),
        F.max(F.when(F.col("ov"), F.col("w_id"))).alias("ov_page"),
        F.max("w_id").alias("one_page"),
        F.max(F.col("support").cast("int")).alias("support"),
        F.max(F.col("m_year").isNull().cast("int")).alias("m_year_null"),
        F.min(F.col("w_year").isNull().cast("int")).alias("w_year_all_null"),
        F.collect_list("w_id").alias("cand_pages"))
    base = F.when(F.col("tier") == 0, "name_year").otherwise("name_only")
    unique = F.col("n") == 1
    via_years = F.col("n_ov") == 1
    method = (F.when(F.col("tier") == 3, "rejected_by_year")
               .when(F.col("tier") == 2, F.when(unique, "name_year_fuzzy").otherwise("rejected_by_year"))
               .otherwise(F.when(unique, base)
                           .when(via_years, F.concat(base, F.lit("+years")))
                           .otherwise("ambiguous")))
    weak_conf = F.when((F.col("m_year_null") == 1) & (F.col("w_year_all_null") == 1), "low").otherwise("medium")
    conf = (F.when(F.col("tier") == 3, F.lit(None).cast("string"))
             .when(F.col("tier") == 2, F.when(unique, F.when(F.col("support") == 1, "medium").otherwise("low")))
             .otherwise(F.when(unique | via_years, F.when(F.col("tier") == 0, "high").otherwise(weak_conf))))
    page = (F.when(F.col("tier") == 3, F.lit(None).cast("long"))
             .when(F.col("tier") == 2, F.when(unique, F.col("one_page")))
             .otherwise(F.when(unique, F.col("one_page")).when(via_years, F.col("ov_page"))))
    return a.select("m_id", method.alias("method"), conf.alias("conf"), page.alias("page"), "cand_pages"), removed


def date_candidates(m, w, named_ids, udfs):
    """Step 3: drivers without any name candidate - the same full birth date and a similar name."""
    nd = (m.join(named_ids, "m_id", "left_anti").filter(F.col("m_full").isNotNull())
           .select("m_id", "m_keys", "m_full", "m_nat", "m_lo", "m_hi", "m_kind"))
    wd = (w.filter(F.col("w_full").isNotNull())
           .select("w_id", "w_keys", "w_full", "w_nat", "w_lo", "w_hi", "w_disc"))
    j = (nd.join(wd, nd["m_full"] == wd["w_full"])
           .withColumn("sim", udfs["similar"]("m_keys", "w_keys"))
           .filter(F.col("sim").isNotNull()))
    contra = (~F.coalesce(nat_agree_expr(), F.lit(True))) | (
        F.col("m_lo").isNotNull() & F.col("w_lo").isNotNull()
        & ~overlap_expr("m_lo", "m_hi", "w_lo", "w_hi"))
    j = j.withColumn("contra", contra.cast("int"))
    # discipline check: only a high date_name (nothing contradicting) is exempt
    keep = (F.col("contra") == 0) | discipline_ok_expr()
    removed = j.filter(~keep).select("m_id", "w_id")
    a = j.filter(keep).groupBy("m_id").agg(
        F.count("*").alias("n"), F.max("w_id").alias("one_page"),
        F.max("contra").alias("contra"), F.collect_list("w_id").alias("cand_pages"))
    unique = F.col("n") == 1
    return a.select(
        "m_id",
        F.when(unique, "date_name").otherwise("ambiguous").alias("method"),
        F.when(unique, F.when(F.col("contra") == 0, "high").otherwise("medium")).alias("conf"),
        F.when(unique, F.col("one_page")).cast("long").alias("page"),
        "cand_pages"), removed


def resolve_conflicts(dec):
    """Step 4: strongest claim of a wiki page wins. Returns (all decisions with `status`, `kept`, `conflict`)."""
    base = F.split("method", r"\+")[0]
    m_rank = (F.when(base == "name_year", METHOD_RANK["name_year"]).when(base == "date_name", METHOD_RANK["date_name"])
               .when(base == "name_year_fuzzy", METHOD_RANK["name_year_fuzzy"])
               .when(base == "name_only", METHOD_RANK["name_only"]).otherwise(4))
    c_rank = F.when(F.col("conf") == "high", 0).when(F.col("conf") == "medium", 1).otherwise(2)
    is_claim = F.col("conf").isNotNull() & F.col("page").isNotNull()
    wp = Window.partitionBy("page")
    claims = (dec.filter(is_claim).withColumn("strength", c_rank * 10 + m_rank)
                 .withColumn("best", F.min("strength").over(wp))
                 .withColumn("n_best", F.sum((F.col("strength") == F.col("best")).cast("int")).over(wp)))
    claims = claims.withColumn(
        "status", F.when(F.col("strength") > F.col("best"), "superseded")
                  .when((F.col("n_best") > 1) & (F.col("best") >= 10), "ambiguous_tie")
                  .otherwise(F.col("method")))
    claims = (claims.withColumn("kept", F.col("status") == F.col("method"))
                    .withColumn("n_kept", F.sum(F.col("kept").cast("int")).over(wp))
                    .withColumn("conflict", F.col("kept") & (F.col("n_kept") > 1)))
    rest = (dec.filter(~is_claim).withColumn("status", F.col("method"))
               .withColumn("kept", F.lit(False)).withColumn("conflict", F.lit(False)))
    cols = ["m_id", "method", "conf", "page", "status", "kept", "conflict"]
    return claims.select(*cols).unionByName(rest.select(*cols))


# ---------- statistics ----------
def counts(df, col):
    return {str(r[0]): r[1] for r in df.groupBy(col).count().collect()}


def compute_stats(out, linked, m_slim, w_slim, msm_total, wiki_total, wiki_fields, series_fields):
    both = out.filter(F.col("source") == "both")
    wiki_only = out.filter(F.col("source") == "wiki_only")
    msm_only = out.filter(F.col("source") == "msm_only")
    st = {"msm_total": msm_total, "wiki_total": wiki_total, "records_total": out.count()}
    st["records_by_source"] = counts(out, "source")
    uniq = both.agg(F.countDistinct("wiki.page_id").alias("w"), F.countDistinct("msm.driver_id").alias("m"),
                    F.count("*").alias("pairs")).first()
    st["unique_wiki_pages_joined"] = uniq["w"]
    st["unique_msm_drivers_linked"] = uniq["m"]
    st["pct_msm_linked"] = round(100 * uniq["m"] / max(1, msm_total), 2)
    st["pct_wiki_joined"] = round(100 * uniq["w"] / max(1, wiki_total), 2)
    st["wiki_pages_with_several_msm_drivers"] = both.filter(F.col("conflict")).select("wiki.page_id").distinct().count()
    st["linked_by_method"] = counts(both, "match_method")
    st["linked_by_confidence"] = counts(both, "confidence")
    st["linked_by_discipline"] = counts(both, "wiki.discipline")
    st["linked_wiki_kind"] = counts(both, "wiki_kind")
    st["msm_only_by_status"] = counts(msm_only, "link_status")
    st["wiki_only_by_discipline"] = counts(wiki_only, "wiki.discipline")
    st["wiki_only_by_kind"] = counts(wiki_only, "wiki_kind")
    st["wiki_only_top_infobox"] = [(r[0], r[1]) for r in wiki_only.groupBy("wiki.infobox_type").count()
                                   .orderBy(F.desc("count")).limit(15).collect()]

    # nationality agreement on the high-confidence links (an independent check: nationality is not used by the match)
    chk = (linked.filter(F.col("confidence") == "high")
                 .join(m_slim.select("m_id", "m_nat"), "m_id").join(w_slim.select("w_id", "w_nat"), "w_id")
                 .select(nat_agree_expr().alias("agree")))
    r = chk.agg(F.sum((F.col("agree") == True).cast("int")).alias("agree"),  # noqa: E712
                F.sum((F.col("agree") == False).cast("int")).alias("differ"),  # noqa: E712
                F.sum(F.col("agree").isNull().cast("int")).alias("missing")).first()
    st["nationality_on_high"] = {"agree": r["agree"] or 0, "differ": r["differ"] or 0, "not_comparable": r["missing"] or 0}

    # how much wiki data was attached to the linked records
    def has(f):
        return F.col(f"wiki.{f}").isNotNull() & (F.col(f"wiki.{f}") != "")
    attached = {f"wiki_{f}": has(f) for f in ("birth_name", "born_place", "died_date", "died_place", "number", "titles",
                                              "awards", "relatives", "current_team", "former_teams", "lead")
                if f in wiki_fields}
    attached["wiki_series_blocks"] = F.size("wiki.series") > 0

    def series_has(f):
        """At least one series block of the page has the statistic `f`."""
        return F.exists("wiki.series", lambda x: x[f].isNotNull())
    for f in ("fastest_laps", "championships", "top_tens", "best_pos", "podiums", "poles", "wins", "starts"):
        if f in series_fields:
            attached[f"wiki_series_with_{f}"] = series_has(f)
    gains = {
        "gain_born_date_filled": F.col("msm.born_date").isNull() & F.col("wiki.born_date").isNotNull(),
        "gain_born_date_more_precise": (F.length("msm.born_date") == 4) & (F.length("wiki.born_date") == 10),
        "gain_died_date_filled": F.col("msm.died_date").isNull() & F.col("wiki.died_date").isNotNull(),
        "gain_nationality_filled": F.col("msm.nationality").isNull() & F.col("wiki.nationality").isNotNull(),
    }
    conds = {**attached, **gains}
    row = both.agg(*[F.sum(c.cast("int")).alias(k) for k, c in conds.items()]).first().asDict()
    st["linked_records_with_attribute"] = {k: (v or 0) for k, v in row.items() if k in attached}
    st["gaps_of_msm_filled_by_wiki"] = {k: (v or 0) for k, v in row.items() if k in gains}
    st["wiki_only_records_with_series"] = wiki_only.filter(F.size("wiki.series") > 0).count()
    return st


# ---------- main ----------
def main():
    ap = argparse.ArgumentParser(
        description="Spark join MSM x Wikipedia drivers (full outer join). Default: pure Spark + Hadoop.")
    ap.add_argument("--links", default=os.path.join(DATA, "join_links.jsonl"),
                    help="one line per link (driver_id, page_id, method...)")
    ap.add_argument("--stats", default=os.path.join(DATA, "spark", "join_stats.json"),
                    help="statistics (local JSON file)")
    ap.add_argument("--writer", choices=["spark", "python"], default="spark",
                    help="spark = Spark reads/writes the files (default); python = FALLBACK without Hadoop: the files "
                         "are read and written with plain Python")
    ap.add_argument("--driver-memory", default="8g", help="driver JVM memory when started with plain `python`")
    args = ap.parse_args()
    writer = args.writer
    python_fallback = writer == "python"

    t0 = time.time()
    os.environ["PYTHONPATH"] = ROOT + os.pathsep + os.environ.get("PYTHONPATH", "")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    builder = (SparkSession.builder.appName("vinf-join-drivers")
               .config("spark.sql.shuffle.partitions", "8")         # tiny data: 200 default shuffle tasks are pure overhead
               .config("spark.ui.showConsoleProgress", "false"))
    if "PYSPARK_GATEWAY_PORT" not in os.environ:
        builder = builder.config("spark.driver.memory", args.driver_memory)
    spark = builder.getOrCreate()
    sc = spark.sparkContext

    # ---- the fallback is always announced; Spark/Hadoop that does not work is NOT silently replaced ----
    if python_fallback:
        print("WARNING: FALLBACK writer 'python' - the files are read and written with plain Python without "
              "Spark/Hadoop file IO (not suitable for big outputs).")
    elif not hadoop_io_ok(spark):
        spark.stop()
        sys.exit("ERROR: Spark cannot use the Hadoop file system here (winutils.exe / hadoop.dll missing?). "
                 "Fix the environment (diagnose with: python spark_jobs/check_spark_env.py), "
                 "or explicitly choose the fallback: --writer python")
    for f in ("wiki_page.py", "matching.py"):
        try:
            sc.addPyFile(os.path.join(ROOT, f))                    # ships the code to the executors (cluster)
        except Exception as e:
            print(f"addPyFile {f} skipped ({type(e).__name__}); local workers use PYTHONPATH={ROOT}")
    udfs = make_udfs()

    # ---- inputs ----
    msm = load_jsonl(spark, MSM_FILE, python_fallback)
    wiki = load_jsonl(spark, WIKI_FILE, python_fallback)
    msm_total, wiki_total = msm.count(), wiki.count()
    print(f"MSM drivers: {msm_total}, wiki records: {wiki_total}")

    # ---- slim views with the join keys ----
    year_cols = [c for c in msm.columns if c.startswith("s__") and c.endswith("__years")]
    empty = F.array().cast("array<long>")
    all_years = F.flatten(F.array(*[F.coalesce(F.col(c), empty) for c in year_cols])) if year_cols else empty
    m = msm.select(
        F.col("driver_id").alias("m_id"),
        udfs["keys2"]("name", "full_name").alias("m_keys"),
        F.when(F.col("born_date").rlike(r"^\d{4}"), F.substring("born_date", 1, 4).cast("int")).alias("m_year"),
        F.when(F.length("born_date") == 10, F.col("born_date")).alias("m_full"),
        udfs["nat"]("nationality").alias("m_nat"),
        F.array_min(all_years).cast("int").alias("m_lo"), F.array_max(all_years).cast("int").alias("m_hi"),
        udfs["kind"](F.to_json("series_names")).alias("m_kind"))
    w = (wiki.select(
            F.col("page_id").alias("w_id"),
            udfs["keys3"]("title_key", "name", "birth_name").alias("w_keys"),
            F.col("title_key").alias("w_tkey"),
            F.col("born_year").cast("int").alias("w_year"),
            F.when(F.length("born_date") == 10, F.col("born_date")).alias("w_full"),
            udfs["nat"]("nationality").alias("w_nat"),
            F.col("discipline").alias("w_disc"),
            udfs["wspan"](F.to_json("series")).alias("w_span"))
         .withColumn("w_lo", F.col("w_span")[0]).withColumn("w_hi", F.col("w_span")[1]).drop("w_span"))
    m = m.persist(StorageLevel.MEMORY_AND_DISK)
    w = w.persist(StorageLevel.MEMORY_AND_DISK)

    # ---- decisions ----
    d_name, removed_name = name_candidates(m, w)
    d_name = d_name.persist(StorageLevel.MEMORY_AND_DISK)
    d_date, removed_date = date_candidates(m, w, d_name.select("m_id"), udfs)
    dec = d_name.unionByName(d_date)
    final = resolve_conflicts(dec).persist(StorageLevel.MEMORY_AND_DISK)
    linked = (final.filter(F.col("kept"))
                   .select("m_id", F.col("page").alias("w_id"), F.col("method").alias("match_method"),
                           F.col("conf").alias("confidence"), "conflict"))
    status = final.select("m_id", F.when(F.col("kept"), "linked").otherwise(F.col("status")).alias("link_status"))

    # ---- full outer join of the original records ----
    msm_rec = msm.select(F.col("driver_id").alias("m_id"), F.struct(*[F.col(c) for c in msm.columns]).alias("msm"))
    title_noise = F.col("title").rlike(NOISE_TITLE_RE)
    ib = F.lower(F.coalesce(F.col("infobox_type"), F.lit("")))
    wiki_kind = (F.when((F.col("match_reason") == "infobox") & ~title_noise, "core")
                  .when(title_noise | ib.isin(*NOISE_INFOBOXES), "likely_noise")
                  .when(F.col("match_reason") == "msm_rescue", "msm_rescue")
                  .otherwise("category"))
    wiki_rec = wiki.select(F.col("page_id").alias("w_id"), F.struct(*[F.col(c) for c in wiki.columns]).alias("wiki"),
                           wiki_kind.alias("wiki_kind"))
    p_both = (linked.join(msm_rec, "m_id").join(wiki_rec, "w_id")
                    .select(F.concat(F.lit("msm:"), F.col("m_id")).alias("record_id"), F.lit("both").alias("source"),
                            F.lit("linked").alias("link_status"), "match_method", "confidence", "conflict",
                            "wiki_kind", "msm", "wiki"))
    p_msm = (msm_rec.join(linked.select("m_id"), "m_id", "left_anti").join(status, "m_id", "left")
                    .select(F.concat(F.lit("msm:"), F.col("m_id")).alias("record_id"), F.lit("msm_only").alias("source"),
                            F.coalesce("link_status", F.lit("unmatched")).alias("link_status"), "msm"))
    p_wiki = (wiki_rec.join(linked.select("w_id"), "w_id", "left_anti")
                      .select(F.concat(F.lit("wiki:"), F.col("w_id")).alias("record_id"), F.lit("wiki_only").alias("source"),
                              F.lit("unmatched").alias("link_status"), "wiki_kind", "wiki"))
    # one row = one MSM driver and/or one wiki page; the structs are MERGED (merge_records.py) into the `line` JSON
    pre = (p_both.unionByName(p_msm, allowMissingColumns=True).unionByName(p_wiki, allowMissingColumns=True)
                 .withColumn("conflict", F.coalesce("conflict", F.lit(False)))
                 .select("record_id", "source", "link_status", "match_method", "confidence", "conflict", "wiki_kind",
                         "msm", "wiki"))
    series_cols = sorted((c for c in msm.columns if c.startswith("s__")),
                         key=lambda c: (c[3:].rsplit("__", 1)[0],
                                        STAT_ORDER.index(c.rsplit("__", 1)[1]) if c.rsplit("__", 1)[
                                                                                      1] in STAT_ORDER else 99))
    merge = make_merge_udf(series_cols)
    out = (pre.withColumn("line", merge("record_id", "source", "link_status", "match_method", "confidence", "conflict",
                                        "wiki_kind", "msm", "wiki"))
              .orderBy("record_id"))
    out = out.persist(StorageLevel.MEMORY_AND_DISK)

    # ---- write ----
    os.makedirs(os.path.dirname(SINGLE_FILE) or ".", exist_ok=True)
    if writer == "spark":
        out.select("line").coalesce(1).write.mode("overwrite").text(OUT)     # the order of orderBy is kept
        part = glob.glob(os.path.join(OUT, "part-*"))
        if len(part) != 1:
            spark.stop()
            sys.exit(f"Expected exactly one part file in {OUT}, found {len(part)}.")
        shutil.move(part[0], SINGLE_FILE)
    else:
        write_lines_local(out, SINGLE_FILE)
    if args.links:
        write_jsonl_local(linked.select(F.col("m_id").alias("driver_id"), F.col("w_id").alias("page_id"),
                                        "match_method", "confidence", "conflict").orderBy("driver_id"), args.links)

    # ---- statistics ----
    wiki_fields = set(wiki.columns)
    series_t = wiki.schema["series"].dataType if "series" in wiki.columns else None
    series_fields = {f.name for f in series_t.elementType.fields} if isinstance(series_t, T.ArrayType) else set()
    stats = compute_stats(out, linked, m, w, msm_total, wiki_total, wiki_fields, series_fields)
    stats["msm_status_all"] = counts(final.select(F.when(F.col("kept"), "linked").otherwise(F.col("status"))
                                                 .alias("s")), "s")
    stats["msm_without_any_candidate"] = msm_total - final.count()
    stats["discipline_filter_removed_pairs"] = removed_name.count() + removed_date.count()
    stats["conflict_superseded"] = final.filter(F.col("status") == "superseded").count()
    stats["conflict_ties_ambiguous"] = final.filter(F.col("status") == "ambiguous_tie").count()
    stats.update({"reader": "python" if python_fallback else "spark", "writer": writer,
                  "fallback_used": python_fallback, "single_file": SINGLE_FILE,
                  "links_file": args.links, "output_columns": len(json.loads(out.select("line").first()["line"])), "seconds": round(time.time() - t0, 1)})
    stats["merged_fields_filled"] = merged_stats(out)
    if args.stats:
        os.makedirs(os.path.dirname(args.stats) or ".", exist_ok=True)
        with open(args.stats, "w", encoding="utf-8") as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()