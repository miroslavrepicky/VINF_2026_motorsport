"""
Driver page extractor (Motor Sport Magazine database) -> ONE JSONL file.
Regex + stdlib only (html.unescape just decodes entities), no HTML parsers.

Input:  data/html/database_drivers_<slug>.html
Output: data/drivers.jsonl        one line per driver (the ONLY data file)
        data/extract_stats.json   extraction statistics + the full list of problems

Wide format: every driver has the statistics columns of EVERY series that exists in the data set,
named  s__<series_id>__<stat>  (stat = starts, wins, poles, podiums, years). A series the driver
never took part in has the value null in all of its columns. "years" is the list of all years
(the range 'Years in Database' is expanded: 1958 - 1962 -> [1958, 1959, 1960, 1961, 1962]).
`series_names` = {series_id: name} only for the series the driver took part in.
`seasons` = every row of the 'Championship Seasons' table, e.g. "1968 F1 World Championship - champion"
(the suffix is added when the Position of that season is 1).

Incremental: existing records are kept; a driver found in the HTML is overwritten, otherwise appended.
The set of series columns is recomputed on every run, so new series add columns to all records.
Use --rebuild to build the file from scratch (needed once after the change from the old multi-file format).
"""
import argparse
import datetime
import glob
import html
import json
import os
import re
from collections import Counter

# ---------- regexes ----------
FLAGS = re.DOTALL

CANONICAL_RE = re.compile(r'<link rel="canonical" href="(https?://[^/"]+/database/drivers/([a-z0-9\-]+)/)"')
NAME_RE = re.compile(r'<h1 class="hero__title[^"]*">(.*?)</h1>', FLAGS)
TAGLINE_RE = re.compile(r'<p class="hero__text">(.*?)</p>', FLAGS)
# some pages have the canonical in the form /database/?post_type=driver&p=ID -> take the slug from the oEmbed link
OEMBED_RE = re.compile(r'rel="alternate"[^>]*oembed[^>]*%2Fdatabase%2Fdrivers%2F([a-z0-9\-]+)%2F')
SITE_DRIVER_URL = "https://www.motorsportmagazine.com/database/drivers/{}/"
GENDER_RE = re.compile(r'<main class="[^"]*\bgender-(\w+)')
WP_ID_RE = re.compile(r'data-behaviour="driver-data-box" data-driver-id="(\d+)"')

# the data box with the driver's vital data and per-series statistics
BOX_RE = re.compile(r'data-behaviour="driver-data-box"[^>]*>(.*?)(?=database-data__expand)', FLAGS)

# generic "label -> value" field inside the data box ({label} is filled in by simple_field)
SIMPLE_FIELD = r'database-data__head">{label}</div><div class="database-data__content[^"]*">(.*?)</div>'
NATIONALITY_RE = re.compile(
    r'database-data__head">Nationality</div><div class="database-data__content[^"]*">'
    r'(?:<div class="flag[^>]*>.*?</div>)?([^<]*)</div>', FLAGS)
LAST_RACE_RE = re.compile(
    r'database-data__head">Recent Race</div><div class="database-data__content[^"]*">'
    r'<a href="[^"]*/races/([^/"]+)/">([^<]+)</a>', FLAGS)

# date like '2nd November 2005', 'November 2005' or '2005'
DATE_RE = re.compile(r'^\s*(?:(\d{1,2})(?:st|nd|rd|th)\s+)?(?:([A-Za-z]+)\s+)?(\d{4})\b')
CIRCA_RE = re.compile(r'^\s*(?:circa|ca\.?|c\.)(?=\s|\d)\s*', re.IGNORECASE)
UNKNOWN_RE = re.compile(r'^\s*(?:-\s*)?unknown\b', re.IGNORECASE)
MONTHS = {m: i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July", "August",
     "September", "October", "November", "December"], 1)}

# series: the <option> list gives the names, the stats blocks give the numbers (both keyed by series id)
SERIES_OPTION_RE = re.compile(r'<option data-target="([^"]+)">([^<]+)</option>')
SERIES_BLOCK_RE = re.compile(
    r'<div data-target="([^"]+)" class="database-data__seasons-stats[^"]*">(.*?)'
    r'(?=<div data-target="[^"]+" class="database-data__seasons-stats|<div class="database-data__expand"|$)',
    FLAGS)
YEARS_RE = re.compile(
    r'Years in Database</div><div class="database-data__content[^"]*">(\d+)'
    r'(?:\s*\((\d{4})(?:\s*-\s*(\d{4}))?\))?', FLAGS)
NUM_FIELD = r'database-data__head">{label}</div><div class="database-data__content[^"]*">\s*(\d+)'

BIO_SECTION_RE = re.compile(r'<section class="c-expanding-content(.*?)expanding-content__buttons', FLAGS)
# without the 'Read full bio' button (short bio) the section ends at the first </section>
BIO_SECTION_NOBTN_RE = re.compile(r'<section class="c-expanding-content(.*?)</section>', FLAGS)
BIO_BLOCK_RE = re.compile(r'<(p|h3|h4)\b[^>]*>(.*?)</\1>', FLAGS)
TEAMS_SECTION_RE = re.compile(r'id="teams">(.*?)</section>', FLAGS)
TEAM_RE = re.compile(r'/database/teams/[^/"]+/">([^<]+)</a>')
TAG_RE = re.compile(r'<[^>]+>')

# 'Championship Seasons' table: one <tr> per season (extra entrants of a season have an empty first cell)
SEASONS_SECTION_RE = re.compile(r'id="championship-seasons">(.*?)</section>', FLAGS)
ROW_RE = re.compile(r'<tr class="database-table__row[^"]*">(.*?)</tr>', FLAGS)
CELL_RE = re.compile(r'<td[^>]*>(.*?)</td>', FLAGS)
POSITION_CELL = 6              # Season | Entrant | Starts | Poles | Podiums | Wins | Position | Points | Results
CHAMPION_SUFFIX = " - champion"   # appended to the season when the position is 1 (use " - winner" if preferred)

# wide format of the series statistics
SERIES_COLS = ("starts", "wins", "poles", "podiums", "years")
COL_PREFIX = "s__"


# ---------- helper functions ----------
def clean(text):
    """Strip tags, decode entities, normalize whitespace."""
    text = html.unescape(TAG_RE.sub("", text)).replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def simple_field(box, label):
    """Return the cleaned text of the data-box field with the given label, or None."""
    m = re.search(SIMPLE_FIELD.format(label=label), box, FLAGS)
    return clean(m.group(1)) if m else None


def num_field(block, label):
    """Return the integer value of a numeric field (Starts, Wins...) in a series block, or None."""
    m = re.search(NUM_FIELD.format(label=label), block, FLAGS)
    return int(m.group(1)) if m else None


def read_jsonl(path):
    """Yield one parsed JSON object per non-empty line; a missing file gives nothing."""
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def write_jsonl(path, rows):
    """Write one JSON object per line via a temporary file, so a crash cannot corrupt the output."""
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    os.replace(tmp, path)


def parse_date(raw):
    """
    '2nd November 2005 - Athens' -> ('2005-11-02', 'full').
    The rest (age, place) is ignored. A missing day/month gives a partial date
    ('2005-11', '2005'). An invalid format -> (None, 'unparsed').
    No value -> (None, 'missing'); 'Unknown' -> (None, 'unknown');
    only a place without a date -> (None, 'place_only'). The output is null in these cases.
    """
    if not raw:
        return None, "missing"
    if UNKNOWN_RE.match(raw):
        return None, "unknown"          # 'Unknown' (also 'Unknown - place')
    cm = CIRCA_RE.match(raw)
    if cm:                              # 'circa 1950 - Paris' -> ('1950', 'circa')
        iso, prec = parse_date(raw[cm.end():])
        return (iso, "circa") if iso else (None, prec)
    if not re.search(r"\d", re.sub(r"\(.*?\)", "", raw)):
        return None, "place_only"       # only a place, no date ('- Athens')
    m = DATE_RE.match(raw)
    if not m:
        return None, "unparsed"
    day, month, year = m.groups()
    if month is not None and month not in MONTHS:
        return None, "unparsed"
    if day and not month:
        return None, "unparsed"
    if day:
        try:
            datetime.date(int(year), MONTHS[month], int(day))    # reject impossible dates (e.g. 31 February)
        except ValueError:
            return None, "unparsed"
        return f"{year}-{MONTHS[month]:02d}-{int(day):02d}", "full"
    if month:
        return f"{year}-{MONTHS[month]:02d}", "month"
    return year, "year"


AGE_RE = re.compile(r"\s*\(\s*Age[^)]*\)", re.IGNORECASE)


def vital_text(label, raw):
    """'Born: 2nd November 2005 - Athens'. The age '(Age N)' is left out, as are 'Unknown' and empty values."""
    if not raw or UNKNOWN_RE.match(raw):
        return None
    v = re.sub(r"^\s*-\s*", "", AGE_RE.sub("", raw))
    v = re.sub(r"\s+", " ", v).strip(" -")
    return f"{label}: {v}" if v else None


def col(sid, stat):
    """Name of the column holding statistic `stat` of series `sid`."""
    return f"{COL_PREFIX}{sid}__{stat}"


def expand_years(year_from, year_to):
    """(1958, 1962) -> [1958, ..., 1962]; (1960, None) -> [1960]; no start year -> None."""
    if year_from is None:
        return None
    year_to = year_from if year_to is None else year_to
    lo, hi = sorted((year_from, year_to))
    return list(range(lo, hi + 1))


def extract_seasons(text):
    """All seasons of the 'Championship Seasons' table as strings; position 1 adds the champion suffix."""
    sec = SEASONS_SECTION_RE.search(text)
    out = []
    for row in ROW_RE.findall(sec.group(1)) if sec else []:
        cells = CELL_RE.findall(row)
        if len(cells) <= POSITION_CELL:
            continue                                   # hidden / spacer row
        season = clean(cells[0])
        if not season:
            continue                                   # continuation row (another entrant of the same season)
        out.append(season + (CHAMPION_SUFFIX if clean(cells[POSITION_CELL]) == "1" else ""))
    return out


def normalize_record(rec, all_sids):
    """Fixed column layout: the base fields followed by every series column (null where missing)."""
    out = {k: v for k, v in rec.items() if not k.startswith(COL_PREFIX)}
    for sid in all_sids:
        for stat in SERIES_COLS:
            out[col(sid, stat)] = rec.get(col(sid, stat))
    return out


# ---------- extraction of a single page ----------
def extract_page(text):
    """Return (driver_dict incl. series columns and seasons, [series_dict for the statistics]) or (None, [])."""
    m = CANONICAL_RE.search(text)
    n = NAME_RE.search(text)
    if m:
        url, slug = m.group(1), m.group(2)
    else:
        o = OEMBED_RE.search(text)
        slug = o.group(1) if o else None
        url = SITE_DRIVER_URL.format(slug) if slug else None
    if not slug or not n:
        return None, []

    box_m = BOX_RE.search(text)
    box = box_m.group(1) if box_m else ""

    wp = WP_ID_RE.search(text)
    gender = GENDER_RE.search(text)
    tagline = TAGLINE_RE.search(text)
    nat = NATIONALITY_RE.search(box)
    last = LAST_RACE_RE.search(box)

    born_raw = simple_field(box, "Born")
    died_raw = simple_field(box, "Died")
    born_date, born_prec = parse_date(born_raw)
    died_date, died_prec = parse_date(died_raw)

    # biography
    bio = None
    bs = BIO_SECTION_RE.search(text) or BIO_SECTION_NOBTN_RE.search(text)
    has_bio_section = bool(bs)
    if bs:
        parts = [clean(b.group(2)) for b in BIO_BLOCK_RE.finditer(bs.group(1))
                 if "product-card" not in b.group(0)[:120]]       # skip embedded product cards
        parts = [p for p in parts if p]
        if parts:
            bio = re.sub(r"\s+", " ", " ".join(parts)).strip()   # no newlines

    # prepend the full born/died data (including the place) to the bio text so it is searchable
    real_bio = bio is not None
    head = [t for t in (vital_text("Born", born_raw), vital_text("Died", died_raw)) if t]
    if head:
        bio = ". ".join(head) + "." + (" " + bio if bio else "")

    # teams
    teams = []
    ts = TEAMS_SECTION_RE.search(text)
    if ts:
        teams = [clean(t) for t in TEAM_RE.findall(ts.group(1))]

    driver = {
        "driver_id": slug,
        "wp_id": int(wp.group(1)) if wp else None,
        "url": url,
        "name": clean(n.group(1)),
        "full_name": simple_field(box, "Full name"),
        "gender": gender.group(1) if gender else None,
        "nationality": clean(nat.group(1)) if nat and clean(nat.group(1)) else None,
        "born_date": born_date,
        "died_date": died_date,
        "last_race_slug": last.group(1) if last else None,
        "last_race_name": clean(last.group(2)) if last else None,
        "tagline": clean(tagline.group(1)) if tagline and clean(tagline.group(1)) else None,
        "bio": bio,
        "teams": teams,
        "_born_prec": born_prec,   # statistics only, removed before writing
        "_died_prec": died_prec,
        "_has_bio_section": has_bio_section,
        "_real_bio": real_bio,
        "_born_raw": born_raw,
        "_died_raw": died_raw,
    }

    # series
    names = {sid: clean(name) for sid, name in SERIES_OPTION_RE.findall(box)}
    series = []
    for sid, block in SERIES_BLOCK_RE.findall(box):
        y = YEARS_RE.search(block)
        years_count = int(y.group(1)) if y else None
        year_from = int(y.group(2)) if y and y.group(2) else None
        year_to = int(y.group(3)) if y and y.group(3) else year_from
        years = expand_years(year_from, year_to)
        series.append({
            "series_id": sid,
            "series_name": names.get(sid),
            "years_count": years_count,      # only for the statistics (compared with len(years))
            "years": years,
            "starts": num_field(block, "Starts"),
            "wins": num_field(block, "Wins"),
            "poles": num_field(block, "Poles"),
            "podiums": num_field(block, "Podiums"),
        })
    driver["seasons"] = extract_seasons(text)
    driver["series_names"] = {s["series_id"]: s["series_name"] or s["series_id"] for s in series}
    for s in series:
        for stat in SERIES_COLS:
            driver[col(s["series_id"], stat)] = s[stat]
    return driver, series


# ---------- main run ----------
def main():
    ap = argparse.ArgumentParser(description="Driver page extractor -> JSONL")
    ap.add_argument("--input", default="data/html", help="folder with the HTML files")
    ap.add_argument("--out", default="data", help="output folder")
    ap.add_argument("--pattern", default="*.html", help="glob pattern of the input files")
    ap.add_argument("--rebuild", action="store_true",
                    help="ignore the existing JSONL files and build them from scratch "
                         "(by default existing records are kept and only updated/added)")
    args = ap.parse_args()

    files = sorted(glob.glob(os.path.join(args.input, args.pattern)))
    os.makedirs(args.out, exist_ok=True)
    drivers_path = os.path.join(args.out, "drivers.jsonl")

    # existing data (incremental mode): driver_id -> record
    drivers_db = {}
    if not args.rebuild:
        drivers_db = {d["driver_id"]: d for d in read_jsonl(drivers_path)}
        print(f"Loaded existing data: {len(drivers_db)} drivers.")
        if any("series_names" not in d for d in drivers_db.values()):
            print("WARNING: the file contains records in the old format (no series_names); "
                  "run with --rebuild to regenerate everything.")

    st = Counter()                 # extraction statistics (of this run only)
    series_counter = Counter()     # number of drivers per series id
    problems = []
    seen = set()                   # driver ids already processed in this run (duplicate protection)

    counter = 0
    for path in files:
        counter += 1
        st["files"] += 1
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
        drv, series = extract_page(text)
        if drv is None:
            st["skipped_not_driver_page"] += 1
            problems.append({"file": os.path.basename(path), "issue": "not_driver_page"})
            continue
        if drv["driver_id"] in seen:
            st["skipped_duplicate"] += 1
            continue
        seen.add(drv["driver_id"])

        st["drivers"] += 1
        st["updated_existing" if drv["driver_id"] in drivers_db else "added_new"] += 1
        st["with_bio" if drv["_real_bio"] else "without_bio"] += 1   # only a real bio, not the born/died text
        st["with_born_died_text"] += 1 if drv["bio"] and not drv["_real_bio"] else 0
        st["with_series" if series else "without_series"] += 1
        st["with_seasons" if drv["seasons"] else "without_seasons"] += 1
        st["champion_seasons"] += sum(x.endswith(CHAMPION_SUFFIX) for x in drv["seasons"])
        st["with_teams" if drv["teams"] else "without_teams"] += 1
        st["with_nationality" if drv["nationality"] else "without_nationality"] += 1
        st["with_last_race" if drv["last_race_slug"] else "without_last_race"] += 1
        st["with_full_name" if drv["full_name"] else "without_full_name"] += 1
        st[f"born_{drv['_born_prec']}"] += 1
        st[f"died_{drv['_died_prec']}"] += 1
        for label in ("born", "died"):
            if drv[f"_{label}_prec"] == "unparsed":
                problems.append({"file": os.path.basename(path), "driver_id": drv["driver_id"],
                                 "issue": f"unparsed_{label}", "raw": drv[f"_{label}_raw"]})
        if drv["died_date"] and not drv["born_date"]:
            st["died_without_born"] += 1
        if drv["born_date"] and drv["died_date"] and drv["died_date"] < drv["born_date"]:
            st["died_before_born"] += 1
            problems.append({"file": os.path.basename(path), "driver_id": drv["driver_id"],
                             "issue": "died_before_born",
                             "raw": f"{drv['born_date']} / {drv['died_date']}"})
        for s in series:
            series_counter[s["series_id"]] += 1
            if None in (s["starts"], s["wins"], s["poles"], s["podiums"]):
                st["series_rows_with_null_stat"] += 1
            if s["years"] is None:
                st["series_without_years"] += 1
            elif s["years_count"] != len(s["years"]):
                st["series_years_with_gaps"] += 1   # the range has more years than 'Years in Database' counts
        st["series_rows"] += len(series)

        # remove the helper fields (they were only needed for the statistics above)
        drv.pop("_born_prec")
        drv.pop("_died_prec")
        if drv.pop("_has_bio_section") and not drv["_real_bio"]:
            st["bio_section_but_empty"] += 1
            problems.append({"file": os.path.basename(path), "driver_id": drv["driver_id"],
                             "issue": "bio_section_but_empty"})
        drv.pop("_real_bio")
        drv.pop("_born_raw")
        drv.pop("_died_raw")
        # an existing driver is overwritten in place (keeps its position), a new one is appended
        drivers_db[drv["driver_id"]] = drv
        print(counter)

    # one column set for everybody: every series that occurs anywhere gets its columns in every record
    all_sids = sorted({sid for d in drivers_db.values() for sid in d.get("series_names", {})})
    write_jsonl(drivers_path, (normalize_record(d, all_sids) for d in drivers_db.values()))
    st["total_drivers_in_file"] = len(drivers_db)
    st["series_columns_groups"] = len(all_sids)

    n = st["drivers"] or 1
    stats = dict(st)
    stats["pct_with_bio"] = round(100 * st["with_bio"] / n, 2)
    stats["pct_with_series"] = round(100 * st["with_series"] / n, 2)
    stats["avg_series_per_driver_with_series"] = (
        round(st["series_rows"] / st["with_series"], 2) if st["with_series"] else 0)
    stats["top_series"] = series_counter.most_common(15)
    stats["distinct_series"] = len(series_counter)
    stats["problems_total"] = len(problems)
    stats["problems"] = problems          # the full list (file name, driver, issue, raw value)

    with open(os.path.join(args.out, "extract_stats.json"), "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    print(json.dumps({k: v for k, v in stats.items() if k != "problems"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()