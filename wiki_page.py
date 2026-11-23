"""
wiki_page.py - extraction of ONE Wikipedia article (a <page> block of enwiki-latest-pages-articles.xml)
into ONE driver record. Regex + stdlib only, no wiki parsers. Used by spark_jobs/wiki_extractor.py the same
way html_extractor.py uses extractor.py: Spark only distributes the work, all the logic is here, so it can be
tested without Spark.

Which pages are kept (match_reason), decided in this order:
  infobox      a target infobox (racing driver, f1 driver, nascar driver, ..., speedway rider)   ~12 000 pages
  category     no such infobox, but a driver category: '<anything> drivers' (minus taxi/truck/bus/device/...)
               or motorcycle racers / motocross riders / speedway riders ...                    ~2 000 pages
  msm_rescue   neither, but the title equals the name of a Motor Sport Magazine driver, the birth year equals
               the MSM birth year AND the page has a motorsport hint (category / text). Catches occasional
               drivers (Trintignant, Cronkite...) while the name+year coincidences of politicians and
               footballers are dropped by the hint.
Redirects and pages outside the main namespace are dropped first.

Record (one JSON object per page): see extract_wiki_page().
"""
import datetime
import html
import re
import unicodedata
from urllib.parse import quote

# ---------- configuration ----------
TARGET_INFOBOXES = frozenset([
    "racing driver", "f1 driver", "nascar driver", "champ car driver", "le mans driver", "wrc driver",
    "rally driver", "motorcycle rider", "motorcycle racer", "speedway rider", "motocross rider"])
MOTO_INFOBOXES = frozenset(["motorcycle rider", "motorcycle racer", "speedway rider", "motocross rider"])
# series label of the plain fields (starts, wins...) of the old single-series infoboxes
INFOBOX_SERIES = {"f1 driver": "Formula One", "wrc driver": "World Rally Championship",
                  "champ car driver": "Champ Car", "le mans driver": "24 Hours of Le Mans"}

# driver categories: '<series / event / nationality> drivers' except the non-racing ones
DRIVERS_SUFFIX_RE = re.compile(r" drivers$", re.IGNORECASE)
NOISE_CAT_RE = re.compile(
    r"\b(?:taxi|truck|bus|train|coach|camel|carriage|hack|stagecoach|device|linux|display|water polo|fictional|"
    r"professional|serial killers|works about|ambulance|tram|school|delivery|chauffeurs?)\b|^dos drivers$",
    re.IGNORECASE)
MOTO_CAT_RE = re.compile(
    r"(?:motorcycle racers|motocross riders|speedway riders|Grand Prix motorcycle riders|"
    r"Superbike World Championship riders|sidecar racers|enduro riders)$", re.IGNORECASE)
# a motorsport hint for the msm_rescue path
MOTORSPORT_CAT_RE = re.compile(
    r"motorsport|racing|racer|formula (?:one|two|three|e|ford|renault|1|2|3)|le mans|rally|grand prix|nascar|"
    r"indycar|indianapolis|sports ?car|touring car|mille miglia|speedway|motocross|drivers$|riders$", re.IGNORECASE)
MOTORSPORT_TEXT_RE = re.compile(
    r"\b(?:racing driver|race car driver|racing team|motorsport|Formula (?:One|Two|Three|[123])|Grand Prix|"
    r"Le Mans|rally|NASCAR|IndyCar|Indianapolis 500|raced)\b")
# maintenance categories that say nothing about the person
MAINT_CAT_RE = re.compile(
    r"^(?:Living people|Articles |All articles|All Wikipedia|CS1|Webarchive|Pages |Use |Short description|"
    r"Wikipedia|Year of (?:birth|death)|Wikidata)|\b(?:births|deaths)$", re.IGNORECASE)

# ---------- regexes ----------
NS_RE = re.compile(r"<ns>(\d+)</ns>")
TITLE_RE = re.compile(r"<title>(.*?)</title>", re.DOTALL)
ID_RE = re.compile(r"<id>(\d+)</id>")
REDIRECT_RE = re.compile(r'<redirect title="')
INFOBOX_RE = re.compile(r"\{\{\s*[Ii]nfobox[ _]+([^|{}\n<]+?)\s*(?=[|\n}<]|&lt;)")
CAT_RE = re.compile(r"\[\[\s*[Cc]ategory\s*:\s*([^\]|]+)")
COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)
REF_RE = re.compile(r"<ref\b[^>]*?/>|<ref\b[^>]*>.*?</ref>", re.DOTALL | re.IGNORECASE)
TOK_RE = re.compile(r"\{\{|\}\}|\[\[|\]\]|\|")
PARAM_RE = re.compile(r"^\s*([A-Za-z0-9_\- ]+?)\s*=(.*)$", re.DOTALL)
NAMED_ARG_RE = re.compile(r"^\s*[A-Za-z][A-Za-z0-9_\- ]*=")
BIRTH_FIELD_RE = re.compile(r"\|\s*birth_date\s*=\s*([^\n]{0,120})", re.IGNORECASE)
YEAR_RE = re.compile(r"\b(1[5-9]\d\d|20\d\d)\b")

INNER_TPL_RE = re.compile(r"\{\{([^{}]*)\}\}")
FILE_LINK_RE = re.compile(r"\[\[\s*(?:File|Image)\s*:[^\[\]]*(?:\[\[[^\[\]]*\]\][^\[\]]*)*\]\]", re.IGNORECASE)
CATEGORY_LINK_RE = re.compile(r"\[\[\s*Category\s*:[^\]]*\]\]", re.IGNORECASE)
LINK_RE = re.compile(r"\[\[([^\]|\[]*)(?:\|([^\]\[]*))?\]\]")
EXT_LINK_RE = re.compile(r"\[(?:https?:)?//[^\s\]]+(?:\s+([^\]]*))?\]")
BR_RE = re.compile(r"<br\s*/?>", re.IGNORECASE)
TAG_RE = re.compile(r"</?[A-Za-z][^>]*>")
TABLE_RE = re.compile(r"\{\|.*?\|\}", re.DOTALL)
BLOCK_TPL_RE = re.compile(r"^[ \t]*\{\{", re.MULTILINE)


# ---------- text helpers ----------
def norm_name(s):
    """'Infobox_Racing  driver' -> 'racing driver'."""
    return re.sub(r"[\s_]+", " ", s).strip().lower()


def norm_key(s):
    """Name key (the same as tokenizer.py): no accents, lowercase, [a-z0-9] tokens joined by a space."""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    return " ".join(re.findall(r"[a-z0-9]+", s))


def title_key(title):
    """'Richard Marsh (racing driver)' -> 'richard marsh'."""
    return norm_key(re.sub(r"\s*\([^)]*\)\s*$", "", title or ""))


def parse_template(text, start):
    """
    text[start:start+2] == '{{'. Returns (name, [raw parameters], end_index).
    Parameters are split on '|' only at the top level of the template (nested {{ }} and [[ ]] are skipped).
    """
    depth, link, cur, end = 1, 0, start + 2, len(text)
    parts = []
    for m in TOK_RE.finditer(text, start + 2):
        t = m.group()
        if t == "{{":
            depth += 1
        elif t == "}}":
            depth -= 1
            if depth == 0:
                parts.append(text[cur:m.start()])
                end = m.end()
                break
        elif t == "[[":
            link += 1
        elif t == "]]":
            link = max(0, link - 1)
        elif depth == 1 and link == 0:            # '|'
            parts.append(text[cur:m.start()])
            cur = m.end()
    else:
        parts.append(text[cur:])                  # unterminated template: take the rest
    return (parts[0].strip() if parts else ""), parts[1:], end


def params_dict(parts):
    """{key: raw value} of the named parameters; keys are lowercase with '_' instead of spaces."""
    out = {}
    for p in parts:
        m = PARAM_RE.match(p)
        if m:
            out[m.group(1).strip().lower().replace(" ", "_")] = m.group(2).strip()
    return out


DROP_TPL_RE = re.compile(
    r"^(?:cite|citation|sfn|efn|refn|harv|main|see also|further|clear|reflist|authority|short description|use |"
    r"good article|featured|pp-|italic title|distinguish|redirect|about|hatnote|other uses|infobox|flagicon|"
    r"flagdeco|ill$|ipa|ipac|respell|audio|listen|convert$|-$|\.\.\.$)")
LIST_TPL = frozenset(["ubl", "unbulleted list", "hlist", "flatlist", "plainlist", "bulleted list", "bulleted"])
FIRST_ARG_TPL = frozenset(["flag", "flagcountry", "nowrap", "nobr", "small", "smaller", "larger", "big", "abbr",
                           "sort", "nts"])


def _tpl_text(m):
    parts = m.group(1).split("|")
    name = norm_name(parts[0])
    args = [a.strip() for a in parts[1:] if not NAMED_ARG_RE.match(a)]
    if DROP_TPL_RE.match(name):
        return ""
    if name in ("br", "break"):
        return "; "
    if name in LIST_TPL:
        return "; ".join(x for x in (re.sub(r"\s*\n\s*\*?\s*", "; ", a).strip(" *;") for a in args) if x)
    if name in FIRST_ARG_TPL:
        return args[0] if args else ""
    if name == "lang":
        return args[-1] if args else ""
    if name == "sortname":
        return " ".join(args[:2])
    if name in ("circa", "c.", "ca"):
        return "c. " + (args[0] if args else "")
    return " ".join(args)                         # unknown template: its positional arguments as text


def clean_wikitext(s):
    """Wikitext value -> plain text (templates, links, refs, comments, bold/italic, html tags removed)."""
    if not s:
        return ""
    s = REF_RE.sub("", COMMENT_RE.sub("", s))
    # links FIRST: _tpl_text splits a template on every '|', which would cut '[[Newcastle, NSW|Newcastle]]' in the middle
    # when the link is an argument of {{nowrap|...}} and leave a '[[' in the text
    s = FILE_LINK_RE.sub("", s)
    s = CATEGORY_LINK_RE.sub("", s)
    s = LINK_RE.sub(lambda m: m.group(2) or m.group(1), s)
    prev = None
    while prev != s:                              # innermost templates first
        prev = s
        s = INNER_TPL_RE.sub(_tpl_text, s)
    s = EXT_LINK_RE.sub(lambda m: m.group(1) or "", s)
    s = BR_RE.sub("; ", s)
    s = TAG_RE.sub("", s)
    s = s.replace("'''", "").replace("''", "").replace("\xa0", " ").replace("[[", "").replace("]]", "")
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"(?:;\s*){2,}", "; ", s)
    return s.strip(" ;,")


# ---------- dates ----------
MONTHS = {}
for _i, _m in enumerate(["january", "february", "march", "april", "may", "june", "july", "august", "september",
                         "october", "november", "december"], 1):
    MONTHS[_m] = MONTHS[_m[:3]] = _i
MONTHS["sept"] = 9
TPL_START_RE = re.compile(r"\{\{\s*([^|{}]+?)\s*(?=[|}])")
DMY_RE = re.compile(r"^(?:(\d{1,2})(?:st|nd|rd|th)?\s+)?(?:([A-Za-z]+)\.?\s+)?(\d{4})\b")
MDY_RE = re.compile(r"^([A-Za-z]+)\.?\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})\b")
ISO_RE = re.compile(r"^(\d{4})-(\d{2})(?:-(\d{2}))?\b")
CIRCA_PREFIX_RE = re.compile(r"^(?:circa|ca\.?|c\.|about|approximately|~)\s*", re.IGNORECASE)
DATE_WRAPPERS = frozenset(["nowrap", "nobr", "small", "circa", "c.", "ca"])


def _iso(y, mo, d, circa):
    """(iso string, precision) of a year / year-month / full date; an impossible date -> (None, 'unparsed')."""
    try:
        if d:
            datetime.date(y, mo, d)
        elif mo and not 1 <= mo <= 12:
            return None, "unparsed"
    except ValueError:
        return None, "unparsed"
    iso = f"{y:04d}-{mo:02d}-{d:02d}" if d else f"{y:04d}-{mo:02d}" if mo else f"{y:04d}"
    return iso, "circa" if circa else "full" if d else "month" if mo else "year"


def parse_text_date(s, circa=False):
    """'6 April 1982', 'January 25, 1976', '1982', '1982-04-06', 'c. 1900' -> (iso, precision)."""
    s = clean_wikitext(s)
    cm = CIRCA_PREFIX_RE.match(s)
    if cm:
        circa, s = True, s[cm.end():]
    if re.match(r"^\W*unknown", s, re.IGNORECASE):
        return None, "unknown"
    m = ISO_RE.match(s)
    if m:
        return _iso(int(m.group(1)), int(m.group(2)), int(m.group(3)) if m.group(3) else None, circa)
    m = MDY_RE.match(s)
    if m and m.group(1).lower() in MONTHS:
        return _iso(int(m.group(3)), MONTHS[m.group(1).lower()], int(m.group(2)), circa)
    m = DMY_RE.match(s)
    if m:
        day, mon, year = m.groups()
        if mon is not None and mon.lower() not in MONTHS:
            return None, "unparsed"
        if day and not mon:
            return None, "unparsed"
        return _iso(int(year), MONTHS[mon.lower()] if mon else None, int(day) if day else None, circa)
    return None, "unparsed"


BORN_YEARS = (1800, 2020)                      # a birth outside this range is a typo, not a driver (same in extractor.py)
DIED_YEARS = (1800, datetime.date.today().year)


def year_ok(year, kind):
    """Is the year plausible for a birth ('born') or a death ('died')?"""
    lo, hi = BORN_YEARS if kind == "born" else DIED_YEARS
    return lo <= year <= hi


def parse_wiki_date(raw, kind="born"):
    """_parse_wiki_date + the range check: a year outside BORN_YEARS / DIED_YEARS -> (None, 'out_of_range')."""
    iso, prec = _parse_wiki_date(raw)
    if iso and not year_ok(int(iso[:4]), kind):
        return None, "out_of_range"
    return iso, prec


def _parse_wiki_date(raw):
    """
    Value of birth_date / death_date -> (iso, precision). iso is 'YYYY-MM-DD', 'YYYY-MM' or 'YYYY';
    precision: full | month | year | circa | unknown | empty | missing | unparsed (out_of_range is added by parse_wiki_date).
    Handles {{birth date|1892|11|16|df=y}}, {{birth date and age|...}}, {{bda|...}}, {{death date and age|d|m|y|b...}}
    (the first three numbers are the date itself), {{birth-date|19 April 1895}}, {{birth year and age|1952}},
    {{circa|...}}, {{nowrap|...}} and plain text dates.
    """
    if raw is None:
        return None, "missing"
    v = COMMENT_RE.sub("", REF_RE.sub("", raw)).strip()
    if not v:
        return None, "empty"
    circa = False
    for _ in range(3):                            # unwrap {{nowrap|{{circa|{{death date ...}}}}}}
        m = TPL_START_RE.match(v)
        if not m or norm_name(m.group(1)) not in DATE_WRAPPERS:
            break
        _, parts, _ = parse_template(v, 0)
        args = [a.strip() for a in parts if not NAMED_ARG_RE.match(a)]
        if not args:
            return None, "unparsed"
        circa = circa or norm_name(m.group(1)) in ("circa", "c.", "ca")
        v = args[0]
    if TPL_START_RE.match(v):
        _, parts, _ = parse_template(v, 0)
        args = [a.strip() for a in parts if not NAMED_ARG_RE.match(a)]
        if not args:
            return None, "unparsed"
        if re.fullmatch(r"\d{3,4}", args[0]):
            mo = day = None
            if len(args) > 1 and re.fullmatch(r"\d{1,2}", args[1]):
                mo = int(args[1])
                if len(args) > 2 and re.fullmatch(r"\d{1,2}", args[2]):
                    day = int(args[2])
            return _iso(int(args[0]), mo, day, circa)
        return parse_text_date(args[0], circa)    # {{birth-date|19 April 1895}}
    return parse_text_date(v, circa)


def page_birth_year(text, cats):
    """Birth year of a page: from the 'YYYY births' category (every biography), else from birth_date."""
    for c in cats:
        if c.endswith(" births") and c[:4].isdigit() and year_ok(int(c[:4]), "born"):
            return int(c[:4])
    m = BIRTH_FIELD_RE.search(text)
    if m:
        y = YEAR_RE.search(m.group(1))
        if y and year_ok(int(y.group(1)), "born"):
            return int(y.group(1))
    return None


# ---------- infoboxes ----------
def find_infoboxes(text):
    """All infoboxes of a page, nested ones (modules, embed=yes) included: [{name, params, start, end, embedded}]."""
    boxes = []
    for m in INFOBOX_RE.finditer(text):
        _, parts, end = parse_template(text, m.start())
        boxes.append({"name": norm_name(m.group(1)), "params": params_dict(parts), "start": m.start(), "end": end})
    for i, b in enumerate(boxes):
        b["embedded"] = (b["params"].get("embed", "").lower() in ("yes", "y", "true", "1")
                         or any(o["start"] < b["start"] < o["end"] for o in boxes[:i]))
    return boxes


def pick_primary(boxes):
    """The infobox the person is described by: a top-level target one, else any target one, else a top-level one."""
    for pred in (lambda b: b["name"] in TARGET_INFOBOXES and not b["embedded"],
                 lambda b: b["name"] in TARGET_INFOBOXES,
                 lambda b: not b["embedded"]):
        for b in boxes:
            if pred(b):
                return b
    return None


def to_int(val):
    m = re.match(r"\s*(\d[\d,]*)", val or "")
    return int(m.group(1).replace(",", "")) if m else None


# statistics: plain keys belong to the infobox's own series, prefixed keys (cup_wins, gp_race_starts...) to <prefix>
PLAIN_STATS = {"starts": "starts", "races": "starts", "wins": "wins", "poles": "poles", "podiums": "podiums",
               "fastest_laps": "fastest_laps", "championships": "championships", "years_active": "years",
               "years": "years", "teams": "teams"}
PREFIXED_STATS = [
    (re.compile(r"^total_([a-z0-9]+?)_races$"), "starts"),
    (re.compile(r"^([a-z0-9]+?)_race_starts$"), "starts"),
    (re.compile(r"^([a-z0-9]+?)_(?:race_)?wins$"), "wins"),
    (re.compile(r"^([a-z0-9]+?)_poles$"), "poles"),
    (re.compile(r"^([a-z0-9]+?)_podiums$"), "podiums"),
    (re.compile(r"^([a-z0-9]+?)_fastest_laps$"), "fastest_laps"),
    (re.compile(r"^([a-z0-9]+?)_top_tens$"), "top_tens"),
    (re.compile(r"^([a-z0-9]+?)_championships$"), "championships"),
    (re.compile(r"^years_in_([a-z0-9]+)$"), "years"),
    (re.compile(r"^([a-z0-9]+?)_active_years$"), "years"),
    (re.compile(r"^([a-z0-9]+?)_(?:car_)?teams?$"), "teams"),
    (re.compile(r"^best_([a-z0-9]+?)_pos$"), "best_pos"),
]
NOT_A_SERIES = frozenset(["class", "race", "event", "stage", "first", "last", "career", "award", "starts", "total",
                          "former", "current", "prev", "previous", "best", "relatives", "module"])
INT_STATS = frozenset(["starts", "wins", "poles", "podiums", "fastest_laps", "top_tens", "championships"])


def series_blocks(box):
    """[{series, infobox, starts, wins, poles, podiums, fastest_laps, top_tens, championships, years, teams, best_pos}]"""
    groups = {}
    for k, v in box["params"].items():
        stat, label = PLAIN_STATS.get(k), None
        if stat is None:
            for rx, st in PREFIXED_STATS:
                m = rx.match(k)
                if m and m.group(1) not in NOT_A_SERIES:
                    stat, label = st, m.group(1)
                    break
        if stat is None:
            continue
        val = clean_wikitext(v)
        val = to_int(val) if stat in INT_STATS else val
        if val not in (None, ""):
            groups.setdefault(label, {})[stat] = val
    out = []
    for label, stats in groups.items():
        if label is None:                         # own series of the infobox
            label = INFOBOX_SERIES.get(box["name"])
            for key in ("last_series", "current_series", "series", "prev_series"):
                if not label and box["params"].get(key):
                    label = clean_wikitext(box["params"][key])[:80]
            label = label or box["name"]
        out.append({"series": label, "infobox": box["name"], **stats})
    return out


# ---------- categories, lead ----------
def is_driver_category(c):
    if not DRIVERS_SUFFIX_RE.search(c):
        return False
    return "monster truck" in c.lower() or not NOISE_CAT_RE.search(c)


def strip_block_templates(text):
    """Remove templates that start a line (infobox, hatnotes, short description...); inline ones stay."""
    out, pos = [], 0
    while True:
        m = BLOCK_TPL_RE.search(text, pos)
        if not m:
            break
        out.append(text[pos:m.start()])
        _, _, pos = parse_template(text, m.end() - 2)
    out.append(text[pos:])
    return "".join(out)


def lead_text(text, max_chars=2000, max_paragraphs=2):
    """First paragraph(s) of the article (before the first heading), as plain text."""
    t = strip_block_templates(text)
    h = re.search(r"^==", t, re.MULTILINE)
    if h:
        t = t[:h.start()]
    t = TABLE_RE.sub("", t)
    t = CATEGORY_LINK_RE.sub("", FILE_LINK_RE.sub("", t))
    paras = []
    for p in re.split(r"\n\s*\n", t):
        if p.lstrip().startswith(("*", ":", "#", "|", "!", "{", "=")):
            continue
        c = clean_wikitext(p)
        if len(c) >= 60:
            paras.append(c)
        if len(paras) >= max_paragraphs:
            break
    return " ".join(paras)[:max_chars] or None

# ---------- tables: names of teams, series and co-drivers ----------
TABLE_BLOCK_RE = re.compile(r"^\{\|.*?^\|\}", re.DOTALL | re.MULTILINE)
TABLE_HEAD_RE = re.compile(r"\b(?:teams?|entrants?|series|championship|co-?drivers?)\b", re.IGNORECASE)
INNER_LINK_RE = re.compile(r"\[\[([^\[\]]*)\]\]")
ROWSPAN_RE = re.compile(r"rowspan\s*=\s*[\"']?(\d+)", re.IGNORECASE)
COLSPAN_RE = re.compile(r"colspan\s*=\s*[\"']?(\d+)", re.IGNORECASE)
HEADER_SPLIT_RE = re.compile(r"\|\||!!")
TEAM_COLS = frozenset(["team", "teams", "team name", "entrant", "entrants"])
TABLE_SERIES_COLS = frozenset(["series", "series name", "championship"])
CODRIVER_COL_RE = re.compile(r"^co-?drivers?$")
JUNK_NAME_RE = re.compile(
    r"^(?:[-–—?]*|n/?a|tba|tbc|none|unknown|nc|dnf|ret|\d+(?:st|nd|rd|th)?)$", re.IGNORECASE)
_UNMASK = str.maketrans({"\x00": "|", "\x02": "{{", "\x03": "}}", "\x04": "[[", "\x05": "]]"})


def _mask(s):
    """Hide the '|' inside {{ }} and [[ ]] (and the brackets themselves), so every '|' left is table syntax."""
    for _ in range(6):
        t = INNER_TPL_RE.sub(lambda m: "\x02" + m.group(1).replace("|", "\x00") + "\x03", s)
        t = INNER_LINK_RE.sub(lambda m: "\x04" + m.group(1).replace("|", "\x00") + "\x05", t)
        if t == s:
            break
        s = t
    return s


def parse_table(tbl):
    """Wikitable -> [[(is_header, attrs, text)]] (one list per row)."""
    rows, cur = [], None
    for line in _mask(tbl).split("\n")[1:]:
        if line.startswith("|}"):
            break
        if line.startswith("|+") or not line.strip():
            continue
        if line.startswith("|-"):
            cur = None
        elif line[0] in "|!":
            if cur is None:
                cur = []
                rows.append(cur)
            for part in (HEADER_SPLIT_RE.split(line[1:]) if line[0] == "!" else line[1:].split("||")):
                attrs, sep, text = part.partition("|")        # the first '|' left separates the cell attributes
                if not sep:
                    attrs, text = "", part
                cur.append((line[0] == "!", attrs, text.translate(_UNMASK).strip()))
        elif cur:                                              # continuation line of the previous cell
            h, a, t = cur[-1]
            cur[-1] = (h, a, (t + " " + line.translate(_UNMASK).strip()).strip())
    return rows


def fill_spans(rows):
    """Apply rowspan / colspan: every row gets one (is_header, text) per column, the spanned cells are repeated."""
    grid, carry = [], {}                                       # carry: column -> [rows still to fill, is_header, text]
    for row in rows:
        out, col, i = [], 0, 0
        while i < len(row) or col in carry:
            if col in carry:
                left, hdr, text = carry[col]
                out.append((hdr, text))
                if left <= 1:
                    del carry[col]
                else:
                    carry[col][0] = left - 1
                col += 1
                continue
            hdr, attrs, text = row[i]
            i += 1
            rs, cs = ROWSPAN_RE.search(attrs), COLSPAN_RE.search(attrs)
            rs = min(int(rs.group(1)), 60) if rs else 1
            cs = min(int(cs.group(1)), 60) if cs else 1
            for _ in range(cs):
                out.append((hdr, text))
                if rs > 1:
                    carry[col] = [rs - 1, hdr, text]
                col += 1
        grid.append(out)
    return grid


def _cell_names(text, skip):
    """Cell -> list of names ('A<br>B' -> [A, B]); empty / placeholder values and the driver's own name are dropped."""
    out = []
    for part in clean_wikitext(text).split(";"):
        v = re.sub(r"\s+", " ", part).strip(" *†‡")
        if v and len(v) <= 100 and not JUNK_NAME_RE.match(v) and norm_key(v) not in skip:
            out.append(v)
    return out


def dedupe_names(values):
    """Order-preserving, exact match (case and spacing ignored)."""
    seen, out = set(), []
    for v in values:
        k = v.casefold()
        if k not in seen:
            seen.add(k)
            out.append(v)
    return out


def table_names(body, skip=()):
    """All wikitables of the page -> (team names, series names, co-driver names): distinct, no years / seasons."""
    teams, series, codrivers = [], [], []
    for m in TABLE_BLOCK_RE.finditer(body):
        tbl = m.group()
        if not TABLE_HEAD_RE.search(tbl[:3000]):
            continue
        grid = fill_spans(parse_table(tbl))
        # header row = the first row made only of header cells with at least 2 different texts (a colspan title row has 1)
        hi = next((i for i, r in enumerate(grid) if len(r) >= 2 and all(h for h, _ in r)
                   and len({t for _, t in r}) >= 2), None)
        if hi is None:
            continue
        head = [re.sub(r"\(s\)", "", clean_wikitext(t).lower()).strip() for _, t in grid[hi]]
        targets = [([i for i, h in enumerate(head) if h in TEAM_COLS], teams),
                   ([i for i, h in enumerate(head) if h in TABLE_SERIES_COLS], series),
                   ([i for i, h in enumerate(head) if CODRIVER_COL_RE.match(h)], codrivers)]
        if not any(idx for idx, _ in targets):
            continue
        for row in grid[hi + 1:]:
            if len({t for _, t in row}) <= 1:                  # footnote / spacer row (one text repeated by colspan)
                continue
            for idx, dest in targets:
                for i in idx:
                    if i < len(row):
                        dest.extend(_cell_names(row[i][1], skip))
    return dedupe_names(teams), dedupe_names(series), dedupe_names(codrivers)

# ---------- page ----------
def split_page(raw):
    """(title, ns, page_id, is_redirect, wikitext) of a <page> block; wikitext is '' for an empty page."""
    rev = raw.find("<revision>")
    head = raw[:rev] if rev != -1 else raw[:3000]
    t, n, i = TITLE_RE.search(head), NS_RE.search(head), ID_RE.search(head)
    title = html.unescape(t.group(1)) if t else None
    ns = int(n.group(1)) if n else None
    redirect = bool(REDIRECT_RE.search(head))
    ts = raw.find("<text", rev if rev != -1 else 0)
    if ts == -1:
        return title, ns, int(i.group(1)) if i else None, redirect, ""
    gt = raw.find(">", ts)
    if gt == -1 or raw[gt - 1] == "/":            # <text ... /> = empty
        return title, ns, int(i.group(1)) if i else None, redirect, ""
    te = raw.rfind("</text>")
    return title, ns, int(i.group(1)) if i else None, redirect, raw[gt + 1:te if te > gt else len(raw)]


def _first(boxes, keys):
    """First non-empty raw value of any of the keys, looking at the boxes in the given order."""
    for b in boxes:
        for k in keys:
            if b["params"].get(k):
                return b["params"][k]
    return None


def extract_wiki_page(raw, msm=None):
    """
    raw: text of one <page>...</page>; msm: {name_key: [birth years]} of the Motor Sport Magazine drivers (or None).
    Returns (record or None, reason); reason is 'ok' or why the page was dropped
    (redirect / not_article / empty / no_match).
    """
    title, ns, page_id, redirect, text = split_page(raw)
    if redirect:
        return None, "redirect"
    if ns != 0:
        return None, "not_article"
    if not text or not title:
        return None, "empty"

    # --- 1. is it a driver page? (cheap checks on the raw text, nothing is parsed yet) ---
    names = {norm_name(x) for x in INFOBOX_RE.findall(text)} if "nfobox" in text else set()
    cats = [html.unescape(c).strip() for c in CAT_RE.findall(text)] if "ategory" in text else []
    cat_hits = [c for c in cats if is_driver_category(c) or MOTO_CAT_RE.search(c)]
    reason, msm_key = None, None
    if names & TARGET_INFOBOXES:
        reason = "infobox"
    elif cat_hits:
        reason = "category"
    elif msm:
        key = title_key(title)
        years = msm.get(key)
        if years:
            by = page_birth_year(text, cats)
            if by is not None and by in years and (
                    any(MOTORSPORT_CAT_RE.search(c) for c in cats) or MOTORSPORT_TEXT_RE.search(text[:10000])):
                reason, msm_key = "msm_rescue", key
    if reason is None:
        return None, "no_match"

    # --- 2. parse the page (only the ~15 000 pages that got here) ---
    body = COMMENT_RE.sub("", REF_RE.sub("", html.unescape(text)))
    boxes = find_infoboxes(body)
    primary = pick_primary(boxes)
    ordered = ([primary] if primary else []) + [b for b in boxes if b is not primary]

    def text_of(*keys):
        return clean_wikitext(_first(ordered, keys)) or None

    born_date, born_prec = parse_wiki_date(_first(ordered, ("birth_date",)), "born")
    died_date, died_prec = parse_wiki_date(_first(ordered, ("death_date",)), "died")
    moto = bool(names & MOTO_INFOBOXES) or any(MOTO_CAT_RE.search(c) for c in cats)
    car = bool(names & (TARGET_INFOBOXES - MOTO_INFOBOXES)) or any(is_driver_category(c) for c in cats)
    series = [s for b in ordered for s in series_blocks(b)]
    own_name = text_of("name") or re.sub(r"\s*\([^)]*\)\s*$", "", title)
    table_teams, table_series, co_drivers = table_names(
        body, {k for k in (title_key(title), norm_key(own_name), norm_key(text_of("birth_name", "full_name") or "")) if k})

    record = {
        "page_id": page_id,
        "title": title,
        "url": "https://en.wikipedia.org/wiki/" + quote(title.replace(" ", "_"), safe="/:()',!*~-._"),
        "title_key": title_key(title),
        "match_reason": reason,
        "matched_msm_key": msm_key,
        "discipline": "both" if moto and car else "motorcycle" if moto else "car" if car else "unknown",
        "infobox_type": primary["name"] if primary else None,
        "infoboxes": sorted(names),
        "name": text_of("name") or re.sub(r"\s*\([^)]*\)\s*$", "", title),
        "birth_name": text_of("birth_name", "full_name", "fullname"),
        "nationality": text_of("nationality"),
        "born_date": born_date, "born_prec": born_prec,
        "born_year": int(born_date[:4]) if born_date else None,
        "born_place": text_of("birth_place"),
        "died_date": died_date, "died_prec": died_prec,
        "died_place": text_of("death_place"),
        "number": text_of("car_number", "bike_number"),
        "titles": text_of("titles"),
        "awards": text_of("awards"),
        "relatives": text_of("relatives"),
        "current_team": text_of("current_team"),
        "former_teams": text_of("former_teams"),
        "series": series,
        "table_teams": table_teams, "table_series": table_series, "co_drivers": co_drivers,
        "match_categories": cat_hits[:5],
        "categories": [c for c in cats if not MAINT_CAT_RE.search(c)],
        "lead": lead_text(body),
        "categorisation": text_of("racing_licence", "racing_license"),

    }
    return record, "ok"


# ---------- reading the dump by byte ranges (no Hadoop, works on local files and Windows) ----------
PAGE_OPEN, PAGE_CLOSE = b"<page>", b"</page>"


def plan_ranges(size, n_parts):
    """[(start, end)] byte ranges covering the file; a page belongs to the range its '<page>' starts in."""
    step = max(1, -(-size // max(1, n_parts)))
    return [(s, min(size, s + step)) for s in range(0, size, step)]


def iter_pages_in_range(path, start, end, chunk=1 << 24):
    """
    Yield the text of every <page>...</page> whose '<page>' tag starts in [start, end). The page that is being read
    when `end` is reached is finished (the next range skips it because its search for '<page>' starts at its own
    `start`). Memory: one chunk + one page.
    """
    with open(path, "rb") as f:
        f.seek(start)
        buf, pos = b"", start                      # buf[0] sits at absolute offset pos
        while True:                                # find the first <page> at offset >= start
            i = buf.find(PAGE_OPEN)
            if i != -1:
                buf, pos = buf[i:], pos + i
                break
            data = f.read(chunk)
            if not data:
                return
            keep = buf[-(len(PAGE_OPEN) - 1):]
            pos += len(buf) - len(keep)
            buf = keep + data
        while pos < end:
            j = buf.find(PAGE_CLOSE)
            while j == -1:
                data = f.read(chunk)
                if not data:
                    return
                scan_from = max(0, len(buf) - len(PAGE_CLOSE) + 1)
                buf += data
                j = buf.find(PAGE_CLOSE, scan_from)
            stop = j + len(PAGE_CLOSE)
            yield buf[:stop].decode("utf-8", errors="replace")
            buf, pos = buf[stop:], pos + stop
            while True:                            # next <page>
                i = buf.find(PAGE_OPEN)
                if i != -1:
                    buf, pos = buf[i:], pos + i
                    break
                data = f.read(chunk)
                if not data:
                    return
                keep = buf[-(len(PAGE_OPEN) - 1):]
                pos += len(buf) - len(keep)
                buf = keep + data