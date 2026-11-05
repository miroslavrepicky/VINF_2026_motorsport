"""
matching.py - shared helpers of the MSM x Wikipedia join (stdlib only).

Used by join_drivers.py (Spark UDFs + constants), indexer.py and searcher.py (nationality filter):
  nat_set(text)         nationality text -> set of normalized keys (England / UK / GBR -> british)
  name_similar(a, b)    'strong' | 'weak' | None - similarity of two sets of name keys
  wiki_span(rec)        (first year, last year) of the career years written in the wiki infobox series blocks
  series_kind(names)    {series_id: name} of an MSM driver -> none | moto | car | mixed
  TOLERANCE             years of tolerance when comparing career ranges
"""
import re
from difflib import SequenceMatcher

from wiki_page import norm_key

YEAR_RE = re.compile(r"\b(1[89]\d\d|20\d\d)\b")
TOLERANCE = 2          # years of tolerance when comparing the career ranges
# MSM series that are motorcycle series (matched on the series NAME); everything else is a car series
MOTO_SERIES_RE = re.compile(
    r"\bmoto(?:gp|cross|2|3|e)?\b|motorcycle|sidecar|superbike|speedway|isle of man|tourist trophy|\btt\b", re.IGNORECASE)


def series_kind(names):
    """{series_id: name} of an MSM driver -> 'none' (no series) | 'moto' (only motorcycle series) | 'car' | 'mixed'."""
    vals = [v for v in (names or {}).values() if v]
    if not vals:
        return "none"
    moto = [bool(MOTO_SERIES_RE.search(v)) for v in vals]
    return "moto" if all(moto) else "mixed" if any(moto) else "car"


def span(years):
    return (min(years), max(years)) if years else None


def wiki_span(rec):
    years = []
    for s in rec.get("series") or []:
        raw = str(s.get("years") or "")
        ys = [int(y) for y in YEAR_RE.findall(raw)]
        if ys and re.search(r"present|current", raw, re.IGNORECASE):
            ys.append(2026)
        years.extend(ys)
    return span(years)

def nat_label(text):
    """Wiki nationality ('AUS', 'England; USA') -> 'Australian' style names (unknown values stay as they are)."""
    out = []
    for part in re.split(r"[;,/]|\band\b", text or ""):
        part = part.strip()
        if part:
            canon = NAT_MAP.get(norm_key(part))
            out.append(canon.title() if canon else part)
    return "; ".join(dict.fromkeys(out)) or None


# ---------- nationality normalization ----------
NAT_GROUPS = {
    "british": ["british", "english", "scottish", "welsh", "northern irish", "united kingdom", "great britain",
                "uk", "england", "scotland", "wales", "northern ireland", "britain"],
    "american": ["american", "united states", "usa", "us", "u s", "america", "united states of america"],
    "german": ["german", "germany", "west german", "east german", "west germany", "east germany"],
    "french": ["french", "france"], "italian": ["italian", "italy"], "spanish": ["spanish", "spain"],
    "dutch": ["dutch", "netherlands", "the netherlands", "holland"], "belgian": ["belgian", "belgium"],
    "swiss": ["swiss", "switzerland"], "austrian": ["austrian", "austria"], "swedish": ["swedish", "sweden"],
    "finnish": ["finnish", "finland"], "danish": ["danish", "denmark"], "norwegian": ["norwegian", "norway"],
    "brazilian": ["brazilian", "brazil"], "argentine": ["argentine", "argentinian", "argentina"],
    "australian": ["australian", "australia"], "canadian": ["canadian", "canada"],
    "new zealander": ["new zealander", "new zealand", "kiwi"], "japanese": ["japanese", "japan"],
    "south african": ["south african", "south africa"], "irish": ["irish", "ireland", "republic of ireland"],
    "czech": ["czech", "czechoslovak", "czechoslovakia", "czech republic"],
    "russian": ["russian", "russia", "soviet", "soviet union", "ussr"], "mexican": ["mexican", "mexico"],
    "polish": ["polish", "poland"], "portuguese": ["portuguese", "portugal"], "hungarian": ["hungarian", "hungary"],
    "monegasque": ["monegasque", "monaco"], "uruguayan": ["uruguayan", "uruguay"],
    "venezuelan": ["venezuelan", "venezuela"], "chilean": ["chilean", "chile"],
}
# 3-letter country codes (IOC / FIFA / ISO) that the wiki infoboxes use for the newer drivers (AUS, JPN, GER, RSA...)
NAT_CODES = {
    "british": ["gbr"], "american": ["usa"], "german": ["ger", "deu", "frg", "gdr"], "french": ["fra"],
    "italian": ["ita"], "spanish": ["esp"], "dutch": ["ned", "nld"], "belgian": ["bel"], "swiss": ["sui", "che"],
    "austrian": ["aut"], "swedish": ["swe"], "finnish": ["fin"], "danish": ["den", "dnk"], "norwegian": ["nor"],
    "brazilian": ["bra"], "argentine": ["arg"], "australian": ["aus"], "canadian": ["can"],
    "new zealander": ["nzl"], "japanese": ["jpn"], "south african": ["rsa", "zaf"], "irish": ["irl"],
    "czech": ["cze", "tch"], "russian": ["rus", "urs"], "mexican": ["mex"], "polish": ["pol"],
    "portuguese": ["por", "prt"], "hungarian": ["hun"], "monegasque": ["mon", "mco"],
    "uruguayan": ["uru", "ury"], "venezuelan": ["ven"], "chilean": ["chi", "chl"],
}
for _canon, _codes in NAT_CODES.items():
    NAT_GROUPS[_canon] += _codes
NAT_GROUPS.update({
    "liechtenstein": ["liechtenstein", "liechtensteiner", "lie"], "colombian": ["colombian", "colombia", "col"],
    "thai": ["thai", "thailand", "tha"], "malaysian": ["malaysian", "malaysia", "mas", "mys"],
    "chinese": ["chinese", "china", "chn"], "indian": ["indian", "india", "ind"],
    "indonesian": ["indonesian", "indonesia", "ina", "idn"], "turkish": ["turkish", "turkey", "tur"],
    "emirati": ["emirati", "united arab emirates", "uae", "are"],
    "singaporean": ["singaporean", "singapore", "sgp", "sin"], "ecuadorian": ["ecuadorian", "ecuador", "ecu"],
    "peruvian": ["peruvian", "peru", "per"], "luxembourgish": ["luxembourgish", "luxembourg", "lux"],
    "greek": ["greek", "greece", "gre", "grc"], "romanian": ["romanian", "romania", "rou", "rom"],
    "bulgarian": ["bulgarian", "bulgaria", "bul", "bgr"], "croatian": ["croatian", "croatia", "cro", "hrv"],
    "slovak": ["slovak", "slovakia", "svk"], "slovenian": ["slovenian", "slovenia", "slo", "svn"],
    "serbian": ["serbian", "serbia", "srb"], "yugoslav": ["yugoslav", "yugoslavia", "yug"],
    "ukrainian": ["ukrainian", "ukraine", "ukr"], "israeli": ["israeli", "israel", "isr"],
    "saudi": ["saudi", "saudi arabian", "saudi arabia", "ksa", "sau"],
    "korean": ["korean", "south korean", "south korea", "kor"], "filipino": ["filipino", "philippines", "phi", "phl"],
    "hong kong": ["hong kong", "hkg"], "taiwanese": ["taiwanese", "taiwan", "chinese taipei", "tpe", "twn"],
    "estonian": ["estonian", "estonia", "est"], "latvian": ["latvian", "latvia", "lva", "lat"],
    "lithuanian": ["lithuanian", "lithuania", "ltu"], "bahraini": ["bahraini", "bahrain", "bhr"],
    "qatari": ["qatari", "qatar", "qat"], "egyptian": ["egyptian", "egypt", "egy"],
    "moroccan": ["moroccan", "morocco", "mar"], "icelandic": ["icelandic", "iceland", "isl"],
})
NAT_MAP = {alias: canon for canon, aliases in NAT_GROUPS.items() for alias in aliases}


def nat_set(text):
    """'British; American' -> {'british', 'american'}; unknown values are kept as their norm_key."""
    out = set()
    for part in re.split(r"[;,/]|\band\b", text or ""):
        k = norm_key(part)
        if k:
            out.add(NAT_MAP.get(k, k))
    return out


def name_similar(mkeys, wkeys):
    """'strong' = same surname or similar whole name, 'weak' = only a shared / prefix word (4+ letters), else None."""
    best = None
    for a in mkeys:
        ta = a.split()
        for b in wkeys:
            tb = b.split()
            if not ta or not tb:
                continue
            if ta[-1] == tb[-1] or SequenceMatcher(None, a, b).ratio() >= 0.85:
                return "strong"
            if any(len(x) >= 4 and len(y) >= 4 and (x.startswith(y) or y.startswith(x)) for x in ta for y in tb):
                best = "weak"
    return best

# ---------- merge of ONE MSM record + ONE wiki record into the final joined record ----------
MSM_ONLY = ("gender", "tagline", "last_race_slug", "last_race_name", "bio", "seasons")
WIKI_ONLY = (("wiki_title", "title"), ("discipline", "discipline"), ("infobox_type", "infobox_type"),
             ("infoboxes", "infoboxes"), ("match_reason", "match_reason"), ("born_place", "born_place"),
             ("died_place", "died_place"), ("number", "number"), ("titles", "titles"), ("awards", "awards"),
             ("relatives", "relatives"), ("current_team", "current_team"), ("lead", "lead"),
             ("categories", "categories"), ("categorisation", "categorisation"), ("co_drivers", "co_drivers"))


def _present(v):
    return v is not None and v != "" and v != [] and v != {}


def _first(*vals):
    return next((v for v in vals if _present(v)), None)


def _more_precise(m, w):
    """MSM date first; the wiki one only when MSM has none or the wiki date is the same date, just more precise."""
    if _present(m) and _present(w) and len(w) > len(m) and w.startswith(m):
        return w
    return _first(m, w)


def _union(*lists):
    """Distinct values of all lists (exact match, case / spacing ignored); None when no source had a list at all."""
    seen, out, any_list = set(), [], False
    for lst in lists:
        if lst is None:
            continue
        any_list = True
        for v in lst:
            v = " ".join(str(v).split())
            if v and v.casefold() not in seen:
                seen.add(v.casefold())
                out.append(v)
    return out if any_list else None


def block_series_names(blocks):
    """Series names of the wiki infobox blocks. The fallback label (= the infobox name) and the short lowercase labels
    taken from parameter names (cup_wins -> 'cup') are not series names and are skipped."""
    out = []
    for b in blocks or []:
        s = (b.get("series") or "").strip()
        if s and s != b.get("infobox") and not re.fullmatch(r"[a-z0-9]+", s):
            out.append(s)
    return out


def block_totals(blocks):
    """fastest_laps / championships / top_tens = sum over the infobox series blocks, best_pos = the best one."""
    res = {}
    for key in ("fastest_laps", "championships", "top_tens"):
        vals = [b[key] for b in blocks or [] if isinstance(b.get(key), int)]
        res[key] = sum(vals) if vals else None
    best = []
    for b in blocks or []:
        m = re.match(r"\s*(\d+)", str(b.get("best_pos") or ""))
        if m:
            best.append((int(m.group(1)), b["best_pos"]))
    res["best_pos"] = min(best)[1] if best else None
    return res


def merge_records(record_id, source, link_status, method, confidence, conflict, wiki_kind, msm, wiki, series_cols=()):
    """
    msm / wiki: the two records of the SAME row (dicts) or None. Returns the final record: every field exactly once,
    MSM first, null gaps filled from wiki, wiki-only fields added; the fields of the missing source are None.
    """
    m, w = msm or {}, wiki or {}
    msm_series = {k: v for k, v in (m.get("series_names") or {}).items() if v}   # inferred struct: drop the nulls
    blocks = w.get("series") or []
    former = [t for t in (w.get("former_teams") or "").split(";")] if w.get("former_teams") else None
    tot = block_totals(blocks)
    rec = {
        "record_id": record_id, "source": source, "link_status": link_status, "match_method": method,
        "confidence": confidence, "conflict": bool(conflict), "wiki_kind": wiki_kind,
        "driver_id": m.get("driver_id"), "wp_id": m.get("wp_id"), "page_id": w.get("page_id"),
        "url_msm": m.get("url"), "url_wiki": w.get("url"),
        "name": _first(m.get("name"), w.get("name")),
        "full_name": _first(m.get("full_name"), w.get("birth_name")),
        "nationality": _first(m.get("nationality"), nat_label(w.get("nationality"))),
        "born_date": _more_precise(m.get("born_date"), w.get("born_date")),
        "died_date": _more_precise(m.get("died_date"), w.get("died_date")),
    }
    for k in MSM_ONLY:
        rec[k] = m.get(k)
    rec["series_names"] = msm_series if msm is not None else None
    for out_key, wiki_key in WIKI_ONLY:
        rec[out_key] = w.get(wiki_key)
    rec.update(tot)
    rec["teams"] = _union(m.get("teams"), w.get("table_teams"), former)
    rec["series"] = _union(list(msm_series.values()) if msm is not None else None,
                           w.get("table_series"), block_series_names(blocks) if wiki is not None else None)
    for c in series_cols:                                    # the wide MSM statistics (null where missing)
        rec[c] = m.get(c)
    return rec