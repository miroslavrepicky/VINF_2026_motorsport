"""
show_join_example.py - shows what the MSM x Wikipedia join did with ONE racer (stdlib only).

    python3 show_join_example.py                         (first well-filled linked racer, source=both)
    python3 show_join_example.py --name "graham hill"    (a linked racer whose name contains the text)
    python3 show_join_example.py --kind wiki_only        (a racer that is NOT in MSM and came only from Wikipedia)
    python3 show_join_example.py --kind msm_only         (an MSM racer for whom no Wikipedia page was found)
    python3 show_join_example.py --joined data/joined_drivers.jsonl

For source=both it prints three blocks: the MSM record BEFORE the join, the Wikipedia record BEFORE the join,
and the joined record AFTER it, followed by the list of what Wikipedia added.
"""
import argparse
import json
import sys

STATS = ("starts", "wins", "poles", "podiums")


def read_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def cut(text, n=160):
    text = " ".join(str(text).split())
    return text if len(text) <= n else text[:n].rstrip() + "..."


def line(label, value):
    print(f"  {label:<14}{value if value not in (None, '', []) else '-'}")


def msm_series(msm):
    """{series name: 'starts=.. wins=.. poles=.. podiums=..'} from the wide columns of an MSM record."""
    out = {}
    for sid, name in (msm.get("series_names") or {}).items():
        if not name:
            continue
        vals = " ".join(f"{s}={msm.get(f's__{sid}__{s}')}" for s in STATS)
        out[name] = vals
    return out


def show_msm(msm):
    print("MSM (before the join)")
    line("name", msm.get("name"))
    line("full_name", msm.get("full_name"))
    line("nationality", msm.get("nationality"))
    line("born_date", msm.get("born_date"))
    line("died_date", msm.get("died_date"))
    line("teams", "; ".join(msm.get("teams") or []))
    for name, vals in list(msm_series(msm).items())[:4]:
        line("series", f"{name}: {vals}")
    line("bio", cut(msm.get("bio") or ""))


def show_wiki(wiki):
    print("Wikipedia (before the join)")
    line("title", wiki.get("title"))
    line("infobox", wiki.get("infobox_type"))
    line("match_reason", wiki.get("match_reason"))
    line("discipline", wiki.get("discipline"))
    line("birth_name", wiki.get("birth_name"))
    line("nationality", wiki.get("nationality"))
    line("born_date", wiki.get("born_date"))
    line("born_place", wiki.get("born_place"))
    line("died_date", wiki.get("died_date"))
    line("died_place", wiki.get("died_place"))
    line("titles", cut(wiki.get("titles") or ""))
    line("awards", cut(wiki.get("awards") or ""))
    line("relatives", cut(wiki.get("relatives") or ""))
    line("teams", cut("; ".join(x for x in (wiki.get("current_team"), wiki.get("former_teams")) if x)))
    for s in (wiki.get("series") or [])[:4]:
        vals = " ".join(f"{k}={s.get(k)}" for k in STATS if s.get(k) is not None)
        line("series", f"{s.get('series')}: {vals or '-'} years={s.get('years') or '-'}")
    line("lead", cut(wiki.get("lead") or ""))
    line("url", wiki.get("url"))


def show_joined(rec):
    print("Joined record (after the join)")
    for k in ("record_id", "source", "link_status", "match_method", "confidence", "conflict", "wiki_kind",
              "name", "nationality", "born_date", "died_date", "url_msm", "url_wiki", "teams"):
        line(k, rec.get(k))


def added_by_wiki(rec):
    """What the Wikipedia side gives that the MSM side did not have."""
    msm, wiki = rec.get("msm") or {}, rec.get("wiki") or {}
    out = []
    for key in ("birth_name", "born_place", "died_place", "titles", "awards", "relatives", "lead"):
        if wiki.get(key):
            out.append(f"{key}: {cut(wiki[key], 90)}")
    if not msm.get("born_date") and wiki.get("born_date"):
        out.append(f"born_date filled: {wiki['born_date']}")
    elif msm.get("born_date") and wiki.get("born_date") and len(wiki["born_date"]) > len(msm["born_date"]):
        out.append(f"born_date more precise: {msm['born_date']} -> {wiki['born_date']}")
    if not msm.get("died_date") and wiki.get("died_date"):
        out.append(f"died_date filled: {wiki['died_date']}")
    if not msm.get("nationality") and wiki.get("nationality"):
        out.append(f"nationality filled: {wiki['nationality']}")
    known = {t.lower() for t in msm.get("teams") or []}
    new_teams = [t for t in "; ".join(x for x in (wiki.get("current_team"), wiki.get("former_teams")) if x).split(";")
                 if t.strip() and t.strip().lower() not in known]
    if new_teams:
        out.append("teams (wiki text): " + cut("; ".join(t.strip() for t in new_teams), 90))
    return out


def rich(rec):
    """How well filled a linked record is (used to pick a readable example automatically)."""
    wiki = rec.get("wiki") or {}
    return sum(bool(wiki.get(k)) for k in ("born_place", "died_place", "titles", "awards", "relatives", "lead"))


def pick(path, kind, name):
    best = None
    for rec in read_jsonl(path):
        if rec.get("source") != kind:
            continue
        who = (rec.get("name") or "").lower()
        if name:
            if name in who:
                return rec
            continue
        if kind == "both":
            if rec.get("confidence") != "high":
                continue
            if rich(rec) >= 5 and (rec.get("msm") or {}).get("bio"):
                return rec
            if best is None or rich(rec) > rich(best):
                best = rec
        elif kind == "wiki_only":
            w = rec.get("wiki") or {}
            if rec.get("wiki_kind") == "core" and w.get("series") and w.get("lead") and w.get("born_place"):
                return rec
        else:
            if (rec.get("msm") or {}).get("bio"):
                return rec
    return best


def main():
    ap = argparse.ArgumentParser(description="Show one racer before and after the MSM x Wikipedia join")
    ap.add_argument("--joined", default="data/joined_drivers.jsonl")
    ap.add_argument("--kind", choices=["both", "wiki_only", "msm_only"], default="both")
    ap.add_argument("--name", default=None, help="part of the racer's name (case-insensitive)")
    args = ap.parse_args()

    rec = pick(args.joined, args.kind, args.name.lower() if args.name else None)
    if rec is None:
        sys.exit("No matching record found.")
    print(f"=== {rec['name']} ({rec['source']}) ===\n")
    if rec.get("msm"):
        show_msm(rec["msm"])
        print()
    if rec.get("wiki"):
        show_wiki(rec["wiki"])
        print()
    show_joined(rec)
    if rec["source"] == "both":
        print("\nAdded by Wikipedia:")
        for item in added_by_wiki(rec):
            print("  +", item)
    elif rec["source"] == "wiki_only":
        print("\nThis racer is not in the MSM data; the whole record comes from Wikipedia.")
    else:
        print(f"\nNo Wikipedia page was linked (link_status = {rec.get('link_status')}).")


if __name__ == "__main__":
    main()
