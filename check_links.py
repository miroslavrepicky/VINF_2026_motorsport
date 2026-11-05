"""
Print samples of the linked records (source=both) of data/joined_drivers.jsonl for a manual check.

    python check_links.py                                         (motorcycle links with confidence medium / low)
    python check_links.py --discipline motorcycle --confidence high --samples 30
    python check_links.py --discipline car --confidence low --method name_only
    python check_links.py --discipline any --confidence high medium low --kind likely_noise     (noise-flagged pages that were linked)

For every sample: MSM name, birth date, nationality and series (names) next to the wiki title, birth date,
nationality, discipline, infobox and series (labels), so it is visible whether it is the same person.
"""
import argparse
import json
import random
from collections import Counter


def main():
    ap = argparse.ArgumentParser(description="Samples of linked records for a manual check")
    ap.add_argument("--file", default="data/joined_drivers.jsonl")
    ap.add_argument("--discipline", default="motorcycle", help="wiki discipline: car | motorcycle | both | unknown | any")
    ap.add_argument("--confidence", nargs="*", default=["medium", "low"], help="high / medium / low (several allowed)")
    ap.add_argument("--method", default=None, help="only this match_method")
    ap.add_argument("--kind", default=None, help="only this wiki_kind: core | category | msm_rescue | likely_noise")
    ap.add_argument("--samples", type=int, default=20)
    ap.add_argument("--seed", type=int, default=1)
    args = ap.parse_args()
    random.seed(args.seed)

    rows = []
    with open(args.file, "r", encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            if r["source"] != "both" or r["confidence"] not in args.confidence:
                continue
            if args.discipline != "any" and r["wiki"].get("discipline") != args.discipline:
                continue
            if args.method and r["match_method"] != args.method:
                continue
            if args.kind and r["wiki_kind"] != args.kind:
                continue
            rows.append(r)

    print(f"{len(rows)} linked records; by method: {dict(Counter(r['match_method'] for r in rows))}")
    for r in random.sample(rows, min(args.samples, len(rows))):
        m, w = r["msm"], r["wiki"]
        # in the joined file series_names is a struct with a field for EVERY series (null where the driver did not race)
        msm_series = ", ".join(v for v in (m.get("series_names") or {}).values() if v) or "-"
        wiki_series = ", ".join(s.get("series") or "?" for s in (w.get("series") or [])) or "-"
        print(f"\n[{r['match_method']}/{r['confidence']}] {m.get('name')}  born={m.get('born_date')}  "
              f"nat={m.get('nationality')}\n    MSM series: {msm_series[:150]}\n"
              f"  <-> {w.get('title')}  born={w.get('born_date')}  nat={w.get('nationality')}  "
              f"{w.get('discipline')}/{w.get('infobox_type')}\n    wiki series: {wiki_series[:150]}")


if __name__ == "__main__":
    main()