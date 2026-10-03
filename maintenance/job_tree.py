# -*- coding: utf-8 -*-
"""
job_tree.py — Print the scheduler job dependency tree from process_scheduler/jobs.json.

Output looks like process_scheduler/job_tree.txt:
  - Each job is drawn in full once, under its *deepest* parent (longest path from a
    root; ties go to the first entry in its `dependencies`), and annotated with
    `◄ also: <other parents>`. Under its other parents it appears as `name …`.
  - Jobs with many parents (>= --hub-min, default 4, e.g. pnl_derivative) are pulled
    out into their own `★` section below the main tree, annotated `◄ needs: <all deps>`.
  - Scheduled jobs show `[HH:MM]`; disabled jobs show `(disabled)`.

Usage:
    python maintenance/job_tree.py
    python maintenance/job_tree.py -o process_scheduler/job_tree.txt
    python maintenance/job_tree.py --hub-min 3
    python maintenance/job_tree.py --jobs path/to/jobs.json
"""
import argparse
import json
import sys
from pathlib import Path

DEFAULT_JOBS = Path(__file__).resolve().parents[1] / "process_scheduler" / "jobs.json"


def load_jobs(path):
    jobs = json.loads(Path(path).read_text(encoding="utf-8"))
    return {j["id"]: j for j in jobs}


def build_tree(jobs, hub_min=4):
    """Return the rendered tree as a list of lines."""
    warnings = []
    order = {jid: i for i, jid in enumerate(jobs)}
    parents = {}
    for jid, job in jobs.items():
        deps = []
        for d in job.get("dependencies") or []:
            if d in jobs:
                deps.append(d)
            else:
                warnings.append(f"! {jid}: unknown dependency {d!r}")
        parents[jid] = deps

    children = {jid: [] for jid in jobs}
    for jid, deps in parents.items():
        for d in deps:
            children[d].append(jid)

    # level = longest path from a root; also detects cycles
    level, visiting = {}, set()

    def get_level(jid):
        if jid in level:
            return level[jid]
        if jid in visiting:
            raise SystemExit(f"Dependency cycle involving {jid!r}")
        visiting.add(jid)
        level[jid] = 1 + max((get_level(p) for p in parents[jid]), default=-1)
        visiting.discard(jid)
        return level[jid]

    for jid in jobs:
        get_level(jid)

    hubs = {jid for jid in jobs if len(parents[jid]) >= hub_min}
    primary = {}
    for jid, deps in parents.items():
        if deps and jid not in hubs:
            primary[jid] = max(deps, key=lambda p: (level[p], -deps.index(p)))

    def label(jid):
        job = jobs[jid]
        s = jid
        if job.get("schedule_time"):
            s += f" [{job['schedule_time']}]"
        if job.get("enabled") is False:
            s += " (disabled)"
        return s

    def sort_children(jid):
        prim = [c for c in children[jid] if primary.get(c) == jid]
        refs = [c for c in children[jid] if primary.get(c) != jid]
        prim.sort(key=lambda c: (jobs[c].get("schedule_time") is None,
                                 jobs[c].get("schedule_time") or "", c))
        refs.sort(key=lambda c: (level[c], order[c]))
        return [(c, True) for c in prim] + [(c, False) for c in refs]

    rows = []            # (text, annotation)
    hub_mentioned = set()

    def walk(jid, prefix):
        kids = sort_children(jid)
        for i, (c, expand) in enumerate(kids):
            last = i == len(kids) - 1
            branch = prefix + ("└── " if last else "├── ")
            if c in hubs:
                tag = "  ★ (see below)" if c not in hub_mentioned else " …"
                hub_mentioned.add(c)
                rows.append((branch + c + tag, None))
            elif not expand:
                rows.append((branch + c + " …", None))
            else:
                others = [p for p in parents[c] if p != jid]
                rows.append((branch + label(c), f"◄ also: {', '.join(others)}" if others else None))
                walk(c, prefix + ("    " if last else "│   "))

    roots = sorted((j for j in jobs if not parents[j]), key=lambda j: order[j])
    for r in roots:
        rows.append((label(r), None))
        walk(r, "")
    for h in sorted(hubs, key=lambda h: (level[h], order[h])):
        rows.append(("", None))
        rows.append((f"★ {label(h)}", f"◄ needs: {', '.join(parents[h])}"))
        walk(h, "")

    width = max((len(t) for t, a in rows if a), default=0) + 3
    lines = [f"{t:<{width}}{a}" if a else t for t, a in rows]
    if warnings:
        lines += [""] + warnings
    return lines


def main():
    parser = argparse.ArgumentParser(description="Print the scheduler job dependency tree.")
    parser.add_argument("--jobs", type=Path, default=DEFAULT_JOBS,
                        help=f"jobs.json path (default: {DEFAULT_JOBS})")
    parser.add_argument("--hub-min", type=int, default=4,
                        help="jobs with at least this many dependencies get their own ★ section (default: 4)")
    parser.add_argument("-o", "--output", type=Path, help="also write the tree to this file")
    args = parser.parse_args()

    text = "\n".join(build_tree(load_jobs(args.jobs), args.hub_min))

    sys.stdout.reconfigure(encoding="utf-8")
    print(text)
    if args.output:
        args.output.write_text(text + "\n", encoding="utf-8")
        print(f"\nWritten to {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
