"""Summarizes a live-check JSON report: findings counted by level and id, and every violation."""

import collections
import json
import sys

report = json.load(open(sys.argv[1]))
counts = collections.Counter()
examples = {}
violations = collections.defaultdict(collections.Counter)


def walk(node):
    if isinstance(node, dict):
        for advice in (node.get("live_check_result") or {}).get("all_advice", []):
            key = (advice["level"], advice["id"])
            counts[key] += 1
            examples.setdefault(key, advice["message"])
            if advice["level"] == "violation":
                violations[advice["id"]][advice["message"]] += 1
        for value in node.values():
            walk(value)
    elif isinstance(node, list):
        for value in node:
            walk(value)


walk(report["samples"])
order = {"violation": 0, "improvement": 1, "information": 2}
print(f"\n{'count':>6}  {'level':<12} {'finding':<45} example")
for (level, finding), count in sorted(counts.items(), key=lambda kv: (order[kv[0][0]], -kv[1])):
    print(f"{count:>6}  {level:<12} {finding:<45} {examples[(level, finding)]}")
for finding, messages in violations.items():
    print(f"\nViolations of {finding}, {len(messages)} distinct:")
    for message, count in messages.most_common(5):
        print(f"{count:>6}  {message}")
