---
name: Consistency Violation Report
about: Report a Jepsen :valid? false finding against RustFS
title: "[Jepsen] "
labels: ["bug", "consistency"]
---

## Environment

- RustFS version / commit SHA:
- Topology (`4x4` / `3x2` / `4x2`):
- Workload (`register` / `append` / `versioned` / `multipart` / `list`):
- Nemesis faults:

## Command Executed

```bash
lein run test ...
```

## Summary from `results.edn`

Paste the `:valid? false` analysis / anomaly cycle / Knossos counterexample.

## Attachments

Attach `timeline.html`, `results.edn`, and relevant excerpts from `n*/rustfs.log`.
