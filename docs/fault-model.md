# Fault Model, Clock Skew & Result Interpretation

## Error Classification (`:fail` vs `:info`)

In Jepsen, accurately distinguishing definite failures from indeterminate outcomes is essential for Knossos and Elle:

- **`:fail` (Definite failure — operation did NOT commit)**:
  - HTTP `412 PreconditionFailed` / `409 ConditionalRequestConflict` (`If-Match` / `If-None-Match` rejected)
  - HTTP `400 BadRequest`, `403 Forbidden`
  - Read errors (reads never mutate cluster state)
- **`:info` (Indeterminate outcome — write MAY have committed before failure)**:
  - Socket / connection timeouts (`SdkClientException`)
  - HTTP `500 InternalError`, `503 SlowDown` / `ServiceUnavailable`, `408 RequestTimeout`
  - Connection reset during active `partition`, `kill`, or `packet` nemesis windows

## Why `start-stop-daemon` Instead of `systemd`?

The official RustFS Linux `.deb`/`.rpm` systemd unit configures `Restart=always` and `ProtectClock=true`:
- `Restart=always` immediately revives killed nodes, defeating Jepsen's `:kill` nemesis.
- `ProtectClock=true` blocks clock adjustments on VM nodes, defeating Jepsen's `:clock` nemesis.

`rustfs.jepsen.db` therefore installs the verified static `rustfs` binary directly and manages it with `start-stop-daemon`.

## Clock Skew Testing (VMs Only)

Docker containers share the host Linux kernel clock (`CAP_SYS_TIME` in one container shifts time for all containers and the host).
- **Docker default**: `--faults partition,kill,pause,packet,drive`
- **Independent VMs (UTM, QEMU/KVM, cloud EC2/GCE instances)**: verify `sudo date -s '+30 seconds'` on `n1` does not shift `n2`, then pass `--faults partition,kill,pause,packet,drive,clock`.

## Interpreting `results.edn`

| `:valid?` | Meaning | Next Step |
|---|---|---|
| `true` | History satisfied Knossos / Elle and all custom invariants | Archive artifacts |
| `false` | Confirmed consistency violation | Inspect `timeline.html`, Elle anomaly cycle graph, and `n*/rustfs.log`; file an issue using the Consistency Violation template |
| `:unknown` | History inconclusive (e.g., too many `:info` crashes or checker search limit reached) | Re-run with longer recovery window or lower concurrency |
