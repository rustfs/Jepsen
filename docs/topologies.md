# RustFS Jepsen Topologies & Quorum Rules

RustFS expands `{start...end}` brace expressions in `RUSTFS_VOLUMES` as a Cartesian product forming a single erasure pool.

## Quorum Formulas

Mirroring `rustfs/rustfs/crates/ecstore`:

- **Default Parity (`default_parity_count`)**:
  - 1 drive -> `0`
  - 2–3 drives -> `1`
  - 4–5 drives -> `2`
  - 6–7 drives -> `3`
  - ≥ 8 drives -> `4`
- **Read Quorum**: `R = total_drives - parity`
- **Write Quorum**: `W = data_shards + 1` when `data_shards == parity`, otherwise `W = data_shards`.

## Supported Topologies

### `4x4` (16 drives, EC:4)
- **Layout**: 4 nodes (`n1..n4`) × 4 drives (`data1..data4`) = 16 drives in one erasure set.
- **Quorums**: `data=12`, `parity=4`, `R=12`, `W=12`.
- **Failure behavior**: Losing any 1 node (4 drives) leaves 12 drives online — both reads and writes continue uninterrupted. A 2-2 network partition leaves 8 drives on each side (< 12), halting writes cleanly on both sides to prevent split-brain.

### `3x2` (6 drives, EC:3)
- **Layout**: 3 nodes (`n1..n3`) × 2 drives (`data1..data2`) = 6 drives in one erasure set.
- **Quorums**: `data=3`, `parity=3`, `R=3`, `W=4`.
- **Failure behavior**: Under a 2-1 partition, the 2-node majority (4 drives) retains both read (`4 >= 3`) and write (`4 >= 4`) quorum, while the isolated single node (2 drives) cannot read or write.

### `4x2` (8 drives, EC:4)
- **Layout**: 4 nodes (`n1..n4`) × 2 drives (`data1..data2`) = 8 drives in one erasure set.
- **Quorums**: `data=4`, `parity=4`, `R=4`, `W=5`.
- **Failure behavior**: Losing 1 node (6 drives remaining) retains both read and write quorum; a 2-2 split (4 drives each) blocks writes on both sides because `4 < 5`.

## Important: Never Reuse Volumes Across Topologies

RustFS persists cluster erasure geometry inside `format.json` on each drive. Always wipe volumes when switching between `4x4`, `3x2`, and `4x2` (`./scripts/up.sh` handles this automatically via `docker compose down -v`).
