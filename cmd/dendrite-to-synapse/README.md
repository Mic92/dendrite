# dendrite-to-synapse

One-shot migration tool that reads a Dendrite PostgreSQL database and writes
into a freshly-initialised Synapse PostgreSQL database.

## What gets migrated

| Area | Dendrite source | Synapse target | Notes |
|---|---|---|---|
| Accounts | `userapi_accounts`, `userapi_profiles` | `users`, `profiles` | bcrypt hashes are compatible **only if Synapse `password_config.pepper` is empty** (the default). |
| Devices / tokens | `userapi_devices` | `devices`, `access_tokens`, `user_ips` | Existing access tokens keep working. |
| 3PIDs | `userapi_threepids` | `user_threepids` | |
| Account data / tags / ignore list | `userapi_account_datas` | `account_data`, `room_account_data`, `room_tags`, `ignored_users` | |
| Filters | `syncapi_filter` | `user_filters` | |
| Pushers | `userapi_pushers` | `pushers` | |
| OpenID tokens | `userapi_openid_tokens` | `open_id_tokens` | |
| E2EE device keys | `keyserver_device_keys` | `e2e_device_keys_json` | local users only |
| Cross-signing | `keyserver_cross_signing_*` | `e2e_cross_signing_*`, hidden `devices` | local users only; key JSON reconstructed from raw pubkey |
| OTK / fallback | `keyserver_one_time_keys`, `keyserver_fallback_keys` | `e2e_one_time_keys_json`, `e2e_fallback_keys_json` | local users only |
| Key backups | `userapi_key_backup*` | `e2e_room_keys*` | etag reset to 1 |
| Media (DB) | `mediaapi_media_repository` | `local_media_repository`, `remote_media_cache` | sha256 derived from Dendrite's base64hash |
| Media (files) | `<base>/h/h/ash/file` | `local_content/..`, `remote_content/..` | built-in, see `-dendrite-media` / `-synapse-media` |
| Rooms | `roomserver_rooms`, `roomserver_published` | `rooms` | `has_auth_chain_index = false` |
| Events | `roomserver_events`, `roomserver_event_json` | `events`, `event_json`, `state_events`, `room_memberships`, `event_auth`, `event_edges`, `redactions`, `rejections` | streamed via COPY |
| State | `roomserver_state_snapshots`, `roomserver_state_block` | `state_groups`, `state_groups_state`, `state_group_edges`, `event_to_state_groups` | rebuilt as delta chains, see below |
| Current state | `syncapi_current_room_state` | `current_state_events`, `local_current_membership` | |
| Extremities | `roomserver_rooms.latest_event_nids`, `syncapi_backward_extremities` | `event_forward_extremities`, `event_backward_extremities`, `room_depth`, `stream_ordering_to_exterm` | |
| Relations | `syncapi_relations` | `event_relations` | |
| Aliases | `roomserver_room_aliases` | `room_aliases`, `room_alias_servers` | |
| Receipts | `syncapi_receipts` | `receipts_linearized`, `receipts_graph` | `event_stream_ordering` backfilled from `events` |
| Server keys | `keydb_server_keys` | `server_signature_keys` | |

Background-update jobs for room/user stats, user directory and sliding-sync
tables are queued so Synapse rebuilds those itself on first start. The
`federation_stream_position`, `event_push_summary_stream_ordering` and
`event_push_summary_last_receipt_stream_id` cursors are advanced past the
imported data so Synapse does not try to re-send millions of PDUs over
federation or replay every historical receipt through `rotate_notifs`.

## What is NOT migrated

* Remote users' device lists / cross-signing keys (Synapse refetches over
  federation).
* Thumbnails (Synapse regenerates on demand).
* Presence, typing, to-device queue, federation send queues, notification
  counts, push rules (clients/federation will repopulate).
* `event_auth_chains` cover index — `rooms.has_auth_chain_index` is set to
  `false`; Synapse falls back to walking `event_auth`. Very large rooms will
  have slower state resolution; consider purging and re-joining those.

## State-group reconstruction

Dendrite stores state-before-event as a snapshot: a `state_snapshot_nid` that
points at an ordered list of `state_block_nids`, each block being a set of
event NIDs. Synapse stores state-at-event as a delta chain of `state_groups`.

The tool walks each room in `event_nid` order, tracking the per-room block
set so it can recognise the common transitions without re-reading the
database: an unchanged block set means state-before is unchanged, and a
single one-event block appended means exactly one state event was added.
Anything else (block compaction, DAG branches, backfill jumps) triggers a
full snapshot reload, so the in-memory state map is always identical to what
Dendrite would resolve.

Each emitted `state_group` is attached to a predecessor chosen by the same
multi-level `[100, 50, 25]` odometer as
[rust-synapse-compress-state](https://github.com/matrix-org/rust-synapse-compress-state):
full snapshots are written only when every level rolls over (≈ every
125 000 groups) or when no level head is a subset of the new state. This
makes `state_groups_state` several times smaller than what Synapse would
write natively, with a bounded resolution depth of ≤ 175 hops, and avoids
having to run the compressor as a separate post-migration pass.

## Performance

Indexes on the bulk tables (`events`, `event_json`, `event_auth`,
`event_edges`, `state_*`, …) are dropped before COPY and recreated
afterwards, turning random B-tree inserts into a single sequential
sort+build.

Measured on a 3.2 M-event / 670-room database (11 GB Dendrite, M-series
laptop, local PostgreSQL 16):

| | rows | time |
|---|---:|---:|
| events phase | 3 198 770 | ~5–6 min |
| state phase | 17 M `state_groups_state` (vs 97 M naïve) | ~12 min |
| everything else | | < 1 min |
| **total** | | **~18 min** |
| Synapse DB size | | **~12 GB** (vs ~31 GB without compression) |

Verified with Synapse 1.152: clean boot, full `/sync`, message history,
`/context` state resolution, E2EE key endpoints, key backup, send/receive,
account-data round-trip, no errors in `homeserver.log`.

## Usage

```bash
# 1. Let Synapse create its schema in an EMPTY database, then stop it.
createdb -E UTF8 -T template0 --locale=C synapse
synapse_homeserver -c homeserver.yaml   # wait until "running", then ^C

# 2. Run the migrator (Dendrite can be stopped or read-only).
go run ./cmd/dendrite-to-synapse \
    -dendrite "host=/run/postgresql dbname=dendrite" \
    -synapse  "host=/run/postgresql dbname=synapse" \
    -server-name example.org \
    -dendrite-media /var/lib/dendrite/media_store \
    -synapse-media  /var/lib/synapse/media_store \
    -media-mode hardlink   # or: copy | symlink

# 3. Copy the Dendrite signing key to Synapse (same ed25519 key, different
#    file format).
# 4. Start Synapse. Do NOT set password_config.pepper.
```

`-skip-events` runs only the cheap phases (users, e2ee, media, account data,
…) which is useful for a quick smoke test.

`-only events,state` re-runs selected phases. The `lookup` phase (NID →
string maps) always runs since every other phase depends on it.

## Media files

Dendrite stores media at
`<base_path>/<base64hash[0]>/<base64hash[1]>/<base64hash[2:]>/file` keyed by
content hash. Synapse stores local media at
`<media_store>/local_content/<id[0:2]>/<id[2:4]>/<id[4:]>` and remote media at
`<media_store>/remote_content/<origin>/<fid[0:2]>/<fid[2:4]>/<fid[4:]>`.

When `-dendrite-media` and `-synapse-media` are both given, the tool walks
`mediaapi_media_repository` and places every file into the Synapse layout.
`-media-mode` selects `copy` (default, atomic via `.tmp`+rename), `hardlink`
(zero-copy on the same filesystem, falls back to copy across filesystems) or
`symlink`. The phase is idempotent: existing destination files are skipped, so
it can be run again later with `-only mediafiles`.

Thumbnails are not copied; Synapse regenerates them on demand.

## Signing key

Synapse must present the **same** ed25519 signing key as Dendrite did, or
federation will reject every historical event. Convert
`matrix_key.pem` → Synapse `signing.key`:

```bash
go run ./cmd/dendrite-to-synapse/convert-key \
    -in /etc/dendrite/matrix_key.pem \
    -out /var/lib/synapse/signing.key
```
