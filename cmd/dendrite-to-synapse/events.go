package main

import (
	"encoding/json"
	"log"
)

type rawEvent struct {
	Type           string            `json:"type"`
	StateKey       *string           `json:"state_key"`
	Sender         string            `json:"sender"`
	RoomID         string            `json:"room_id"`
	Depth          int64             `json:"depth"`
	OriginServerTS int64             `json:"origin_server_ts"`
	PrevEvents     []json.RawMessage `json:"prev_events"`
	AuthEvents     []json.RawMessage `json:"auth_events"`
	Redacts        string            `json:"redacts"`
	Content        struct {
		Membership  string `json:"membership"`
		DisplayName string `json:"displayname"`
		AvatarURL   string `json:"avatar_url"`
		URL         string `json:"url"`
		Redacts     string `json:"redacts"`
	} `json:"content"`
}

// eventIDFromRef extracts an event ID from a prev_events / auth_events entry,
// which is either a bare string (room v3+) or a [id, {hashes}] pair (room v1/2).
func eventIDFromRef(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var pair []json.RawMessage
	if json.Unmarshal(raw, &pair) == nil && len(pair) > 0 {
		if json.Unmarshal(pair[0], &s) == nil {
			return s
		}
	}
	return ""
}

// migrateEvents streams every event from Dendrite into Synapse's events,
// event_json, state_events, room_memberships, event_auth, event_edges,
// redactions and rejections tables.
//
// stream_ordering is allocated densely in event_nid order, which preserves the
// order in which Dendrite originally persisted the events and is therefore a
// reasonable approximation of Synapse's own ordering.
func (m *migrator) migrateEvents() error {
	events, err := m.newCopier("events",
		"topological_ordering", "event_id", "type", "room_id", "processed", "outlier",
		"depth", "origin_server_ts", "received_ts", "sender", "contains_url",
		"stream_ordering", "state_key", "rejection_reason")
	if err != nil {
		return err
	}
	ejson, err := m.newCopier("event_json", "event_id", "room_id", "internal_metadata", "json", "format_version")
	if err != nil {
		return err
	}
	stateEv, err := m.newCopier("state_events", "event_id", "room_id", "type", "state_key")
	if err != nil {
		return err
	}
	memb, err := m.newCopier("room_memberships",
		"event_id", "user_id", "sender", "room_id", "membership", "display_name", "avatar_url",
		"event_stream_ordering")
	if err != nil {
		return err
	}
	eauth, err := m.newCopier("event_auth", "event_id", "auth_id", "room_id")
	if err != nil {
		return err
	}
	edges, err := m.newCopier("event_edges", "event_id", "prev_event_id", "is_state")
	if err != nil {
		return err
	}
	redact, err := m.newCopier("redactions", "event_id", "redacts", "have_censored", "received_ts")
	if err != nil {
		return err
	}
	reject, err := m.newCopier("rejections", "event_id", "reason", "last_check")
	if err != nil {
		return err
	}

	// One streaming query: lib/pq fetches rows lazily, so this does not
	// buffer 3M rows in memory. Batching with LIMIT/offset would re-scan
	// roomserver_event_json from the start on every batch (no lower bound on
	// the merge-join's second input), turning the phase O(n²).
	rows, err := m.src.Query(`
		SELECT e.room_nid, e.event_type_nid, e.event_state_key_nid,
		       e.state_snapshot_nid, e.depth, e.event_id, e.is_rejected, j.event_json
		FROM roomserver_events e
		JOIN roomserver_event_json j USING (event_nid)
		ORDER BY e.event_nid`)
	if err != nil {
		return err
	}
	var total int64
	for rows.Next() {
		var roomNID, typeNID, skNID, snapNID, depth int64
		var eventID, eventJSON string
		var rejected bool
		if err := rows.Scan(&roomNID, &typeNID, &skNID, &snapNID, &depth, &eventID, &rejected, &eventJSON); err != nil {
			return err
		}

		roomID := m.roomNIDToID[roomNID]
		roomVer := m.roomNIDToVersion[roomNID]
		fmtVer := formatVersion(roomVer)
		etype := m.eventTypeNID[typeNID]

		var ev rawEvent
		if err := json.Unmarshal([]byte(eventJSON), &ev); err != nil {
			log.Printf("  warn: bad event JSON for %s: %v", eventID, err)
			continue
		}
		if ev.RoomID != "" {
			roomID = ev.RoomID
		}
		if ev.Type != "" {
			etype = ev.Type
		}

		// Dendrite occasionally has event_state_key_nid=0 for events that
		// do carry a state_key in their JSON (observed for a handful of
		// federated membership events). Trust the JSON in that case so
		// the event still ends up in state_events.
		isState := skNID != 0 || ev.StateKey != nil
		outlier := snapNID == 0 && etype != "m.room.create"
		containsURL := ev.Content.URL != ""

		so := m.nextStreamOrdering
		m.nextStreamOrdering++

		var stateKey string
		var stateKeyVal any
		if isState {
			if ev.StateKey != nil {
				stateKey = *ev.StateKey
			} else {
				stateKey = m.stateKeyNID[skNID]
			}
			stateKeyVal = stateKey
		}
		var rejReason any
		if rejected {
			rejReason = "auth_error"
		}

		if err := events.add(depth, eventID, etype, roomID, true, outlier, depth,
			ev.OriginServerTS, ev.OriginServerTS, ev.Sender, containsURL, so,
			stateKeyVal, rejReason); err != nil {
			return err
		}

		im := `{}`
		if outlier {
			im = `{"outlier":true}`
		}
		if err := ejson.add(eventID, roomID, im, eventJSON, fmtVer); err != nil {
			return err
		}

		if isState {
			if err := stateEv.add(eventID, roomID, etype, stateKey); err != nil {
				return err
			}
			// Synapse only records room_memberships for events that are
			// part of the room DAG; outlier membership events (e.g.
			// invite/join stubs received over federation before we had the
			// room) are skipped.
			if etype == "m.room.member" && !outlier {
				if err := memb.add(eventID, stateKey, ev.Sender, roomID, ev.Content.Membership,
					nullEmpty(ev.Content.DisplayName), nullEmpty(ev.Content.AvatarURL), so); err != nil {
					return err
				}
			}
		}

		if !outlier {
			for _, a := range ev.AuthEvents {
				if id := eventIDFromRef(a); id != "" {
					if err := eauth.add(eventID, id, roomID); err != nil {
						return err
					}
				}
			}
			for _, p := range ev.PrevEvents {
				if id := eventIDFromRef(p); id != "" {
					if err := edges.add(eventID, id, false); err != nil {
						return err
					}
				}
			}
		}

		if etype == "m.room.redaction" {
			target := ev.Redacts
			if target == "" {
				target = ev.Content.Redacts
			}
			if target != "" {
				if err := redact.add(eventID, target, false, ev.OriginServerTS); err != nil {
					return err
				}
			}
		}

		if rejected {
			if err := reject.add(eventID, "auth_error", ""); err != nil {
				return err
			}
		}

		total++
		if total%100000 == 0 {
			log.Printf("  events: %d", total)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}

	for _, c := range []*copier{events, ejson, stateEv, memb, eauth, edges, redact, reject} {
		if err := c.close(); err != nil {
			return err
		}
	}

	return nil
}

func (m *migrator) migrateRelations() error {
	rows, err := m.src.Query(`SELECT child_event_id, event_id, rel_type FROM syncapi_relations`)
	if err != nil {
		return err
	}
	rel, err := m.newCopier("event_relations", "event_id", "relates_to_id", "relation_type")
	if err != nil {
		return err
	}
	seen := map[string]struct{}{}
	for rows.Next() {
		var child, parent, rt string
		if err := rows.Scan(&child, &parent, &rt); err != nil {
			return err
		}
		if _, ok := seen[child]; ok {
			continue
		}
		seen[child] = struct{}{}
		if err := rel.add(child, parent, rt); err != nil {
			return err
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	return rel.close()
}

func (m *migrator) migrateExtremities() error {
	fwd, err := m.newCopier("event_forward_extremities", "event_id", "room_id")
	if err != nil {
		return err
	}
	rows, err := m.src.Query(`
		SELECT r.room_id, e.event_id
		FROM roomserver_rooms r, unnest(r.latest_event_nids) nid
		JOIN roomserver_events e ON e.event_nid = nid`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var roomID, eventID string
		if err := rows.Scan(&roomID, &eventID); err != nil {
			return err
		}
		if err := fwd.add(eventID, roomID); err != nil {
			return err
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := fwd.close(); err != nil {
		return err
	}

	// room_depth: Synapse tracks the *minimum* depth ever seen per room (used
	// to decide whether backfill can go further). Compute it from the events
	// table we just populated rather than from the forward extremities.
	if _, err := m.dst.Exec(`
		INSERT INTO room_depth(room_id, min_depth)
		SELECT room_id, MIN(depth) FROM events WHERE NOT outlier GROUP BY room_id
		ON CONFLICT (room_id) DO UPDATE SET min_depth = LEAST(room_depth.min_depth, EXCLUDED.min_depth)`); err != nil {
		return err
	}

	if _, err := m.dst.Exec(`
		INSERT INTO stream_ordering_to_exterm(stream_ordering, room_id, event_id)
		SELECT e.stream_ordering, f.room_id, f.event_id
		FROM event_forward_extremities f JOIN events e USING (event_id)`); err != nil {
		return err
	}

	rows, err = m.src.Query(`SELECT DISTINCT room_id, prev_event_id FROM syncapi_backward_extremities`)
	if err != nil {
		return err
	}
	bwd, err := m.newCopier("event_backward_extremities", "event_id", "room_id")
	if err != nil {
		return err
	}
	for rows.Next() {
		var room, prev string
		if err := rows.Scan(&room, &prev); err != nil {
			return err
		}
		if err := bwd.add(prev, room); err != nil {
			return err
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	return bwd.close()
}

func (m *migrator) migrateReceipts() error {
	rows, err := m.src.Query(`SELECT room_id, receipt_type, user_id, event_id, receipt_ts FROM syncapi_receipts`)
	if err != nil {
		return err
	}
	lin, err := m.newCopier("receipts_linearized",
		"stream_id", "room_id", "receipt_type", "user_id", "event_id", "data")
	if err != nil {
		return err
	}
	graph, err := m.newCopier("receipts_graph", "room_id", "receipt_type", "user_id", "event_ids", "data")
	if err != nil {
		return err
	}
	for rows.Next() {
		var room, rt, uid, eid string
		var ts int64
		if err := rows.Scan(&room, &rt, &uid, &eid, &ts); err != nil {
			return err
		}
		data, _ := json.Marshal(map[string]int64{"ts": ts})
		sid := m.nextReceipt
		m.nextReceipt++
		if err := lin.add(sid, room, rt, uid, eid, string(data)); err != nil {
			return err
		}
		eids, _ := json.Marshal([]string{eid})
		if err := graph.add(room, rt, uid, string(eids), string(data)); err != nil {
			return err
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := lin.close(); err != nil {
		return err
	}
	if err := graph.close(); err != nil {
		return err
	}
	// rotate_notifs asserts event_stream_ordering is non-NULL; backfill it now
	// that the events table is populated.
	_, err = m.dst.Exec(`
		UPDATE receipts_linearized r
		SET event_stream_ordering = e.stream_ordering
		FROM events e WHERE e.event_id = r.event_id`)
	return err
}

func (m *migrator) bumpSequences() error {
	type seq struct {
		name string
		val  int64
	}
	for _, s := range []seq{
		{"events_stream_seq", m.nextStreamOrdering},
		{"state_group_id_seq", m.nextStateGroup},
		{"account_data_sequence", m.nextAccountData},
		{"device_lists_sequence", m.nextDeviceList},
		{"e2e_cross_signing_keys_sequence", m.nextXSignStream},
		{"receipts_sequence", m.nextReceipt},
		{"pushers_sequence", m.nextPusher},
	} {
		if s.val < 2 {
			// The phase that allocates this counter was skipped (-only); leave
			// the sequence untouched rather than rewinding it.
			continue
		}
		if _, err := m.dst.Exec("SELECT setval($1, $2)", s.name, s.val); err != nil {
			return err
		}
	}
	for _, sp := range []struct {
		name string
		val  int64
	}{
		{"events", m.nextStreamOrdering},
		{"account_data", m.nextAccountData},
		{"receipts", m.nextReceipt},
	} {
		if _, err := m.dst.Exec(
			`INSERT INTO stream_positions(stream_name, instance_name, stream_id) VALUES ($1,'master',$2)
			 ON CONFLICT(stream_name, instance_name) DO UPDATE SET stream_id = EXCLUDED.stream_id`,
			sp.name, sp.val); err != nil {
			return err
		}
	}
	// Point background loops at "now" so they do not replay history that we
	// did not migrate. federation_stream_position prevents 3M PDUs being
	// re-sent over federation; the event_push_summary positions stop
	// rotate_notifs from walking 50K+ historical receipts (some of which
	// reference events we never had and would trip an assertion on a NULL
	// event_stream_ordering).
	for _, q := range []string{
		`UPDATE federation_stream_position SET stream_id = $1 WHERE type = 'events'`,
		`UPDATE event_push_summary_stream_ordering SET stream_ordering = $1`,
	} {
		if _, err := m.dst.Exec(q, m.nextStreamOrdering); err != nil {
			return err
		}
	}
	if _, err := m.dst.Exec(
		`UPDATE event_push_summary_last_receipt_stream_id SET stream_id = $1`,
		m.nextReceipt); err != nil {
		return err
	}
	if _, err := m.dst.Exec(`UPDATE event_push_summary_stream_ordering SET stream_ordering = $1`,
		m.nextStreamOrdering); err != nil {
		return err
	}
	if _, err := m.dst.Exec(`UPDATE appservice_stream_position SET stream_ordering = $1`,
		m.nextStreamOrdering); err != nil {
		log.Printf("  warn: appservice_stream_position: %v", err)
	}
	// Queue background updates so Synapse rebuilds its derived tables.
	for _, bg := range []struct{ name, dep string }{
		{"populate_stats_process_rooms", ""},
		{"populate_stats_process_users", ""},
		{"populate_user_directory_createtables", ""},
		{"populate_user_directory_process_rooms", "populate_user_directory_createtables"},
		{"populate_user_directory_process_users", "populate_user_directory_process_rooms"},
		{"populate_user_directory_cleanup", "populate_user_directory_process_users"},
		{"sliding_sync_prefill_joined_rooms_to_recalculate_table_bg_update", ""},
		{"sliding_sync_joined_rooms_bg_update", "sliding_sync_prefill_joined_rooms_to_recalculate_table_bg_update"},
		{"sliding_sync_membership_snapshots_bg_update", ""},
	} {
		var dep any
		if bg.dep != "" {
			dep = bg.dep
		}
		if _, err := m.dst.Exec(
			`INSERT INTO background_updates(update_name, progress_json, depends_on) VALUES ($1,'{}',$2) ON CONFLICT DO NOTHING`,
			bg.name, dep); err != nil {
			return err
		}
	}
	return nil
}
