package main

import (
	"log"

	"github.com/lib/pq"
)

// migrateState reconstructs Synapse's state_groups model from Dendrite's
// state_snapshots / state_blocks.
//
// For every event we determine its exact state-before from Dendrite's
// state_snapshot_nid → state_block_nids → event_nids resolution. Tracking the
// per-room block set lets us recognise the overwhelmingly common transitions
// cheaply:
//
//   - same block set        → state-before unchanged
//   - one block appended    → a single state event was added; apply it
//   - anything else         → reload the full snapshot
//
// One Synapse state_group is emitted per state event (and per divergence). The
// prev/delta for each group is chosen using the same multi-level odometer as
// github.com/matrix-org/rust-synapse-compress-state so the resulting
// state_groups_state is already compressed.
func (m *migrator) migrateState() error {
	sg, err := m.newCopier("state_groups", "id", "room_id", "event_id")
	if err != nil {
		return err
	}
	sgs, err := m.newCopier("state_groups_state", "state_group", "room_id", "type", "state_key", "event_id")
	if err != nil {
		return err
	}
	sge, err := m.newCopier("state_group_edges", "state_group", "prev_state_group")
	if err != nil {
		return err
	}
	etsg, err := m.newCopier("event_to_state_groups", "event_id", "state_group")
	if err != nil {
		return err
	}

	cursors := map[int64]*roomCursor{}

	loader, err := m.newSnapshotLoader()
	if err != nil {
		return err
	}

	rows, err := m.src.Query(`
		SELECT e.room_nid, e.event_id, e.event_nid, e.event_type_nid, e.event_state_key_nid,
		       e.state_snapshot_nid, ss.state_block_nids
		FROM roomserver_events e
		LEFT JOIN roomserver_state_snapshots ss USING (state_snapshot_nid)
		ORDER BY e.event_nid`)
	if err != nil {
		return err
	}
	var total, fullSnaps, reloads int64
	for rows.Next() {
		var roomNID, eventNID, typeNID, skNID, snapNID int64
		var eventID string
		var blocks pq.Int64Array
		if err := rows.Scan(&roomNID, &eventID, &eventNID, &typeNID, &skNID, &snapNID, &blocks); err != nil {
			return err
		}

		roomID := m.roomNIDToID[roomNID]
		etype := m.eventTypeNID[typeNID]
		isState := skNID != 0

		if snapNID == 0 && etype != "m.room.create" {
			continue // outlier
		}

		c := cursors[roomNID]
		if c == nil {
			c = newRoomCursor()
			cursors[roomNID] = c
		}

		// Bring c.cur to state-before this event, exactly as Dendrite would
		// resolve state_snapshot_nid. After this call c.cur == resolve(blocks)
		// and equalsLast tells us whether that is the same map as the state
		// stored in c.lastSG.
		equalsLast, err := c.sync(blocks, loader)
		if err != nil {
			return err
		}
		if c.reloaded {
			reloads++
		}

		if !isState && equalsLast && c.lastSG != 0 {
			if err := etsg.add(eventID, c.lastSG); err != nil {
				return err
			}
			continue
		}

		// Compute state-at this event. We mutate c.cur in place and remember
		// the overwritten value so the resolve(curBlocks) invariant can be
		// restored before the next sync (handled inside sync via c.extra).
		if isState {
			c.applyExtra(eventNID, stateKey{etype, m.stateKeyNID[skNID]}, eventID)
		}

		gid := m.nextStateGroup
		m.nextStateGroup++
		if err := sg.add(gid, roomID, eventID); err != nil {
			return err
		}

		// Fast path: state-before == state-at-L0.head and we are adding one
		// key, so the delta is exactly that key and subset trivially holds.
		if isState && equalsLast && c.levels[0].head == c.lastSG &&
			c.levels[0].len > 0 && c.levels[0].len < c.levels[0].max {
			if err := sge.add(gid, c.levels[0].head); err != nil {
				return err
			}
			if err := sgs.add(gid, roomID, etype, c.extra.k.stateKey, eventID); err != nil {
				return err
			}
			c.levels[0].head, c.levels[0].headState = gid, nil
			c.levels[0].len++
		} else {
			prevSG, prevState := c.advance(gid)
			if prevSG == 0 {
				for k, v := range c.cur {
					if err := sgs.add(gid, roomID, k.etype, k.stateKey, v); err != nil {
						return err
					}
				}
				fullSnaps++
			} else {
				if err := sge.add(gid, prevSG); err != nil {
					return err
				}
				for k, v := range c.cur {
					if prevState[k] != v {
						if err := sgs.add(gid, roomID, k.etype, k.stateKey, v); err != nil {
							return err
						}
					}
				}
			}
		}

		if err := etsg.add(eventID, gid); err != nil {
			return err
		}
		c.lastSG = gid

		total++
		if total%50000 == 0 {
			log.Printf("  state: %d state_groups (%d full, %d reloads, %d rows)",
				total, fullSnaps, reloads, sgs.n)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	log.Printf("  state: %d full snapshots, %d reloads", fullSnaps, reloads)

	for _, c := range []*copier{sg, sgs, sge, etsg} {
		if err := c.close(); err != nil {
			return err
		}
	}
	return nil
}

type stateKey struct{ etype, stateKey string }
type stateMap map[stateKey]string

func (s stateMap) clone() stateMap {
	out := make(stateMap, len(s))
	for k, v := range s {
		out[k] = v
	}
	return out
}

type level struct {
	max       int
	len       int
	head      int64
	headState stateMap // nil for level 0 while on the fast path
}

// extra records a state event that has been applied to cur on top of what
// curBlocks resolves to, so it can be undone if the next event turns out not
// to descend from it (DAG sibling) or confirmed if the next snapshot appended
// exactly that event.
type extra struct {
	nid    int64
	k      stateKey
	old    string
	hadOld bool
}

type roomCursor struct {
	levels    []level
	cur       stateMap // == resolve(curBlocks) ∪ extra; equals state-at-lastSG between iterations
	curBlocks map[int64]struct{}
	extra     *extra
	lastSG    int64
	reloaded  bool // last sync() did a full reload (for stats / L0 headState handoff)
}

// Default level sizes match rust-synapse-compress-state. Max lookup hops = 175.
func newRoomCursor() *roomCursor {
	return &roomCursor{
		levels:    []level{{max: 100}, {max: 50}, {max: 25}},
		cur:       stateMap{},
		curBlocks: map[int64]struct{}{},
	}
}

// sync reconciles c.cur with the given snapshot block set so that on return
// c.cur == resolve(blocks). It reports whether the resulting map is identical
// to the state stored in c.lastSG (i.e. c.cur as it was on entry).
func (c *roomCursor) sync(blocks pq.Int64Array, l *snapshotLoader) (equalsLast bool, err error) {
	c.reloaded = false

	var added []int64
	removed := 0
	for _, b := range blocks {
		if _, ok := c.curBlocks[b]; !ok {
			added = append(added, b)
		}
	}
	if len(blocks)-len(added) != len(c.curBlocks) {
		removed = len(c.curBlocks) - (len(blocks) - len(added))
	}

	switch {
	case removed == 0 && len(added) == 0:
		// Same snapshot. c.cur may still carry extra; if so, state-before
		// this event lacks it (this event is a sibling of extra's event).
		if c.extra == nil {
			return true, nil
		}
		c.undoExtra()
		return false, nil

	case removed == 0 && len(added) == 1:
		ent, ok := l.singleBlock[added[0]]
		if ok && c.extra != nil && ent.nid == c.extra.nid {
			// Linear chain: the appended block is exactly the pending extra,
			// which is already in c.cur. Absorb it.
			c.curBlocks[added[0]] = struct{}{}
			c.extra = nil
			return true, nil
		}
		if ok && c.extra == nil {
			// A single foreign state event was appended (e.g. previous event
			// in this room was non-state). Apply it directly.
			c.cur[ent.k] = ent.v
			c.curBlocks[added[0]] = struct{}{}
			return false, nil
		}
		// Multi-entry block appended, or extra mismatch: fall through to reload.
	}

	// Hand the outgoing map to level 0 so advance() can still diff against it.
	c.levels[0].headState = c.cur
	c.cur, err = l.load(blocks)
	if err != nil {
		return false, err
	}
	c.curBlocks = make(map[int64]struct{}, len(blocks))
	for _, b := range blocks {
		c.curBlocks[b] = struct{}{}
	}
	c.extra = nil
	c.reloaded = true
	return false, nil
}

func (c *roomCursor) applyExtra(nid int64, k stateKey, v string) {
	old, had := c.cur[k]
	c.cur[k] = v
	c.extra = &extra{nid: nid, k: k, old: old, hadOld: had}
}

func (c *roomCursor) undoExtra() {
	if c.extra.hadOld {
		c.cur[c.extra.k] = c.extra.old
	} else {
		delete(c.cur, c.extra.k)
	}
	c.extra = nil
}

// advance moves the multi-level odometer for a new state group and returns the
// chosen predecessor (0 = emit full snapshot) plus its full state for diffing.
//
// Level 0 never stores headState (the fast path in migrateState handles the
// common case without cloning). When level 0 rolls over and a higher level is
// chosen, that level's stored headState is checked for the Synapse "deltas
// cannot remove keys" constraint; if it is not a subset of c.cur the search
// continues to the next level up before falling back to a full snapshot. This
// is a bounded version of rust-synapse-compress-state's chain walk-up.
func (c *roomCursor) advance(newSG int64) (int64, stateMap) {
	var prevSG int64
	var prevState stateMap
	picked := false
	for i := range c.levels {
		l := &c.levels[i]
		if !picked && l.len < l.max {
			if l.headState != nil && isSubset(l.headState, c.cur) {
				prevSG, prevState = l.head, l.headState
			}
			l.head, l.len = newSG, l.len+1
			if i > 0 {
				l.headState = c.cur.clone()
			} else {
				l.headState = nil // fast path will keep it nil; next reload repopulates
			}
			picked = true
			continue
		}
		if picked {
			if prevSG != 0 {
				return prevSG, prevState
			}
			// The picked level's head was not a valid base; try this level's
			// (older, usually smaller) head without advancing it.
			if l.head != 0 && l.headState != nil && isSubset(l.headState, c.cur) {
				return l.head, l.headState
			}
			continue
		}
		// Level full: roll it over and continue to the next.
		l.head, l.len = newSG, 1
		if i > 0 {
			l.headState = c.cur.clone()
		} else {
			l.headState = nil
		}
	}
	return prevSG, prevState
}

func isSubset(a, b stateMap) bool {
	if len(a) > len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

type blockEntry struct {
	nid int64 // event_nid
	k   stateKey
	v   string // event_id
}

// snapshotLoader materialises Dendrite snapshots. singleBlock indexes every
// one-event state block so the linear-append case (which accounts for ~95% of
// snapshot transitions) can be handled without a round trip.
type snapshotLoader struct {
	entryStmt   func(pq.Int64Array) (stateMap, error)
	singleBlock map[int64]blockEntry
}

func (l *snapshotLoader) load(blocks pq.Int64Array) (stateMap, error) {
	if len(blocks) == 0 {
		return stateMap{}, nil
	}
	return l.entryStmt(blocks)
}

func (m *migrator) newSnapshotLoader() (*snapshotLoader, error) {
	l := &snapshotLoader{singleBlock: map[int64]blockEntry{}}

	// A handful of events in the wild have event_state_key_nid=0 (Dendrite
	// never allocated a NID) yet are referenced from state blocks. Recover the
	// real state_key from the event JSON so they do not all collapse onto "".
	// Populated lazily on first encounter to avoid scanning every block.
	zeroSK := map[int64]string{}
	skStmt, err := m.src.Prepare(
		`SELECT event_json::json->>'state_key' FROM roomserver_event_json WHERE event_nid=$1`)
	if err != nil {
		return nil, err
	}
	resolveSK := func(skNID, eventNID int64) string {
		if skNID != 0 {
			return m.stateKeyNID[skNID]
		}
		if sk, ok := zeroSK[eventNID]; ok {
			return sk
		}
		var sk *string
		if err := skStmt.QueryRow(eventNID).Scan(&sk); err != nil || sk == nil {
			zeroSK[eventNID] = ""
			return ""
		}
		zeroSK[eventNID] = *sk
		return *sk
	}

	rows, err := m.src.Query(`
		SELECT b.state_block_nid, e.event_nid, e.event_id, e.event_type_nid, e.event_state_key_nid
		FROM roomserver_state_block b
		JOIN roomserver_events e ON e.event_nid = b.event_nids[1]
		WHERE cardinality(b.event_nids) = 1`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var bnid, enid, t, sk int64
		var id string
		if err := rows.Scan(&bnid, &enid, &id, &t, &sk); err != nil {
			return nil, err
		}
		l.singleBlock[bnid] = blockEntry{nid: enid, k: stateKey{m.eventTypeNID[t], resolveSK(sk, enid)}, v: id}
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	log.Printf("  state: %d single-event blocks indexed (%d sk-nid-0 fixups)", len(l.singleBlock), len(zeroSK))

	// Dendrite resolves duplicate (type, state_key) entries by block order:
	// later blocks win (roomserver/state LoadStateAtSnapshot). ORDINALITY
	// preserves that order so the map dedup keeps the correct entry.
	stmt, err := m.src.Prepare(`
		SELECT e.event_nid, e.event_id, e.event_type_nid, e.event_state_key_nid
		FROM unnest($1::bigint[]) WITH ORDINALITY AS bn(state_block_nid, ord)
		JOIN roomserver_state_block b USING (state_block_nid)
		JOIN LATERAL unnest(b.event_nids) AS u(event_nid) ON TRUE
		JOIN roomserver_events e ON e.event_nid = u.event_nid
		ORDER BY bn.ord`)
	if err != nil {
		return nil, err
	}
	l.entryStmt = func(blocks pq.Int64Array) (stateMap, error) {
		out := stateMap{}
		rows, err := stmt.Query(blocks)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id string
			var enid, t, sk int64
			if err := rows.Scan(&enid, &id, &t, &sk); err != nil {
				return nil, err
			}
			out[stateKey{m.eventTypeNID[t], resolveSK(sk, enid)}] = id
		}
		return out, rows.Close()
	}
	return l, nil
}

func (m *migrator) migrateCurrentState() error {
	rows, err := m.src.Query(`
		SELECT room_id, event_id, type, state_key, membership
		FROM syncapi_current_room_state`)
	if err != nil {
		return err
	}
	cse, err := m.newCopier("current_state_events",
		"event_id", "room_id", "type", "state_key", "membership")
	if err != nil {
		return err
	}
	lcm, err := m.newCopier("local_current_membership", "room_id", "user_id", "event_id", "membership")
	if err != nil {
		return err
	}
	for rows.Next() {
		var roomID, eventID, etype, sk string
		var membership *string
		if err := rows.Scan(&roomID, &eventID, &etype, &sk, &membership); err != nil {
			return err
		}
		var mship any
		if etype == "m.room.member" && membership != nil {
			mship = *membership
		}
		if err := cse.add(eventID, roomID, etype, sk, mship); err != nil {
			return err
		}
		if etype == "m.room.member" && m.isLocal(sk) && membership != nil {
			if err := lcm.add(roomID, sk, eventID, *membership); err != nil {
				return err
			}
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := cse.close(); err != nil {
		return err
	}
	if err := lcm.close(); err != nil {
		return err
	}

	// The per-row trigger SELECTs from events for every UPDATE; disable it
	// for the bulk backfill.
	for _, t := range []string{"current_state_events", "room_memberships", "local_current_membership"} {
		if _, err := m.dst.Exec("ALTER TABLE " + t + " DISABLE TRIGGER check_event_stream_ordering"); err != nil {
			return err
		}
	}
	defer func() {
		for _, t := range []string{"current_state_events", "room_memberships", "local_current_membership"} {
			_, _ = m.dst.Exec("ALTER TABLE " + t + " ENABLE TRIGGER check_event_stream_ordering")
		}
	}()
	if _, err := m.dst.Exec(`
		UPDATE current_state_events cse
		SET event_stream_ordering = e.stream_ordering
		FROM events e WHERE e.event_id = cse.event_id AND cse.event_stream_ordering IS NULL`); err != nil {
		return err
	}
	if _, err := m.dst.Exec(`
		UPDATE local_current_membership lcm
		SET event_stream_ordering = e.stream_ordering
		FROM events e WHERE e.event_id = lcm.event_id AND lcm.event_stream_ordering IS NULL`); err != nil {
		return err
	}
	if _, err := m.dst.Exec(`
		UPDATE room_memberships rm
		SET event_stream_ordering = e.stream_ordering
		FROM events e WHERE e.event_id = rm.event_id AND rm.event_stream_ordering IS NULL`); err != nil {
		return err
	}
	return nil
}
