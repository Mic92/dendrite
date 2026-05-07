// Command dendrite-to-synapse migrates a Dendrite PostgreSQL database into a
// freshly-initialised Synapse PostgreSQL database.
//
// The Synapse database MUST already contain the Synapse schema (i.e. start
// Synapse once against an empty database so it creates all tables, then stop
// it, then run this tool). The tool only INSERTs data; it does not create
// tables.
//
// The tool is intentionally a standalone binary so that it can stream the
// (potentially multi-GB) event tables using PostgreSQL COPY without pulling in
// the rest of Dendrite.
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/lib/pq"
)

type migrator struct {
	src        *sql.DB // dendrite
	dst        *sql.DB // synapse
	serverName string

	roomNIDToID      map[int64]string
	roomNIDToVersion map[int64]string
	eventTypeNID     map[int64]string
	stateKeyNID      map[int64]string

	nextStreamOrdering int64
	nextStateGroup     int64
	nextAccountData    int64
	nextDeviceList     int64
	nextXSignStream    int64
	nextReceipt        int64
	nextPusher         int64

	skipEvents bool
}

func main() {
	var (
		srcConn    = flag.String("dendrite", "", "PostgreSQL connection string for the Dendrite database (required)")
		dstConn    = flag.String("synapse", "", "PostgreSQL connection string for the Synapse database (required)")
		serverName = flag.String("server-name", "", "Local homeserver name (required, e.g. example.org)")
		only       = flag.String("only", "", "Comma separated list of phases to run (default: all). Phases: users,e2ee,media,mediafiles,rooms,events,state,currentstate,extremities,receipts,accountdata,aliases,serverkeys,pushers,relations,sequences")
		skipEvents = flag.Bool("skip-events", false, "Skip the (slow) events/state phases; only migrate users/e2ee/media/etc.")
		dMedia     = flag.String("dendrite-media", "", "Dendrite media base_path (if set together with -synapse-media, media files are copied)")
		sMedia     = flag.String("synapse-media", "", "Synapse media_store_path")
		mediaMode  = flag.String("media-mode", "copy", "How to place media files: copy | hardlink | symlink")
	)
	flag.Parse()

	if *srcConn == "" || *dstConn == "" || *serverName == "" {
		flag.Usage()
		os.Exit(2)
	}

	src, err := sql.Open("postgres", *srcConn)
	if err != nil {
		log.Fatalf("open dendrite db: %v", err)
	}
	dst, err := sql.Open("postgres", *dstConn)
	if err != nil {
		log.Fatalf("open synapse db: %v", err)
	}
	if err := src.Ping(); err != nil {
		log.Fatalf("ping dendrite db: %v", err)
	}
	if err := dst.Ping(); err != nil {
		log.Fatalf("ping synapse db: %v", err)
	}

	m := &migrator{
		src:                src,
		dst:                dst,
		serverName:         *serverName,
		roomNIDToID:        map[int64]string{},
		roomNIDToVersion:   map[int64]string{},
		eventTypeNID:       map[int64]string{},
		stateKeyNID:        map[int64]string{},
		nextStreamOrdering: 1,
		nextStateGroup:     1,
		nextAccountData:    2,
		nextDeviceList:     2,
		nextXSignStream:    2,
		nextReceipt:        2,
		nextPusher:         1,
		skipEvents:         *skipEvents,
	}

	enabled := map[string]bool{}
	if *only != "" {
		for _, p := range strings.Split(*only, ",") {
			enabled[strings.TrimSpace(p)] = true
		}
	}
	run := func(name string, fn func() error) {
		if len(enabled) > 0 && !enabled[name] && name != "lookup" {
			log.Printf("[skip] %s", name)
			return
		}
		t0 := time.Now()
		log.Printf("[start] %s", name)
		if err := fn(); err != nil {
			log.Fatalf("[fail] %s: %v", name, err)
		}
		log.Printf("[done] %s in %s", name, time.Since(t0).Truncate(time.Millisecond))
	}

	run("lookup", m.loadLookups)
	run("users", m.migrateUsers)
	run("e2ee", m.migrateE2EE)
	run("media", m.migrateMedia)
	if *dMedia != "" && *sMedia != "" {
		run("mediafiles", func() error { return m.migrateMediaFiles(*dMedia, *sMedia, *mediaMode) })
	} else {
		log.Printf("[skip] mediafiles (set -dendrite-media and -synapse-media to copy files)")
	}
	run("rooms", m.migrateRooms)
	if !m.skipEvents {
		run("events", func() error {
			return m.withoutIndexes(
				[]string{"events", "event_json", "event_auth", "event_edges", "state_events", "room_memberships", "redactions"},
				m.migrateEvents)
		})
		run("state", func() error {
			return m.withoutIndexes(
				[]string{"state_groups_state", "state_groups", "state_group_edges", "event_to_state_groups"},
				m.migrateState)
		})
		run("currentstate", m.migrateCurrentState)
		run("extremities", m.migrateExtremities)
		run("relations", m.migrateRelations)
	}
	run("aliases", m.migrateAliases)
	run("accountdata", m.migrateAccountData)
	run("receipts", m.migrateReceipts)
	run("pushers", m.migratePushers)
	run("serverkeys", m.migrateServerKeys)
	run("sequences", m.bumpSequences)

	log.Printf("migration complete")
	if *dMedia == "" || *sMedia == "" {
		log.Printf("NOTE: media files were NOT copied. Re-run with -dendrite-media and -synapse-media (and -only mediafiles) to copy them.")
	}
	if !m.skipEvents {
		log.Printf("NOTE: rooms.has_auth_chain_index is set to false; Synapse will fall back to slow auth-chain queries. Consider purging and rejoining very large rooms.")
	}
}

func (m *migrator) loadLookups() error {
	rows, err := m.src.Query(`SELECT room_nid, room_id, room_version FROM roomserver_rooms`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var nid int64
		var id, ver string
		if err := rows.Scan(&nid, &id, &ver); err != nil {
			return err
		}
		m.roomNIDToID[nid] = id
		m.roomNIDToVersion[nid] = ver
	}
	if err := rows.Close(); err != nil {
		return err
	}

	rows, err = m.src.Query(`SELECT event_type_nid, event_type FROM roomserver_event_types`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var nid int64
		var t string
		if err := rows.Scan(&nid, &t); err != nil {
			return err
		}
		m.eventTypeNID[nid] = t
	}
	if err := rows.Close(); err != nil {
		return err
	}

	rows, err = m.src.Query(`SELECT event_state_key_nid, event_state_key FROM roomserver_event_state_keys`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var nid int64
		var k string
		if err := rows.Scan(&nid, &k); err != nil {
			return err
		}
		m.stateKeyNID[nid] = k
	}
	return rows.Close()
}

func (m *migrator) userID(localpart string) string {
	return "@" + localpart + ":" + m.serverName
}

func (m *migrator) isLocal(mxid string) bool {
	return strings.HasSuffix(mxid, ":"+m.serverName)
}

func formatVersion(roomVersion string) int {
	switch roomVersion {
	case "1", "2":
		return 1
	case "3":
		return 2
	case "4", "5", "6", "7", "8", "9", "10", "11",
		"org.matrix.msc2176", "org.matrix.msc3787", "org.matrix.msc2716v4",
		"org.matrix.msc3667", "org.matrix.msc3757.10", "org.matrix.msc3757.11":
		return 3
	default: // "12", hydra, ...
		return 4
	}
}

type copier struct {
	tx   *sql.Tx
	stmt *sql.Stmt
	n    int64
	name string
}

func (m *migrator) newCopier(table string, cols ...string) (*copier, error) {
	tx, err := m.dst.Begin()
	if err != nil {
		return nil, err
	}
	stmt, err := tx.Prepare(pq.CopyIn(table, cols...))
	if err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("prepare COPY %s: %w", table, err)
	}
	return &copier{tx: tx, stmt: stmt, name: table}, nil
}

func (c *copier) add(args ...any) error {
	c.n++
	_, err := c.stmt.Exec(args...)
	return err
}

func (c *copier) close() error {
	if _, err := c.stmt.Exec(); err != nil {
		c.tx.Rollback()
		return fmt.Errorf("flush COPY %s: %w", c.name, err)
	}
	if err := c.stmt.Close(); err != nil {
		c.tx.Rollback()
		return err
	}
	if err := c.tx.Commit(); err != nil {
		return err
	}
	log.Printf("  %-40s %d rows", c.name, c.n)
	return nil
}
