package main

import (
	"fmt"
	"log"
	"time"

	"github.com/lib/pq"
)

// withoutIndexes drops every plain (non-constraint-backing, non-FK-referenced)
// index on the given tables, runs fn, then recreates them. Bulk COPY into a
// table with many B-tree indexes is dominated by random index page writes;
// building the index once afterwards is a sequential sort+write and typically
// several times faster.
func (m *migrator) withoutIndexes(tables []string, fn func() error) error {
	rows, err := m.dst.Query(`
		SELECT i.indexrelid::regclass::text, pg_get_indexdef(i.indexrelid)
		FROM pg_index i
		JOIN pg_class t ON t.oid = i.indrelid
		WHERE t.relname = ANY($1)
		  AND t.relnamespace = 'public'::regnamespace
		  AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = i.indexrelid)
		  AND NOT EXISTS (SELECT 1 FROM pg_constraint fk WHERE fk.conindid = i.indexrelid AND fk.contype = 'f')
		  AND NOT EXISTS (SELECT 1 FROM pg_constraint fk WHERE fk.confrelid = i.indrelid AND fk.conindid = i.indexrelid)`,
		pq.Array(tables))
	if err != nil {
		return err
	}
	type idx struct{ name, def string }
	var dropped []idx
	for rows.Next() {
		var name, def string
		if err := rows.Scan(&name, &def); err != nil {
			return err
		}
		dropped = append(dropped, idx{name, def})
	}
	if err := rows.Close(); err != nil {
		return err
	}

	for _, ix := range dropped {
		if _, err := m.dst.Exec("DROP INDEX IF EXISTS " + pq.QuoteIdentifier(ix.name)); err != nil {
			return fmt.Errorf("drop %s: %w", ix.name, err)
		}
	}
	log.Printf("  dropped %d indexes on %v", len(dropped), tables)

	if err := fn(); err != nil {
		// Best-effort restore so a failed run does not leave the schema broken.
		for _, ix := range dropped {
			_, _ = m.dst.Exec(ix.def)
		}
		return err
	}

	t0 := time.Now()
	for _, ix := range dropped {
		if _, err := m.dst.Exec(ix.def); err != nil {
			return fmt.Errorf("recreate %s: %w", ix.name, err)
		}
	}
	log.Printf("  recreated %d indexes in %s", len(dropped), time.Since(t0).Truncate(time.Millisecond))
	return nil
}
