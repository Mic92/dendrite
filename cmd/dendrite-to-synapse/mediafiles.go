package main

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
)

// migrateMediaFiles relocates media from Dendrite's content-addressed layout
// (<base>/<h0>/<h1>/<h2:>/file) into Synapse's media_store layout
// (local_content/<id[0:2]>/<id[2:4]>/<id[4:]> and
// remote_content/<origin>/<fid[0:2]>/<fid[2:4]>/<fid[4:]>).
func (m *migrator) migrateMediaFiles(src, dst, mode string) error {
	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("dendrite media path: %w", err)
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}

	place := placeCopy
	switch mode {
	case "copy", "":
		place = placeCopy
	case "hardlink":
		place = placeHardlink
	case "symlink":
		place = placeSymlink
	default:
		return fmt.Errorf("unknown -media-mode %q (want copy|hardlink|symlink)", mode)
	}

	rows, err := m.src.Query(`SELECT media_id, media_origin, base64hash FROM mediaapi_media_repository`)
	if err != nil {
		return err
	}
	defer rows.Close()

	var total, copied, skipped, missing int
	for rows.Next() {
		var id, origin, hash string
		if err := rows.Scan(&id, &origin, &hash); err != nil {
			return err
		}
		total++

		from, ok := dendriteMediaPath(src, hash)
		if !ok {
			missing++
			continue
		}
		if _, err := os.Stat(from); err != nil {
			missing++
			continue
		}

		var to string
		if origin == m.serverName {
			to, ok = synapseLocalPath(dst, id)
		} else {
			to, ok = synapseRemotePath(dst, origin, id)
		}
		if !ok {
			log.Printf("  warn: skip %s/%s: id too short for Synapse layout", origin, id)
			missing++
			continue
		}

		if _, err := os.Stat(to); err == nil {
			skipped++
			continue
		}
		if err := os.MkdirAll(filepath.Dir(to), 0o755); err != nil {
			return err
		}
		if err := place(from, to); err != nil {
			return fmt.Errorf("%s -> %s: %w", from, to, err)
		}
		copied++
		if copied%1000 == 0 {
			log.Printf("  media files: %d copied, %d skipped, %d missing", copied, skipped, missing)
		}
	}
	log.Printf("  media files: total=%d copied=%d skipped=%d missing=%d", total, copied, skipped, missing)
	return rows.Err()
}

func dendriteMediaPath(base, hash string) (string, bool) {
	if len(hash) < 3 {
		return "", false
	}
	return filepath.Join(base, hash[0:1], hash[1:2], hash[2:], "file"), true
}

func synapseLocalPath(base, id string) (string, bool) {
	if len(id) < 5 {
		return "", false
	}
	return filepath.Join(base, "local_content", id[0:2], id[2:4], id[4:]), true
}

func synapseRemotePath(base, origin, fid string) (string, bool) {
	if len(fid) < 5 {
		return "", false
	}
	return filepath.Join(base, "remote_content", origin, fid[0:2], fid[2:4], fid[4:]), true
}

func placeCopy(from, to string) error {
	in, err := os.Open(from)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := to + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, to)
}

func placeHardlink(from, to string) error {
	if err := os.Link(from, to); err != nil {
		return placeCopy(from, to)
	}
	return nil
}

func placeSymlink(from, to string) error {
	abs, err := filepath.Abs(from)
	if err != nil {
		return err
	}
	return os.Symlink(abs, to)
}

// sha256HexFromBase64Hash converts Dendrite's RawURLEncoding(sha256) into the
// hex digest Synapse stores. Returns nil for malformed input so the column
// stays NULL.
func sha256HexFromBase64Hash(b64 string) any {
	raw, err := base64.RawURLEncoding.DecodeString(b64)
	if err != nil || len(raw) != 32 {
		return nil
	}
	return hex.EncodeToString(raw)
}
