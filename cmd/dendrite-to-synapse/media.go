package main

func (m *migrator) migrateMedia() error {
	rows, err := m.src.Query(`
		SELECT media_id, content_type, file_size_bytes, creation_ts, upload_name, user_id, base64hash
		FROM mediaapi_media_repository WHERE media_origin = $1`, m.serverName)
	if err != nil {
		return err
	}
	local, err := m.newCopier("local_media_repository",
		"media_id", "media_type", "media_length", "created_ts", "upload_name", "user_id",
		"safe_from_quarantine", "authenticated", "sha256")
	if err != nil {
		return err
	}
	for rows.Next() {
		var id, ct, name, uid, hash string
		var size, ts int64
		if err := rows.Scan(&id, &ct, &size, &ts, &name, &uid, &hash); err != nil {
			return err
		}
		if err := local.add(id, ct, size, ts, nullEmpty(name), nullEmpty(uid), false, false,
			sha256HexFromBase64Hash(hash)); err != nil {
			return err
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := local.close(); err != nil {
		return err
	}

	// filesystem_id := media_id so migrateMediaFiles can place remote files
	// without an extra lookup table.
	rows, err = m.src.Query(`
		SELECT media_id, media_origin, content_type, file_size_bytes, creation_ts, upload_name, base64hash
		FROM mediaapi_media_repository WHERE media_origin <> $1`, m.serverName)
	if err != nil {
		return err
	}
	remote, err := m.newCopier("remote_media_cache",
		"media_origin", "media_id", "media_type", "created_ts", "upload_name", "media_length",
		"filesystem_id", "authenticated", "sha256")
	if err != nil {
		return err
	}
	for rows.Next() {
		var id, origin, ct, name, hash string
		var size, ts int64
		if err := rows.Scan(&id, &origin, &ct, &size, &ts, &name, &hash); err != nil {
			return err
		}
		if err := remote.add(origin, id, ct, ts, nullEmpty(name), size, id, false,
			sha256HexFromBase64Hash(hash)); err != nil {
			return err
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	return remote.close()
}

func nullEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
