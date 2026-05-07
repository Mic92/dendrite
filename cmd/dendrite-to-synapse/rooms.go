package main

import "encoding/base64"

// migrateRooms creates the bare room rows. Everything event-related is filled
// in by later phases. has_auth_chain_index is left false because rebuilding
// Synapse's auth-chain cover index from Dendrite data would effectively mean
// reimplementing Synapse's algorithm; Synapse copes without it (slower state
// res on very large rooms).
func (m *migrator) migrateRooms() error {
	r, err := m.newCopier("rooms", "room_id", "is_public", "creator", "room_version", "has_auth_chain_index")
	if err != nil {
		return err
	}
	pub := map[string]bool{}
	{
		rows, err := m.src.Query(`SELECT room_id, published FROM roomserver_published WHERE appservice_id = '' AND network_id = ''`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var id string
			var p bool
			if err := rows.Scan(&id, &p); err != nil {
				return err
			}
			pub[id] = p
		}
		rows.Close()
	}
	for nid, id := range m.roomNIDToID {
		if err := r.add(id, pub[id], nil, m.roomNIDToVersion[nid], false); err != nil {
			return err
		}
	}
	return r.close()
}

func (m *migrator) migrateAliases() error {
	rows, err := m.src.Query(`SELECT alias, room_id, creator_id FROM roomserver_room_aliases`)
	if err != nil {
		return err
	}
	a, err := m.newCopier("room_aliases", "room_alias", "room_id", "creator")
	if err != nil {
		return err
	}
	srv, err := m.newCopier("room_alias_servers", "room_alias", "server")
	if err != nil {
		return err
	}
	for rows.Next() {
		var alias, room, creator string
		if err := rows.Scan(&alias, &room, &creator); err != nil {
			return err
		}
		if err := a.add(alias, room, nullEmpty(creator)); err != nil {
			return err
		}
		if err := srv.add(alias, m.serverName); err != nil {
			return err
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := a.close(); err != nil {
		return err
	}
	return srv.close()
}

func (m *migrator) migrateServerKeys() error {
	rows, err := m.src.Query(`SELECT server_name, server_key_id, valid_until_ts, server_key FROM keydb_server_keys`)
	if err != nil {
		return err
	}
	sk, err := m.newCopier("server_signature_keys", "server_name", "key_id", "from_server", "ts_added_ms", "verify_key", "ts_valid_until_ms")
	if err != nil {
		return err
	}
	for rows.Next() {
		var name, kid, key string
		var validUntil int64
		if err := rows.Scan(&name, &kid, &validUntil, &key); err != nil {
			return err
		}
		raw, err := base64.RawStdEncoding.DecodeString(key)
		if err != nil {
			continue
		}
		if err := sk.add(name, kid, name, validUntil, raw, validUntil); err != nil {
			return err
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	return sk.close()
}
