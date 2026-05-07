package main

import (
	"encoding/json"
)

// migrateE2EE copies device keys, cross-signing keys/signatures, one-time and
// fallback keys, and server-side key backups for *local* users only. Remote
// users' keys are intentionally dropped: Synapse will refetch them over
// federation on demand and Dendrite stores them in a different shape anyway.
func (m *migrator) migrateE2EE() error {
	if err := m.migrateDeviceKeys(); err != nil {
		return err
	}
	if err := m.migrateCrossSigning(); err != nil {
		return err
	}
	if err := m.migrateOTKFallback(); err != nil {
		return err
	}
	return m.migrateKeyBackups()
}

func (m *migrator) migrateDeviceKeys() error {
	rows, err := m.src.Query(`SELECT user_id, device_id, ts_added_secs, key_json FROM keyserver_device_keys`)
	if err != nil {
		return err
	}
	dk, err := m.newCopier("e2e_device_keys_json", "user_id", "device_id", "ts_added_ms", "key_json")
	if err != nil {
		return err
	}
	for rows.Next() {
		var uid, did, kj string
		var ts int64
		if err := rows.Scan(&uid, &did, &ts, &kj); err != nil {
			return err
		}
		if !m.isLocal(uid) || kj == "" {
			continue
		}
		if err := dk.add(uid, did, ts*1000, kj); err != nil {
			return err
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	return dk.close()
}

func (m *migrator) migrateCrossSigning() error {
	type target struct{ user, key string }
	type sig struct{ originUser, originKey, sigVal string }
	sigsByTarget := map[target][]sig{}
	{
		rows, err := m.src.Query(`SELECT origin_user_id, origin_key_id, target_user_id, target_key_id, signature FROM keyserver_cross_signing_sigs`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var ou, ok, tu, tk, s string
			if err := rows.Scan(&ou, &ok, &tu, &tk, &s); err != nil {
				return err
			}
			sigsByTarget[target{tu, tk}] = append(sigsByTarget[target{tu, tk}], sig{ou, ok, s})
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}

	purpose := map[int]string{1: "master", 2: "self_signing", 3: "user_signing"}

	rows, err := m.src.Query(`SELECT user_id, key_type, key_data FROM keyserver_cross_signing_keys`)
	if err != nil {
		return err
	}
	ck, err := m.newCopier("e2e_cross_signing_keys", "user_id", "keytype", "keydata", "stream_id")
	if err != nil {
		return err
	}
	// Synapse models cross-signing keys as hidden devices so that signature
	// lookups by device_id find them.
	hiddenDev, err := m.newCopier("devices", "user_id", "device_id", "hidden")
	if err != nil {
		return err
	}
	for rows.Next() {
		var uid, kd string
		var kt int
		if err := rows.Scan(&uid, &kt, &kd); err != nil {
			return err
		}
		if !m.isLocal(uid) {
			continue
		}
		p, ok := purpose[kt]
		if !ok {
			continue
		}
		keyID := "ed25519:" + kd
		obj := map[string]any{
			"user_id": uid,
			"usage":   []string{p},
			"keys":    map[string]string{keyID: kd},
		}
		// Embed signatures targeting this key so the reconstructed JSON
		// matches what the client originally uploaded.
		if ss := sigsByTarget[target{uid, kd}]; len(ss) > 0 {
			signatures := map[string]map[string]string{}
			for _, s := range ss {
				if signatures[s.originUser] == nil {
					signatures[s.originUser] = map[string]string{}
				}
				signatures[s.originUser][s.originKey] = s.sigVal
			}
			obj["signatures"] = signatures
		}
		blob, _ := json.Marshal(obj)
		sid := m.nextXSignStream
		m.nextXSignStream++
		if err := ck.add(uid, p, string(blob), sid); err != nil {
			return err
		}
		if err := hiddenDev.add(uid, kd, true); err != nil {
			return err
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := ck.close(); err != nil {
		return err
	}
	if err := hiddenDev.close(); err != nil {
		return err
	}

	cs, err := m.newCopier("e2e_cross_signing_signatures", "user_id", "key_id", "target_user_id", "target_device_id", "signature")
	if err != nil {
		return err
	}
	for t, ss := range sigsByTarget {
		for _, s := range ss {
			if !m.isLocal(s.originUser) {
				continue
			}
			if err := cs.add(s.originUser, s.originKey, t.user, t.key, s.sigVal); err != nil {
				return err
			}
		}
	}
	return cs.close()
}

func (m *migrator) migrateOTKFallback() error {
	rows, err := m.src.Query(`SELECT user_id, device_id, key_id, algorithm, ts_added_secs, key_json FROM keyserver_one_time_keys`)
	if err != nil {
		return err
	}
	otk, err := m.newCopier("e2e_one_time_keys_json", "user_id", "device_id", "algorithm", "key_id", "ts_added_ms", "key_json")
	if err != nil {
		return err
	}
	for rows.Next() {
		var uid, did, kid, alg, kj string
		var ts int64
		if err := rows.Scan(&uid, &did, &kid, &alg, &ts, &kj); err != nil {
			return err
		}
		if !m.isLocal(uid) {
			continue
		}
		if err := otk.add(uid, did, alg, kid, ts*1000, kj); err != nil {
			return err
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := otk.close(); err != nil {
		return err
	}

	rows, err = m.src.Query(`SELECT user_id, device_id, key_id, algorithm, key_json, used FROM keyserver_fallback_keys`)
	if err != nil {
		return err
	}
	fb, err := m.newCopier("e2e_fallback_keys_json", "user_id", "device_id", "algorithm", "key_id", "key_json", "used")
	if err != nil {
		return err
	}
	for rows.Next() {
		var uid, did, kid, alg, kj string
		var used bool
		if err := rows.Scan(&uid, &did, &kid, &alg, &kj, &used); err != nil {
			return err
		}
		if !m.isLocal(uid) {
			continue
		}
		if err := fb.add(uid, did, alg, kid, kj, used); err != nil {
			return err
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	return fb.close()
}

func (m *migrator) migrateKeyBackups() error {
	// Versions: Synapse expects etag as bigint; Dendrite stores an opaque
	// string. We cannot recover the original counter, so start each version's
	// etag at 1 — clients will see a changed etag and resync, which is the
	// safe behaviour after a server migration anyway.
	rows, err := m.src.Query(`SELECT user_id, version, algorithm, auth_data, deleted FROM userapi_key_backup_versions`)
	if err != nil {
		return err
	}
	ver, err := m.newCopier("e2e_room_keys_versions", "user_id", "version", "algorithm", "auth_data", "deleted", "etag")
	if err != nil {
		return err
	}
	for rows.Next() {
		var uid, alg, auth string
		var v int64
		var del int
		if err := rows.Scan(&uid, &v, &alg, &auth, &del); err != nil {
			return err
		}
		if err := ver.add(uid, v, alg, auth, del, 1); err != nil {
			return err
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := ver.close(); err != nil {
		return err
	}

	rows, err = m.src.Query(`SELECT user_id, room_id, session_id, version, first_message_index, forwarded_count, is_verified, session_data FROM userapi_key_backups`)
	if err != nil {
		return err
	}
	bk, err := m.newCopier("e2e_room_keys", "user_id", "room_id", "session_id", "version", "first_message_index", "forwarded_count", "is_verified", "session_data")
	if err != nil {
		return err
	}
	for rows.Next() {
		var uid, rid, sid, sdata string
		var ver int64
		var fmi, fc int
		var iv bool
		// Dendrite stores version as text; Synapse as bigint. lib/pq parses
		// the text into int64 for us.
		if err := rows.Scan(&uid, &rid, &sid, &ver, &fmi, &fc, &iv, &sdata); err != nil {
			return err
		}
		if err := bk.add(uid, rid, sid, ver, fmi, fc, iv, sdata); err != nil {
			return err
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	return bk.close()
}
