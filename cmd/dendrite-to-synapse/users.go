package main

import (
	"database/sql"
	"encoding/json"
	"time"
)

func (m *migrator) migrateUsers() error {
	rows, err := m.src.Query(`
		SELECT localpart, created_ts, password_hash, appservice_id, is_deactivated, account_type
		FROM userapi_accounts WHERE server_name = $1`, m.serverName)
	if err != nil {
		return err
	}
	users, err := m.newCopier("users",
		"name", "password_hash", "creation_ts", "admin", "is_guest",
		"appservice_id", "deactivated", "shadow_banned", "approved", "locked", "suspended")
	if err != nil {
		return err
	}
	for rows.Next() {
		var localpart string
		var createdTS int64
		var pwHash, appsvc sql.NullString
		var deactivated sql.NullBool
		var acctType int
		if err := rows.Scan(&localpart, &createdTS, &pwHash, &appsvc, &deactivated, &acctType); err != nil {
			return err
		}
		var admin, guest, deact int
		if acctType == 3 {
			admin = 1
		}
		if acctType == 2 {
			guest = 1
		}
		if deactivated.Valid && deactivated.Bool {
			deact = 1
		}
		// Dendrite stores creation in ms, Synapse in seconds.
		if err := users.add(m.userID(localpart), pwHash, createdTS/1000, admin, guest,
			appsvc, deact, false, true, false, false); err != nil {
			return err
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := users.close(); err != nil {
		return err
	}

	rows, err = m.src.Query(`
		SELECT localpart, COALESCE(display_name,''), COALESCE(avatar_url,'')
		FROM userapi_profiles WHERE server_name = $1`, m.serverName)
	if err != nil {
		return err
	}
	profiles, err := m.newCopier("profiles", "user_id", "displayname", "avatar_url", "full_user_id")
	if err != nil {
		return err
	}
	for rows.Next() {
		var localpart, dn, av string
		if err := rows.Scan(&localpart, &dn, &av); err != nil {
			return err
		}
		if err := profiles.add(localpart, nullEmpty(dn), nullEmpty(av), m.userID(localpart)); err != nil {
			return err
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := profiles.close(); err != nil {
		return err
	}

	rows, err = m.src.Query(`
		SELECT access_token, session_id, device_id, localpart, created_ts, display_name,
		       last_seen_ts, ip, user_agent
		FROM userapi_devices WHERE server_name = $1`, m.serverName)
	if err != nil {
		return err
	}
	devices, err := m.newCopier("devices", "user_id", "device_id", "display_name", "last_seen", "ip", "user_agent", "hidden")
	if err != nil {
		return err
	}
	tokens, err := m.newCopier("access_tokens", "id", "user_id", "device_id", "token", "last_validated")
	if err != nil {
		return err
	}
	ips, err := m.newCopier("user_ips", "user_id", "access_token", "device_id", "ip", "user_agent", "last_seen")
	if err != nil {
		return err
	}
	for rows.Next() {
		var token, devID, localpart string
		var sessionID, createdTS, lastSeen int64
		var dn, ip, ua sql.NullString
		if err := rows.Scan(&token, &sessionID, &devID, &localpart, &createdTS, &dn, &lastSeen, &ip, &ua); err != nil {
			return err
		}
		uid := m.userID(localpart)
		if err := devices.add(uid, devID, dn, lastSeen, ip, ua, false); err != nil {
			return err
		}
		if err := tokens.add(sessionID, uid, devID, token, lastSeen); err != nil {
			return err
		}
		if ip.Valid && ip.String != "" {
			if err := ips.add(uid, token, devID, ip.String, ua.String, lastSeen); err != nil {
				return err
			}
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := devices.close(); err != nil {
		return err
	}
	if err := tokens.close(); err != nil {
		return err
	}
	if err := ips.close(); err != nil {
		return err
	}

	rows, err = m.src.Query(`SELECT threepid, medium, localpart FROM userapi_threepids WHERE server_name = $1`, m.serverName)
	if err != nil {
		return err
	}
	tp, err := m.newCopier("user_threepids", "user_id", "medium", "address", "validated_at", "added_at")
	if err != nil {
		return err
	}
	now := time.Now().UnixMilli()
	for rows.Next() {
		var addr, medium, localpart string
		if err := rows.Scan(&addr, &medium, &localpart); err != nil {
			return err
		}
		if err := tp.add(m.userID(localpart), medium, addr, now, now); err != nil {
			return err
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := tp.close(); err != nil {
		return err
	}

	rows, err = m.src.Query(`SELECT id, localpart, filter FROM syncapi_filter`)
	if err != nil {
		return err
	}
	filters, err := m.newCopier("user_filters", "user_id", "filter_id", "filter_json", "full_user_id")
	if err != nil {
		return err
	}
	for rows.Next() {
		var id int64
		var localpart, filter string
		if err := rows.Scan(&id, &localpart, &filter); err != nil {
			return err
		}
		if err := filters.add(localpart, id, []byte(filter), m.userID(localpart)); err != nil {
			return err
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := filters.close(); err != nil {
		return err
	}

	rows, err = m.src.Query(`SELECT token, localpart, token_expires_at_ms FROM userapi_openid_tokens WHERE server_name = $1`, m.serverName)
	if err != nil {
		return err
	}
	oid, err := m.newCopier("open_id_tokens", "token", "ts_valid_until_ms", "user_id")
	if err != nil {
		return err
	}
	for rows.Next() {
		var token, localpart string
		var exp int64
		if err := rows.Scan(&token, &localpart, &exp); err != nil {
			return err
		}
		if err := oid.add(token, exp, m.userID(localpart)); err != nil {
			return err
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	return oid.close()
}

func (m *migrator) migrateAccountData() error {
	rows, err := m.src.Query(`SELECT localpart, room_id, type, content FROM userapi_account_datas WHERE server_name = $1`, m.serverName)
	if err != nil {
		return err
	}
	global, err := m.newCopier("account_data", "user_id", "account_data_type", "stream_id", "content")
	if err != nil {
		return err
	}
	room, err := m.newCopier("room_account_data", "user_id", "room_id", "account_data_type", "stream_id", "content")
	if err != nil {
		return err
	}
	tags, err := m.newCopier("room_tags", "user_id", "room_id", "tag", "content")
	if err != nil {
		return err
	}
	ignored, err := m.newCopier("ignored_users", "ignorer_user_id", "ignored_user_id")
	if err != nil {
		return err
	}
	for rows.Next() {
		var localpart, roomID, typ, content string
		if err := rows.Scan(&localpart, &roomID, &typ, &content); err != nil {
			return err
		}
		uid := m.userID(localpart)
		// Synapse synthesises m.push_rules from its push_rules table on every
		// sync; a stale blob in account_data would never be updated. Dendrite's
		// push-rule storage format also differs, so we drop it here and let
		// users start from Synapse defaults.
		if typ == "m.push_rules" {
			continue
		}
		sid := m.nextAccountData
		m.nextAccountData++
		if roomID == "" {
			if err := global.add(uid, typ, sid, content); err != nil {
				return err
			}
			if typ == "m.ignored_user_list" {
				var parsed struct {
					IgnoredUsers map[string]any `json:"ignored_users"`
				}
				if json.Unmarshal([]byte(content), &parsed) == nil {
					for u := range parsed.IgnoredUsers {
						if err := ignored.add(uid, u); err != nil {
							return err
						}
					}
				}
			}
		} else {
			if typ == "m.tag" {
				var parsed struct {
					Tags map[string]json.RawMessage `json:"tags"`
				}
				if json.Unmarshal([]byte(content), &parsed) == nil {
					for tag, c := range parsed.Tags {
						if err := tags.add(uid, roomID, tag, string(c)); err != nil {
							return err
						}
					}
				}
				continue
			}
			if err := room.add(uid, roomID, typ, sid, content); err != nil {
				return err
			}
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := global.close(); err != nil {
		return err
	}
	if err := room.close(); err != nil {
		return err
	}
	if err := tags.close(); err != nil {
		return err
	}
	return ignored.close()
}

func (m *migrator) migratePushers() error {
	rows, err := m.src.Query(`
		SELECT localpart, profile_tag, kind, app_id, app_display_name, device_display_name,
		       pushkey, pushkey_ts_ms, lang, data
		FROM userapi_pushers WHERE server_name = $1`, m.serverName)
	if err != nil {
		return err
	}
	p, err := m.newCopier("pushers",
		"id", "user_name", "profile_tag", "kind", "app_id", "app_display_name",
		"device_display_name", "pushkey", "ts", "lang", "data", "enabled")
	if err != nil {
		return err
	}
	for rows.Next() {
		var localpart, kind, appID, appDN, devDN, pushkey, lang, data string
		var profileTag sql.NullString
		var ts int64
		if err := rows.Scan(&localpart, &profileTag, &kind, &appID, &appDN, &devDN, &pushkey, &ts, &lang, &data); err != nil {
			return err
		}
		id := m.nextPusher
		m.nextPusher++
		if err := p.add(id, m.userID(localpart), profileTag.String, kind, appID, appDN, devDN, pushkey, ts, lang, data, true); err != nil {
			return err
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	return p.close()
}
