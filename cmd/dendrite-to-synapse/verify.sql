-- Verification suite for dendrite-to-synapse output.
-- Run against the Synapse database; every "bad" column must be 0.
--
--   psql synapse -f verify.sql

\set QUIET on
\pset format aligned
\echo '=== REFERENTIAL INTEGRITY (expect bad=0) ==='
select 'profiles->users'              chk, count(*) bad from profiles p left join users u on u.name=p.full_user_id where u.name is null
union all select 'devices->users (visible)',       count(*) from devices d left join users u on u.name=d.user_id where u.name is null and not d.hidden
union all select 'access_tokens->users',           count(*) from access_tokens a left join users u on u.name=a.user_id where u.name is null
union all select 'access_tokens->devices',         count(*) from access_tokens a left join devices d on d.user_id=a.user_id and d.device_id=a.device_id where d.device_id is null
union all select 'user_filters->users',            count(*) from user_filters f left join users u on u.name=f.full_user_id where u.name is null
union all select 'account_data->users',            count(*) from account_data a left join users u on u.name=a.user_id where u.name is null
union all select 'room_account_data->users',       count(*) from room_account_data a left join users u on u.name=a.user_id where u.name is null
union all select 'pushers->users',                 count(*) from pushers p left join users u on u.name=p.user_name where u.name is null
union all select 'e2e_device_keys->devices',       count(*) from e2e_device_keys_json k left join devices d using(user_id,device_id) where d.device_id is null
union all select 'e2e_otk->devices',               count(*) from e2e_one_time_keys_json k left join devices d using(user_id,device_id) where d.device_id is null
union all select 'e2e_room_keys->versions',        count(*) from e2e_room_keys k left join e2e_room_keys_versions v using(user_id,version) where v.version is null
union all select 'e2e_xsign keydata is JSON',      count(*) from e2e_cross_signing_keys where keydata::jsonb->>'user_id' is null
union all select 'events->rooms',                  count(*) from events e left join rooms r using(room_id) where r.room_id is null
union all select 'event_json<->events',            abs((select count(*) from events)-(select count(*) from event_json))
union all select 'state_events->events',           count(*) from state_events s left join events e using(event_id) where e.event_id is null
union all select 'room_memberships->events',       count(*) from room_memberships m left join events e using(event_id) where e.event_id is null
union all select 'event_edges->events',            count(*) from event_edges ee left join events e on e.event_id=ee.event_id where e.event_id is null
union all select 'event_fwd_ext->events',          count(*) from event_forward_extremities f left join events e using(event_id) where e.event_id is null
union all select 'redactions->events',             count(*) from redactions rd left join events e using(event_id) where e.event_id is null
union all select 'rejections->events',             count(*) from rejections rj left join events e using(event_id) where e.event_id is null
union all select 'event_relations->events',        count(*) from event_relations r left join events e using(event_id) where e.event_id is null
union all select 'etsg->events',                   count(*) from event_to_state_groups g left join events e using(event_id) where e.event_id is null
union all select 'etsg->state_groups',             count(*) from event_to_state_groups g left join state_groups s on s.id=g.state_group where s.id is null
union all select 'non-outlier missing etsg',       count(*) from events e left join event_to_state_groups g using(event_id) where not e.outlier and g.event_id is null
union all select 'sg_edges->state_groups(self)',   count(*) from state_group_edges e left join state_groups g on g.id=e.state_group where g.id is null
union all select 'sg_edges->state_groups(prev)',   count(*) from state_group_edges e left join state_groups g on g.id=e.prev_state_group where g.id is null
union all select 'state_groups->rooms',            count(*) from state_groups s left join rooms r using(room_id) where r.room_id is null
union all select 'cse->events',                    count(*) from current_state_events c left join events e using(event_id) where e.event_id is null
union all select 'cse->state_events',              count(*) from current_state_events c left join state_events s using(event_id) where s.event_id is null
union all select 'cse stream_ordering match',      count(*) from current_state_events c join events e using(event_id) where c.event_stream_ordering is distinct from e.stream_ordering
union all select 'lcm->users',                     count(*) from local_current_membership l left join users u on u.name=l.user_id where u.name is null
union all select 'room_memberships so match',      count(*) from room_memberships m join events e using(event_id) where m.event_stream_ordering is distinct from e.stream_ordering
union all select 'room_aliases->rooms',            count(*) from room_aliases a left join rooms r using(room_id) where r.room_id is null
union all select 'dup events.stream_ordering',     count(*)-count(distinct stream_ordering) from events
union all select 'dup events.event_id',            count(*)-count(distinct event_id) from events
union all select 'dup e2e_xsign stream_id',        count(*)-count(distinct stream_id) from e2e_cross_signing_keys
order by 1;

\echo '=== SEQUENCES (seq must be > max) ==='
select 'events_stream_seq'               s, (select last_value from events_stream_seq) seq, (select coalesce(max(stream_ordering),0) from events) maxv
union all select 'state_group_id_seq',      (select last_value from state_group_id_seq), (select coalesce(max(id),0) from state_groups)
union all select 'account_data_sequence',   (select last_value from account_data_sequence), greatest(coalesce((select max(stream_id) from account_data),0),coalesce((select max(stream_id) from room_account_data),0))
union all select 'receipts_sequence',       (select last_value from receipts_sequence), (select coalesce(max(stream_id),0) from receipts_linearized)
union all select 'e2e_cross_signing_keys_sequence', (select last_value from e2e_cross_signing_keys_sequence), (select coalesce(max(stream_id),0) from e2e_cross_signing_keys)
union all select 'pushers_sequence',        (select last_value from pushers_sequence), (select coalesce(max(id),0) from pushers);
