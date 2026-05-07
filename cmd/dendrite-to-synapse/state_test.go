package main

import (
	"testing"

	"github.com/lib/pq"
)

func mkLoader() *snapshotLoader {
	return &snapshotLoader{
		singleBlock: map[int64]blockEntry{
			10: {nid: 100, k: stateKey{"m.room.member", "@a"}, v: "$a1"},
			11: {nid: 101, k: stateKey{"m.room.member", "@b"}, v: "$b1"},
			12: {nid: 102, k: stateKey{"m.room.member", "@c"}, v: "$c1"},
		},
		entryStmt: func(blocks pq.Int64Array) (stateMap, error) {
			full := map[int64]blockEntry{
				10: {nid: 100, k: stateKey{"m.room.member", "@a"}, v: "$a1"},
				11: {nid: 101, k: stateKey{"m.room.member", "@b"}, v: "$b1"},
				12: {nid: 102, k: stateKey{"m.room.member", "@c"}, v: "$c1"},
				20: {nid: 200, k: stateKey{"m.room.name", ""}, v: "$n"},
			}
			out := stateMap{}
			for _, b := range blocks {
				if e, ok := full[b]; ok {
					out[e.k] = e.v
				}
			}
			return out, nil
		},
	}
}

func TestSyncLinearChain(t *testing.T) {
	l := mkLoader()
	c := newRoomCursor()

	// 1) m.room.create: blocks={}, isState. equalsLast=true (empty==empty).
	eq, _ := c.sync(nil, l)
	if !eq || len(c.cur) != 0 {
		t.Fatalf("create: eq=%v cur=%v", eq, c.cur)
	}
	c.applyExtra(100, stateKey{"m.room.member", "@a"}, "$a1")
	c.lastSG = 1

	// 2) next state event, blocks={10} (the appended block IS extra) → absorb.
	eq, _ = c.sync(pq.Int64Array{10}, l)
	if !eq || c.extra != nil || c.cur[stateKey{"m.room.member", "@a"}] != "$a1" {
		t.Fatalf("absorb: eq=%v extra=%v cur=%v", eq, c.extra, c.cur)
	}
	c.applyExtra(101, stateKey{"m.room.member", "@b"}, "$b1")
	c.lastSG = 2

	// 3) sibling: same blocks={10}, extra still pending → undo.
	eq, _ = c.sync(pq.Int64Array{10}, l)
	if eq {
		t.Fatal("sibling should not equalsLast")
	}
	if _, ok := c.cur[stateKey{"m.room.member", "@b"}]; ok {
		t.Fatal("sibling: extra not undone")
	}
	if c.cur[stateKey{"m.room.member", "@a"}] != "$a1" {
		t.Fatal("sibling: lost @a")
	}
	c.lastSG = 3

	// 4) foreign append: blocks={10,12}, no extra → apply directly.
	eq, _ = c.sync(pq.Int64Array{10, 12}, l)
	if eq || c.cur[stateKey{"m.room.member", "@c"}] != "$c1" {
		t.Fatalf("foreign append: eq=%v cur=%v", eq, c.cur)
	}
	c.lastSG = 4

	// 5) divergence: blocks={20} → full reload.
	eq, _ = c.sync(pq.Int64Array{20}, l)
	if eq || !c.reloaded {
		t.Fatalf("reload: eq=%v reloaded=%v", eq, c.reloaded)
	}
	if len(c.cur) != 1 || c.cur[stateKey{"m.room.name", ""}] != "$n" {
		t.Fatalf("reload: wrong cur %v", c.cur)
	}
}

func TestSyncUndoRestoresOld(t *testing.T) {
	l := mkLoader()
	c := newRoomCursor()
	c.sync(pq.Int64Array{10}, l) // cur={@a:$a1}
	c.applyExtra(999, stateKey{"m.room.member", "@a"}, "$a2")
	if c.cur[stateKey{"m.room.member", "@a"}] != "$a2" {
		t.Fatal("applyExtra didn't overwrite")
	}
	// sibling with same blocks → undo to $a1
	eq, _ := c.sync(pq.Int64Array{10}, l)
	if eq || c.cur[stateKey{"m.room.member", "@a"}] != "$a1" {
		t.Fatalf("undo: eq=%v cur=%v", eq, c.cur)
	}
}

func TestAdvanceOdometer(t *testing.T) {
	c := &roomCursor{levels: []level{{max: 3, len: 3, head: 3}, {max: 2}}}
	c.cur = stateMap{}
	for i, want := range []int64{0, 4, 4} {
		c.cur[stateKey{"t", string(rune('a' + i))}] = "$x"
		prev, _ := c.advance(int64(4 + i))
		if prev != want {
			t.Errorf("sg=%d: prev=%d want %d", 4+i, prev, want)
		}
	}
}

func TestAdvanceSubsetWalkUp(t *testing.T) {
	c := &roomCursor{levels: []level{{max: 3}, {max: 2}, {max: 2}}}
	c.levels[0].len, c.levels[0].head = 3, 9
	c.levels[1].len, c.levels[1].head = 1, 5
	c.levels[1].headState = stateMap{{"m.room.member", "@gone"}: "$a", {"m.room.name", ""}: "$n"}
	c.levels[2].len, c.levels[2].head = 1, 1
	c.levels[2].headState = stateMap{{"m.room.name", ""}: "$n"}
	c.cur = stateMap{{"m.room.name", ""}: "$n2", {"m.room.topic", ""}: "$t"}

	prev, prevState := c.advance(10)
	if prev != 1 || prevState[stateKey{"m.room.name", ""}] != "$n" {
		t.Fatalf("expected walk-up to L2, got prev=%d", prev)
	}
	if c.levels[1].head != 10 || c.levels[2].head != 1 {
		t.Fatalf("levels not advanced correctly: %+v", c.levels)
	}
}

func TestIsSubset(t *testing.T) {
	a := stateMap{{"m.room.member", "@a"}: "$1", {"m.room.name", ""}: "$2"}
	b := stateMap{{"m.room.member", "@a"}: "$1", {"m.room.name", ""}: "$3", {"m.room.topic", ""}: "$4"}
	if !isSubset(a, b) || isSubset(b, a) || !isSubset(stateMap{}, a) {
		t.Fatal("isSubset broken")
	}
}
