package ack

import (
	"testing"

	"github.com/kordar/goetl/checkpoint"
)

func TestStrictAckTracker_Order(t *testing.T) {
	t.Parallel()
	tk := NewStrictAckTracker()
	tk.Add("a", &checkpoint.Cursor{Values: []any{1}})
	tk.Add("b", &checkpoint.Cursor{Values: []any{2}})
	if _, ok := tk.Commit(); ok {
		t.Fatalf("commit before ack should be false")
	}
	tk.Ack("a")
	cur, ok := tk.Commit()
	if !ok || cur.Values[0] != 1 {
		t.Fatalf("commit1=%v ok=%v", cur, ok)
	}
	tk.Ack("b")
	cur, ok = tk.Commit()
	if !ok || cur.Values[0] != 2 {
		t.Fatalf("commit2=%v ok=%v", cur, ok)
	}
	if _, ok := tk.Commit(); ok {
		t.Fatalf("commit3 should be false")
	}
}
