package common

import "testing"

// RequireMatchesIterator drains it and compares each entry to the
// expected batch using testing.T helpers. Fails immediately on mismatch.
func RequireMatchesIterator(t *testing.T, iter EntryIterator, expected []*Entry) {
	t.Helper()

	for i := range expected {
		entry, err := iter.Next()
		if err != nil {
			t.Fatalf("unexpected iterator error: %v", err)
		}
		if entry == nil {
			t.Fatalf("iterator exhausted at index %d", i)
		}
		if !entriesEqual(entry, expected[i]) {
			t.Fatalf("entry mismatch at %d: got %+v want %+v", i, entry, expected[i])
		}
	}

	entry, err := iter.Next()
	if err != nil {
		t.Fatalf("unexpected iterator error at end: %v", err)
	}
	if entry != nil {
		t.Fatalf("expected iterator to be exhausted, got %+v", entry)
	}
}

func entriesEqual(a, b *Entry) bool {
	return a.Type == b.Type && a.Seq == b.Seq && string(a.Key) == string(b.Key) && string(a.Value) == string(b.Value)
}
