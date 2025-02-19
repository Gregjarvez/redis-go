package stream

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewTrieStream(t *testing.T) {
	name := "test-stream"
	trie := NewTrieStream(name)

	// Assertions
	assert.Equal(t, name, trie.Name, "TrieStream name mismatch")
	assert.Nil(t, trie.Value, "TrieStream.Value should be nil on initialization")
}

func TestAddSingleEntry(t *testing.T) {
	trie := NewTrieStream("test-stream")

	id := "key1"
	entries := map[string]interface{}{
		"field1": "value1",
		"field2": "value2",
	}

	trie.Add(id, entries)

	// Assertions
	assert.NotNil(t, trie.Value, "Root node should be initialized")
	assert.Equal(t, id, trie.Value.Prefix, "Root node prefix mismatch")
	assert.Len(t, trie.Value.Entries, 1, "Root node should have exactly 1 entry")
	assert.Equal(t, id, trie.Value.Entries[0].Id, "Entry ID mismatch")
	assert.Equal(t, "value1", trie.Value.Entries[0].Elements["field1"], "Entry field1 value mismatch")
	assert.Equal(t, "value2", trie.Value.Entries[0].Elements["field2"], "Entry field2 value mismatch")
}

func TestAddWithCommonPrefix(t *testing.T) {
	trie := NewTrieStream("test-stream")

	// Add first entry
	id1 := "common-key1"
	entries1 := map[string]interface{}{
		"field1": "value1",
	}
	trie.Add(id1, entries1)

	// Add second entry with common prefix
	id2 := "common-key2"
	entries2 := map[string]interface{}{
		"field2": "value2",
	}
	trie.Add(id2, entries2)

	// Assertions
	assert.NotNil(t, trie.Value, "Root node should be initialized")
	assert.Equal(t, "common-key", trie.Value.Prefix, "Root node prefix mismatch with common prefix")

	// Validate children nodes
	child1, exists1 := trie.Value.Children['1']
	assert.True(t, exists1, "Child node '1' should exist")
	assert.Equal(t, child1.Prefix, "1", "Child node '1' prefix mismatch")
	assert.Len(t, child1.Entries, 1, "Child node '1' should have exactly 1 entry")
	assert.Equal(t, child1.Entries[0].Id, id1, "Child node '1' entry ID mismatch")

	child2, exists2 := trie.Value.Children['2']
	assert.True(t, exists2, "Child node '2' should exist")
	assert.Equal(t, child2.Prefix, "2", "Child node '2' prefix mismatch")
	assert.Len(t, child2.Entries, 1, "Child node '2' should have exactly 1 entry")
	assert.Equal(t, child2.Entries[0].Id, id2, "Child node '2' entry ID mismatch")
}

func TestGet(t *testing.T) {
	trie := NewTrieStream("test-stream")

	// Add multiple entries
	entries := []struct {
		id       string
		elements map[string]interface{}
	}{
		{"key1", map[string]interface{}{"field1": "value1"}},
		{"key2", map[string]interface{}{"field2": "value2"}},
		{"common-key", map[string]interface{}{"field3": "value3"}},
	}

	for _, entry := range entries {
		trie.Add(entry.id, entry.elements)
	}

	// Retrieve existing entry
	foundEntry := trie.Get("key1")
	assert.NotNil(t, foundEntry, "Expected to find entry with ID 'key1'")
	assert.Equal(t, "value1", foundEntry.Elements["field1"], "Field1 value does not match for 'key1'")

	foundEntry = trie.Get("common-key")
	assert.NotNil(t, foundEntry, "Expected to find entry with ID 'common-key'")
	assert.Equal(t, "value3", foundEntry.Elements["field3"], "Field3 value does not match for 'common-key'")

	// Try to retrieve a non-existing entry
	notFoundEntry := trie.Get("non-existent")
	assert.Nil(t, notFoundEntry, "Expected nil for non-existing entry")
}

func TestLongestCommonPrefix(t *testing.T) {
	tests := []struct {
		a, b, expected string
	}{
		{"abcde", "abcfg", "abc"},
		{"prefix1", "prefix2", "prefix"},
		{"different", "diffuse", "diff"},
		{"short", "shorter", "short"},
		{"no-match", "different", ""},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := longestCommonPrefix(tt.a, tt.b)
			assert.Equal(t, tt.expected, result, "Longest common prefix mismatch")
		})
	}
}

func TestAddAndRetrieveComplex(t *testing.T) {
	trie := NewTrieStream("test-stream")

	// Add entries with nested common prefixes
	trie.Add("alpha", map[string]interface{}{"field": "value-a"})
	trie.Add("alphabet", map[string]interface{}{"field": "value-ab"})
	trie.Add("alphabets", map[string]interface{}{"field": "value-abs"})

	// Retrieve each specific item
	entry := trie.Get("alpha")
	assert.NotNil(t, entry, "Expected to find 'alpha'")
	assert.Equal(t, "value-a", entry.Elements["field"], "Field value mismatch for 'alpha'")

	entry = trie.Get("alphabet")
	assert.NotNil(t, entry, "Expected to find 'alphabet'")
	assert.Equal(t, "value-ab", entry.Elements["field"], "Field value mismatch for 'alphabet'")

	entry = trie.Get("alphabets")
	assert.NotNil(t, entry, "Expected to find 'alphabets'")
	assert.Equal(t, "value-abs", entry.Elements["field"], "Field value mismatch for 'alphabets'")
}
