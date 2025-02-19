package stream

import (
	"errors"
	"fmt"
)

type Entry struct {
	Id       string
	Elements map[string]interface{}
}

type Node struct {
	Prefix   string
	Entries  []*Entry
	Children map[byte]*Node
}

// Stream  Compressed prefix tree.
type Stream struct {
	Name       string
	Value      *Node
	TailPrefix string
	length     int64
}

func (s *Stream) GetType() string {
	return "stream"
}

func (s *Stream) GetValue() string {
	return ""
}

func (s *Stream) IsExpired() bool {
	return false
}

func NewTrieStream(name string) *Stream {
	return &Stream{
		Name:  name,
		Value: nil,
	}
}

func (s *Stream) Add(id string, entries map[string]interface{}) error {
	current := s.Value
	err := s.validatePrefix(id)

	s.length++
	s.TailPrefix = id

	if err != nil {
		return err
	}

	for {
		if current == nil {
			s.Value = &Node{
				Prefix:   id,
				Entries:  []*Entry{{Id: id, Elements: entries}},
				Children: make(map[byte]*Node),
			}
			return nil
		}

		commonPrefix := longestCommonPrefix(id, current.Prefix)
		// prefix at node : computing
		// inserting id : computer
		// longest common prefix will be comput
		// comput != computing. Split computing into comput and ing
		// root
		if commonPrefix != current.Prefix {
			child := &Node{
				Prefix:   current.Prefix[len(commonPrefix):],
				Entries:  current.Entries,
				Children: current.Children,
			}

			current.Prefix = commonPrefix
			current.Entries = nil
			current.Children = map[byte]*Node{
				child.Prefix[0]: child,
			}

			if len(id) > len(commonPrefix) {
				current.Children[id[len(commonPrefix)]] = &Node{
					Prefix: id[len(commonPrefix):],
					Entries: []*Entry{
						{Id: id, Elements: entries},
					},
				}
			} else {
				current.Entries = append(current.Entries, &Entry{Id: id, Elements: entries})
			}
			return nil
		}

		id = id[len(commonPrefix):]
		child, exists := current.Children[id[0]]

		if !exists {
			current.Children[id[0]] = &Node{
				Prefix: id,
				Entries: []*Entry{
					{Id: id, Elements: entries},
				},
			}
			return nil
		}

		current = child
	}
}

func (s *Stream) Get(id string) *Entry {
	entryId := id
	current := s.Value
	/*
		Say we need to find the entry for with id computer
		get the longest common prefix
		compare the lcp to the current node lcp
		if they do not match. id is not in the trie

		id = id - lcp
		if the current
		check the children[id[0]] range the map to find the id

	*/
	for {
		if current == nil {
			return nil
		}

		commonPrefix := longestCommonPrefix(entryId, current.Prefix)
		entryId = entryId[len(commonPrefix):]

		if len(entryId) == 0 {
			for _, entry := range current.Entries {
				if entry.Id == id {
					return entry
				}
			}
			return nil
		}

		child, exists := current.Children[entryId[0]]
		if !exists {
			return nil
		}

		current = child
	}
}

func (s *Stream) validatePrefix(id string) error {
	fmt.Println("Validating prefix: ", id, " length: ", s.length, " tailPrefix: ", s.TailPrefix)

	if id <= "0-0" {
		return errors.New("ERR The ID specified in XADD must be greater than 0-0")
	}

	if id <= s.TailPrefix {
		return errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}

	return nil
}

func longestCommonPrefix(a, b string) string {
	length := min(len(a), len(b))

	for i := 0; i < length; i++ {
		if a[i] != b[i] {
			return a[:i]
		}
	}
	return a[:length]
}
