package stream

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
	Name  string
	Value *Node
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

func (s *Stream) Add(id string, entries map[string]interface{}) {
	current := s.Value
	for {
		if current == nil {
			s.Value = &Node{
				Prefix:   id,
				Entries:  append(current.Entries, &Entry{Id: id, Elements: entries}),
				Children: make(map[byte]*Node),
			}
			return
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
			return
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
			return
		}

		current = child
	}
}

func (s *Stream) Get(id string) *Entry {
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

		commonPrefix := longestCommonPrefix(id, current.Prefix)
		id = id[len(commonPrefix):]

		if len(id) == 0 {
			for _, entry := range current.Entries {
				if entry.Id == id {
					return entry
				}
			}
			return nil
		}

		child, exists := current.Children[id[0]]
		if !exists {
			return nil
		}

		current = child
	}
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
