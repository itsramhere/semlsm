package base

// GroupOf returns the group prefix of key per B4: find the depth-th occurrence
// of delim; if found, return key[:idx+1] (including the delimiter), otherwise
// return the whole key. All keys sharing such a prefix are contiguous in byte
// order, so groups are contiguous in a sorted scan.
//
// depth <= 0 disables grouping: the whole key is the group.
// The result aliases key; copy before retaining.
func GroupOf(key []byte, depth int, delim byte) []byte {
	if depth <= 0 {
		return key
	}
	seen := 0
	for i, c := range key {
		if c != delim {
			continue
		}
		seen++
		if seen == depth {
			return key[:i+1]
		}
	}
	return key
}
