package sstable

// Iterator walks all entries of an SST in internal-key order.
// Key and Value alias the current block's decoded buffer and stay valid
// until the next positioning call; copy before retaining.
type Iterator struct {
	r       *Reader
	blockIt *blockIterator
	blocki  int // current block index, -1 = before start
	err     error
}

// NewIterator returns an iterator positioned before the first entry.
func (r *Reader) NewIterator() *Iterator {
	return &Iterator{r: r, blocki: -1}
}

// First positions at the smallest entry.
func (it *Iterator) First() {
	it.err = nil
	if err := it.loadBlock(0); err != nil {
		return
	}
	it.blockIt.First()
	it.sync()
}

// SeekGE positions at the first entry with internal key >= target.
func (it *Iterator) SeekGE(target []byte) {
	it.err = nil
	i := sortSearchIndex(it.r.indexKeys, target)
	if i >= len(it.r.indexKeys) {
		it.blocki = -2 // exhausted
		it.blockIt = nil
		return
	}
	if err := it.loadBlock(i); err != nil {
		return
	}
	it.blockIt.SeekGE(target)
	it.sync()
}

// loadBlock decodes block i into the current position.
func (it *Iterator) loadBlock(i int) error {
	blk, err := it.r.readBlock(i)
	if err != nil {
		it.err = err
		it.blocki = -2
		it.blockIt = nil
		return err
	}
	it.blocki = i
	it.blockIt = blk.newIterator()
	return nil
}

// sync normalizes position: skip exhausted blocks forward.
func (it *Iterator) sync() {
	if it.blockIt == nil {
		return
	}
	if it.blockIt.Err() != nil {
		it.err = it.blockIt.Err()
		it.blocki = -2
		it.blockIt = nil
		return
	}
	for !it.blockIt.Valid() && it.blocki+1 < len(it.r.indexKeys) {
		if err := it.loadBlock(it.blocki + 1); err != nil {
			return
		}
		it.blockIt.First()
		if it.blockIt.Err() != nil {
			it.err = it.blockIt.Err()
			it.blocki = -2
			it.blockIt = nil
			return
		}
	}
	if !it.blockIt.Valid() {
		it.blocki = -2
		it.blockIt = nil
	}
}

// Next advances; Valid must be checked afterwards.
func (it *Iterator) Next() {
	if it.blocki == -2 {
		return
	}
	if it.blockIt == nil {
		it.First()
		return
	}
	it.blockIt.Next()
	it.sync()
}

// Valid reports whether the iterator is positioned.
func (it *Iterator) Valid() bool {
	return it.blocki >= 0 && it.blockIt != nil && it.blockIt.Valid() && it.err == nil
}

// Key returns the current internal key (aliased).
func (it *Iterator) Key() []byte { return it.blockIt.Key() }

// Value returns the current value (aliased).
func (it *Iterator) Value() []byte { return it.blockIt.Value() }

// Err returns any corruption encountered.
func (it *Iterator) Err() error { return it.err }

// Close releases the iterator's resources. Blocks are owned by the reader,
// so this is bookkeeping only; it exists so Iterators compose with
// merge.Iter and satisfies the interface contract.
func (it *Iterator) Close() error {
	it.blockIt = nil
	it.blocki = -2
	it.err = nil
	return nil
}

// Compile-time check: an SST Iterator is a merge.Iter source (M-008 item 7).
var _ interface {
	First()
	SeekGE(ikey []byte)
	Next()
	Valid() bool
	Key() []byte
	Value() []byte
	Err() error
	Close() error
} = (*Iterator)(nil)
