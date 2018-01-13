// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chunkenc

import (
	"encoding/binary"
)

// UnorderedChunk accepts out-of-order sample data and compresses it only when the data is read.
type UnorderedChunk struct {
	bitCache *bstream
	samples  []sample
}

// NewUnorderedChunk returns a new chunk of the given size.
func NewUnorderedChunk() *UnorderedChunk {
	b := make([]byte, 2, 128)
	return &UnorderedChunk{
		bitCache: &bstream{stream: b, count: 0},
	}
}

// Encoding returns the encoding type.
func (c *UnorderedChunk) Encoding() Encoding {
	return EncXOR
}

// Bytes returns the underlying byte slice of the chunk.
func (c *UnorderedChunk) Bytes() []byte {
	// Return the cache contents if no samples have been added since
	if len(c.samples) == c.cachedSamples() {
		return c.bitCache.bytes()
	}

	a := &xorAppender{
		b: c.bitCache,
	}
	if binary.BigEndian.Uint16(a.b.bytes()) == 0 {
		a.leading = 0xff
	}

	for _, s := range c.samples {
		a.Append(s.t, s.v)
	}
	return c.bitCache.bytes()
}

// NumSamples returns the number of samples in the chunk.
func (c *UnorderedChunk) NumSamples() int {
	return len(c.samples)
}

func (c *UnorderedChunk) cachedSamples() int {
	return int(binary.BigEndian.Uint16(c.bitCache.bytes()))
}

// Appender implements the Chunk interface.
func (c *UnorderedChunk) Appender() (Appender, error) {
	return &unorderedAppender{chunk: c}, nil
}

func (c *UnorderedChunk) iterator() *unorderedIterator {
	return &unorderedIterator{
		// FIXME pass pointer to samples instead?
		chunk:    c,
		numTotal: uint16(c.NumSamples()),
	}
}

// Iterator implements the Chunk interface.
func (c *UnorderedChunk) Iterator() Iterator {
	return c.iterator()
}

type sample struct {
	t int64
	v float64
}

type unorderedAppender struct {
	chunk *UnorderedChunk
}

func (a *unorderedAppender) Append(t int64, v float64) {
	// FIXME: See if we can re-use the samples from head (see AddFast)
	a.chunk.samples = append(a.chunk.samples, sample{
		t: t,
		v: v,
	})
}

type unorderedIterator struct {
	chunk    *UnorderedChunk
	numTotal uint16
	numRead  uint16

	t int64
	v float64

	err error
}

func (it *unorderedIterator) At() (int64, float64) {
	return it.t, it.v
}

func (it *unorderedIterator) Err() error {
	return it.err
}

func (it *unorderedIterator) Next() bool {
	if it.err != nil || it.numRead == it.numTotal {
		return false
	}

	it.t = it.chunk.samples[it.numRead].t
	it.v = it.chunk.samples[it.numRead].v

	it.numRead++
	return true
}
