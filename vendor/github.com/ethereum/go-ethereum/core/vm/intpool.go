// Copyright 2017 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package vm

import "math/big"

var checkVal = big.NewInt(-42)

const poolLimit = 256

// IntPool is a Pool of big integers that
// can be reused for all big.Int operations.
type IntPool struct {
	Pool *Stack
}

func NewIntPool() *IntPool {
	return &IntPool{Pool: Newstack()}
}

// Get retrieves a big int from the Pool, allocating one if the Pool is empty.
// Note, the returned int's Value is arbitrary and will not be zeroed!
func (p *IntPool) Get() *big.Int {
	if p.Pool.Len() > 0 {
		return p.Pool.Pop()
	}
	return new(big.Int)
}

// GetZero retrieves a big int from the Pool, setting it to zero or allocating
// a new one if the Pool is empty.
func (p *IntPool) GetZero() *big.Int {
	if p.Pool.Len() > 0 {
		return p.Pool.Pop().SetUint64(0)
	}
	return new(big.Int)
}

// Put Returns an allocated big int to the Pool to be later reused by Get calls.
// Note, the values as saved as is; neither Put nor Get zeroes the ints out!
func (p *IntPool) Put(is ...*big.Int) {
	if len(p.Pool.data) > poolLimit {
		return
	}
	for _, i := range is {
		// VerifyPool is a build flag. Pool verification makes sure the integrity
		// of the integer Pool by comparing values to a default Value.
		if VerifyPool {
			i.Set(checkVal)
		}
		p.Pool.Push(i)
	}
}
