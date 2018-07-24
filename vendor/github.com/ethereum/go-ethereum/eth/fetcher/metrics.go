// Copyright 2015 The go-ethereum Authors
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

// Contains the metrics collected by the fetcher.

package fetcher

import (
	"github.com/ethereum/go-ethereum/metrics"
)

var (
	PropAnnounceInMeter   = metrics.NewRegisteredMeter("eth/fetcher/prop/Announces/in", nil)
	PropAnnounceOutTimer  = metrics.NewRegisteredTimer("eth/fetcher/prop/Announces/out", nil)
	PropAnnounceDropMeter = metrics.NewRegisteredMeter("eth/fetcher/prop/Announces/drop", nil)
	PropAnnounceDOSMeter  = metrics.NewRegisteredMeter("eth/fetcher/prop/Announces/dos", nil)

	PropBroadcastInMeter   = metrics.NewRegisteredMeter("eth/fetcher/prop/broadcasts/in", nil)
	PropBroadcastOutTimer  = metrics.NewRegisteredTimer("eth/fetcher/prop/broadcasts/out", nil)
	PropBroadcastDropMeter = metrics.NewRegisteredMeter("eth/fetcher/prop/broadcasts/drop", nil)
	PropBroadcastDOSMeter  = metrics.NewRegisteredMeter("eth/fetcher/prop/broadcasts/dos", nil)

	HeaderFetchMeter = metrics.NewRegisteredMeter("eth/fetcher/fetch/Headers", nil)
	BodyFetchMeter   = metrics.NewRegisteredMeter("eth/fetcher/fetch/bodies", nil)

	HeaderFilterInMeter  = metrics.NewRegisteredMeter("eth/fetcher/filter/Headers/in", nil)
	HeaderFilterOutMeter = metrics.NewRegisteredMeter("eth/fetcher/filter/Headers/out", nil)
	BodyFilterInMeter    = metrics.NewRegisteredMeter("eth/fetcher/filter/bodies/in", nil)
	BodyFilterOutMeter   = metrics.NewRegisteredMeter("eth/fetcher/filter/bodies/out", nil)
)
