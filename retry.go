// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

// These functions are based on: https://github.com/googleapis/google-cloud-go/blob/6a9c12a395245d8500c267437c2dfa897049a719/bigquery/storage/managedwriter/retry.go
import (
	"errors"
	"io"
	"math/rand"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// This retry predicate is used for higher level retries, enqueing appends onto to a bidi
// Channel and evaluating whether an append should be retried (re-enqueued).
func retryPredicate(err error) (shouldRetry bool) {
	if err == nil {
		return
	}

	s, ok := status.FromError(err)
	// Non-status based error conditions.
	if !ok {
		// EOF can happen in the case of connection close.
		if errors.Is(err, io.EOF) {
			shouldRetry = true
			return
		}
		// All other non-status errors are treated as non-retryable (including context errors).
		return
	}
	switch s.Code() {
	case codes.Aborted,
		codes.Canceled,
		codes.DeadlineExceeded,
		codes.FailedPrecondition,
		codes.Internal,
		codes.Unavailable:
		shouldRetry = true
		return
	case codes.ResourceExhausted:
		if strings.HasPrefix(s.Message(), "Exceeds 'AppendRows throughput' quota") {
			// Note: internal b/246031522 opened to give this a structured error
			// And avoid string parsing.  Should be a QuotaFailure or similar.
			shouldRetry = true
			return
		}
	}
	return
}

// StatelessRetryer is used for backing off within a continuous process, like processing the responses
// From the receive side of the bidi stream.  An individual item in that process has a notion of an attempt
// Count, and we use maximum retries as a way of evicting bad items.
type statelessRetryer struct {
	mu sync.Mutex // guards r
	r  *rand.Rand

	minBackoff  time.Duration
	jitter      time.Duration
	maxAttempts int
}

func newStatelessRetryer(numRetryAttempts int) *statelessRetryer {
	return &statelessRetryer{
		r:           rand.New(rand.NewSource(time.Now().UnixNano())),
		minBackoff:  50 * time.Millisecond,
		jitter:      time.Second,
		maxAttempts: numRetryAttempts,
	}
}

func (sr *statelessRetryer) pause() time.Duration {
	jitter := sr.jitter.Nanoseconds()
	if jitter > 0 {
		sr.mu.Lock()
		jitter = sr.r.Int63n(jitter)
		sr.mu.Unlock()
	}
	pause := sr.minBackoff.Nanoseconds() + jitter

	return time.Duration(pause)
}

func (sr *statelessRetryer) Retry(err error, attemptCount int) (time.Duration, bool) {
	if attemptCount >= sr.maxAttempts {
		return 0, false
	}
	shouldRetry := retryPredicate(err)
	if shouldRetry {
		return sr.pause(), true
	}
	return 0, false
}
