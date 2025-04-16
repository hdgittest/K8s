/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package apitesting

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

// errReadOnClosedResBody is returned by methods in the "http" package, when
// reading from a response body after it's been closed.
// Detecting this error is required because read is not cancellable.
// From https://github.com/golang/go/blob/go1.20/src/net/http/transport.go#L2779
var errReadOnClosedResBody = errors.New("http: read on closed response body")

// AssertResponseBodyClosed fails the test if the response Body is NOT closed.
// If not already closed, the response body will be drained and closed.
// Defer when your test is expected to close the response body before ending.
func AssertResponseBodyClosed(t *testing.T, body io.ReadCloser) {
	assert.Equal(t, errReadOnClosedResBody, DrainAndCloseResponseBody(body))
}

// DrainAndCloseResponseBody reads from the response body until EOF, discarding the
// content, and closes the response body when finished or on error.
// Returns an error when either Read or Close error. If both error, the errors
// are joined and returned.
//
// In a defer from a test, use with t.Error or assert.NoError, NOT t.Fatal or
// require.NoError, unless the defer also captures panics, otherwise the test
// may not fail.
func DrainAndCloseResponseBody(body io.ReadCloser) error {
	errCh := make(chan error)
	go func() {
		// Close after done reading
		defer func() {
			defer close(errCh)
			if err := body.Close(); err != nil {
				errCh <- err
			}
		}()
		// Read until EOF and discard
		if _, err := io.Copy(io.Discard, body); err != nil {
			errCh <- err
		}
	}()

	// Wait until Read and Close are both done.
	// Combine errors, if multiple.
	var multiErr error
	for err := range errCh {
		if multiErr != nil {
			multiErr = errors.Join(multiErr, err)
		} else {
			multiErr = err
		}
	}
	return multiErr
}

// ReadAllAndCloseResponseBody reads from the response body until EOF and then
// closing the body, returning the content and any errors.
// Returns an error when either Read or Close error. If both error, the errors
// are joined and returned.
func ReadAllAndCloseResponseBody(body io.ReadCloser) ([]byte, error) {
	errCh := make(chan error)
	bodyCh := make(chan []byte)
	go func() {
		// Close after done reading
		defer func() {
			defer close(errCh)
			if err := body.Close(); err != nil {
				errCh <- err
			}
		}()
		defer close(bodyCh)
		// Read until EOF and discard
		bodyBytes, err := io.ReadAll(body)
		if err != nil {
			errCh <- err
		}
		bodyCh <- bodyBytes
	}()

	// Wait until Read and Close are both done.
	// Combine errors, if multiple.
	var bodyBytes []byte
	var multiErr error
	var errClosed, bodyClosed bool
	for {
		select {
		case err, ok := <-errCh:
			if !ok {
				if bodyClosed {
					return bodyBytes, multiErr
				}
				errClosed = true
				continue
			}
			if multiErr != nil {
				multiErr = errors.Join(multiErr, err)
			} else {
				multiErr = err
			}
		case b, ok := <-bodyCh:
			if !ok {
				if errClosed {
					return bodyBytes, multiErr
				}
				bodyClosed = true
				continue
			}
			bodyBytes = b
		}
	}
}
