/*
Copyright The Kubernetes Authors.

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

package proxy

import (
	"bytes"
	"context"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/streaming/pkg/httpstream"
	"k8s.io/streaming/pkg/httpstream/spdy"
)

const dialTestBearerToken = "dial-test-token"

type dialTestServerOptions struct {
	acceptWebsocket bool
	acceptSPDY      bool
	gateSPDY        bool
	requestCapacity int
}

type dialTestServer struct {
	server            *httptest.Server
	acceptWebsocket   bool
	acceptSPDY        bool
	gateSPDY          bool
	websocketRequests chan string
	spdyRequests      chan string
	requestErrors     chan error
	spdyGate          chan struct{}
	releaseSPDYOnce   sync.Once
}

func newDialTestServer(t *testing.T, options dialTestServerOptions) *dialTestServer {
	t.Helper()

	requestCapacity := options.requestCapacity
	if requestCapacity < 1 {
		requestCapacity = 1
	}
	testServer := &dialTestServer{
		acceptWebsocket:   options.acceptWebsocket,
		acceptSPDY:        options.acceptSPDY,
		gateSPDY:          options.gateSPDY,
		websocketRequests: make(chan string, requestCapacity),
		spdyRequests:      make(chan string, requestCapacity),
		requestErrors:     make(chan error, requestCapacity),
		spdyGate:          make(chan struct{}),
	}
	testServer.server = httptest.NewTLSServer(http.HandlerFunc(testServer.serveHTTP))
	t.Cleanup(func() {
		testServer.releaseSPDY()
		testServer.server.Close()
	})
	return testServer
}

func (s *dialTestServer) newDialer(t *testing.T) *Dialer {
	t.Helper()

	certificatePEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: s.server.Certificate().Raw,
	})
	dialer, err := NewDialer(Proxy{
		Kind:      "pods",
		Namespace: "default",
		KubeConfig: &rest.Config{
			Host:        s.server.URL,
			BearerToken: dialTestBearerToken,
			TLSClientConfig: rest.TLSClientConfig{
				CAData: certificatePEM,
			},
		},
		Port: 2379,
	})
	if err != nil {
		t.Fatalf("creating dialer: %v", err)
	}
	return dialer
}

func (s *dialTestServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	endpoint := path.Base(strings.TrimSuffix(r.URL.Path, "/portforward"))
	if r.TLS == nil {
		s.requestErrors <- fmt.Errorf("request for %s did not use TLS", endpoint)
	}
	if got := r.Header.Get("Authorization"); got != "Bearer "+dialTestBearerToken {
		s.requestErrors <- fmt.Errorf("request for %s has authorization %q", endpoint, got)
	}

	switch {
	case strings.EqualFold(r.Header.Get(httpstream.HeaderUpgrade), "websocket"):
		s.websocketRequests <- endpoint
		if !s.acceptWebsocket {
			http.Error(w, "websocket forwarding is disabled", http.StatusForbidden)
			return
		}
		s.serveWebsocket(w, r, endpoint)
	case strings.EqualFold(r.Header.Get(httpstream.HeaderUpgrade), spdy.HeaderSpdy31):
		s.spdyRequests <- endpoint
		if s.gateSPDY {
			select {
			case <-s.spdyGate:
			case <-r.Context().Done():
				return
			}
		}
		if !s.acceptSPDY {
			http.Error(w, "SPDY forwarding is disabled", http.StatusForbidden)
			return
		}
		s.serveSPDY(w, r, endpoint)
	default:
		s.requestErrors <- fmt.Errorf("request for %s has unexpected upgrade header %q", endpoint, r.Header.Get(httpstream.HeaderUpgrade))
		http.Error(w, "unsupported upgrade", http.StatusBadRequest)
	}
}

func (s *dialTestServer) serveSPDY(w http.ResponseWriter, r *http.Request, endpoint string) {
	connection := spdy.NewResponseUpgrader().UpgradeResponse(w, r, s.streamHandler(endpoint))
	if connection == nil {
		return
	}
	defer connection.Close()
	<-connection.CloseChan()
}

func (s *dialTestServer) serveWebsocket(w http.ResponseWriter, r *http.Request, endpoint string) {
	protocols := strings.Split(r.Header.Get("Sec-WebSocket-Protocol"), ",")
	for i := range protocols {
		protocols[i] = strings.TrimSpace(protocols[i])
	}
	websocketConnection, err := (&websocket.Upgrader{
		Subprotocols: protocols,
		CheckOrigin:  func(*http.Request) bool { return true },
	}).Upgrade(w, r, nil)
	if err != nil {
		s.requestErrors <- fmt.Errorf("upgrading websocket for %s: %w", endpoint, err)
		return
	}
	defer websocketConnection.Close()

	connection, err := spdy.NewServerConnection(
		portforward.NewTunnelingConnection("test-server", websocketConnection),
		s.streamHandler(endpoint),
	)
	if err != nil {
		s.requestErrors <- fmt.Errorf("creating SPDY connection for websocket %s: %w", endpoint, err)
		return
	}
	defer connection.Close()
	<-connection.CloseChan()
}

func (s *dialTestServer) streamHandler(endpoint string) httpstream.NewStreamHandler {
	return func(stream httpstream.Stream, replySent <-chan struct{}) error {
		if stream.Headers().Get(corev1.StreamType) != corev1.StreamTypeData {
			go func() {
				<-replySent
				_ = stream.Close()
			}()
			return nil
		}

		go func() {
			defer stream.Close()
			<-replySent
			if _, err := io.WriteString(stream, endpoint+":"); err != nil {
				return
			}
			_, _ = io.Copy(stream, stream)
		}()
		return nil
	}
}

func (s *dialTestServer) releaseSPDY() {
	s.releaseSPDYOnce.Do(func() {
		close(s.spdyGate)
	})
}

func TestDialContextWebsocketSuccess(t *testing.T) {
	g := NewWithT(t)
	testServer := newDialTestServer(t, dialTestServerOptions{
		acceptWebsocket: true,
		requestCapacity: 2,
	})
	dialer := testServer.newDialer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := dialer.DialContext(ctx, scheme, "pod-websocket")
	if err != nil {
		t.Fatalf("dialing over websocket: %v", err)
	}
	defer conn.Close()

	g.Expect(exchangeDialTestPayload(conn, "pod-websocket")).To(Succeed())
	g.Expect(receiveDialTestRequest(t, testServer.websocketRequests)).To(Equal("pod-websocket"))
	g.Expect(noDialTestRequest(testServer.spdyRequests)).To(BeTrue())
	g.Expect(noDialTestRequest(testServer.requestErrors)).To(BeTrue())
}

func TestDialContextConcurrentSPDYFallback(t *testing.T) {
	g := NewWithT(t)
	const concurrency = 8

	testServer := newDialTestServer(t, dialTestServerOptions{
		acceptSPDY:      true,
		gateSPDY:        true,
		requestCapacity: concurrency * 2,
	})
	dialer := testServer.newDialer(t)

	type dialResult struct {
		err error
	}
	results := make(chan dialResult, concurrency)
	endpoints := make([]string, concurrency)
	for i := range concurrency {
		endpoint := fmt.Sprintf("pod-%02d", i)
		endpoints[i] = endpoint
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			conn, err := dialer.DialContext(ctx, scheme, endpoint)
			if err == nil {
				err = exchangeDialTestPayload(conn, endpoint)
				closeErr := conn.Close()
				if err == nil {
					err = closeErr
				}
			}
			results <- dialResult{err: err}
		}()
	}

	spdyEndpoints := make(map[string]int, concurrency)
	for range concurrency {
		spdyEndpoints[receiveDialTestRequest(t, testServer.spdyRequests)]++
	}
	testServer.releaseSPDY()

	for range concurrency {
		select {
		case result := <-results:
			g.Expect(result.err).To(Succeed())
		case <-time.After(10 * time.Second):
			t.Fatal("concurrent dials did not complete")
		}
	}

	websocketEndpoints := make(map[string]int, concurrency)
	for range concurrency {
		websocketEndpoints[receiveDialTestRequest(t, testServer.websocketRequests)]++
	}
	for _, endpoint := range endpoints {
		g.Expect(spdyEndpoints[endpoint]).To(Equal(1))
		g.Expect(websocketEndpoints[endpoint]).To(Equal(1))
	}
	g.Expect(noDialTestRequest(testServer.spdyRequests)).To(BeTrue())
	g.Expect(noDialTestRequest(testServer.requestErrors)).To(BeTrue())
}

func TestDialContextSPDYFallbackError(t *testing.T) {
	g := NewWithT(t)
	testServer := newDialTestServer(t, dialTestServerOptions{
		requestCapacity: 4,
	})
	dialer := testServer.newDialer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := dialer.DialContext(ctx, scheme, "pod-error")
	if err == nil {
		t.Fatal("expected port-forwarding error")
	}
	if !strings.Contains(err.Error(), "SPDY forwarding is disabled") {
		t.Fatalf("error %q does not include the SPDY rejection", err)
	}
	if conn != nil {
		t.Fatal("dial returned a connection after a failed upgrade")
	}
	g.Expect(receiveDialTestRequest(t, testServer.websocketRequests)).To(Equal("pod-error"))
	g.Expect(receiveDialTestRequest(t, testServer.spdyRequests)).To(Equal("pod-error"))
	g.Expect(noDialTestRequest(testServer.requestErrors)).To(BeTrue())
}

func exchangeDialTestPayload(conn net.Conn, endpoint string) error {
	payload := []byte("payload-" + endpoint)
	if _, err := conn.Write(payload); err != nil {
		return fmt.Errorf("writing payload for %s: %w", endpoint, err)
	}

	expected := append([]byte(endpoint+":"), payload...)
	got := make([]byte, len(expected))
	if _, err := io.ReadFull(conn, got); err != nil {
		return fmt.Errorf("reading payload for %s: %w", endpoint, err)
	}
	if !bytes.Equal(got, expected) {
		return fmt.Errorf("endpoint %s received %q, want %q", endpoint, got, expected)
	}
	return nil
}

func receiveDialTestRequest(t *testing.T, requests <-chan string) string {
	t.Helper()

	select {
	case endpoint := <-requests:
		return endpoint
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for port-forward request")
		return ""
	}
}

func noDialTestRequest[T any](requests <-chan T) bool {
	select {
	case <-requests:
		return false
	default:
		return true
	}
}
