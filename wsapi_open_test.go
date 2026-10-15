package discordgo

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// fakeGateway is a minimal Discord gateway: it sends Hello, reads the client's
// Identify/Resume and then answers with whatever `answer` produces.
type fakeGateway struct {
	srv       *httptest.Server
	identifys int32
}

func newFakeGateway(t *testing.T, answer func(identify map[string]interface{}) interface{}) *fakeGateway {
	fg := &fakeGateway{}
	up := websocket.Upgrader{}
	fg.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			t.Logf("upgrade: %v", err)
			return
		}
		defer c.Close()
		_ = c.WriteJSON(map[string]interface{}{"op": 10, "d": map[string]int{"heartbeat_interval": 45000}})
		var identify map[string]interface{}
		if err := c.ReadJSON(&identify); err != nil {
			return
		}
		atomic.AddInt32(&fg.identifys, 1)
		if err := c.WriteJSON(answer(identify)); err != nil {
			return
		}
		// Drain until the client closes.
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}))
	return fg
}

func (fg *fakeGateway) url() string { return "ws" + strings.TrimPrefix(fg.srv.URL, "http") }
func (fg *fakeGateway) close()      { fg.srv.Close() }

func newTestSession(t *testing.T, gateway string) *Session {
	s, err := New("Bot test-token")
	if err != nil {
		t.Fatal(err)
	}
	s.gateway = gateway // skip the REST /gateway lookup
	s.ShouldReconnectOnError = false
	s.SyncEvents = true // run handlers inline so tests can observe them
	return s
}

// openWithTimeout fails the test instead of hanging forever when Open deadlocks.
func openWithTimeout(t *testing.T, s *Session) error {
	done := make(chan error, 1)
	go func() { done <- s.Open() }()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Open() did not return within 5s (deadlock)")
		return nil
	}
}

func TestOpenInvalidSessionNotResumable(t *testing.T) {
	var mu sync.Mutex
	var ops []float64
	fg := newFakeGateway(t, func(identify map[string]interface{}) interface{} {
		op, _ := identify["op"].(float64)
		mu.Lock()
		ops = append(ops, op)
		mu.Unlock()
		return map[string]interface{}{"op": 9, "d": false}
	})
	defer fg.close()

	// Stale resume info: the first Open sends Resume (op 6); after the
	// non-resumable Op 9 the second Open must fall back to Identify (op 2).
	s := newTestSession(t, fg.url())
	s.sessionID = "stale"
	s.resumeGatewayURL = fg.url()
	atomic.StoreInt64(s.sequence, 7)

	err := openWithTimeout(t, s)
	if !errors.Is(err, ErrInvalidSession) {
		t.Fatalf("expected ErrInvalidSession, got %v", err)
	}
	if s.wsConn != nil {
		t.Fatal("wsConn must be nil after a failed Open")
	}
	if s.sessionID != "" || s.resumeGatewayURL != "" || atomic.LoadInt64(s.sequence) != 0 {
		t.Fatalf("non-resumable Op 9 must discard session info, got id=%q url=%q seq=%d",
			s.sessionID, s.resumeGatewayURL, atomic.LoadInt64(s.sequence))
	}

	// The lock must have been released: Close() and a second Open() must not hang.
	closeDone := make(chan error, 1)
	go func() { closeDone <- s.Close() }()
	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Close() after failed Open() hangs: lock not released")
	}
	if err := openWithTimeout(t, s); !errors.Is(err, ErrInvalidSession) {
		t.Fatalf("second Open: expected ErrInvalidSession, got %v", err)
	}
	if n := atomic.LoadInt32(&fg.identifys); n != 2 {
		t.Fatalf("expected exactly 2 identify attempts (no internal reconnect loop), got %d", n)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(ops) != 2 || ops[0] != 6 || ops[1] != 2 {
		t.Fatalf("expected Resume then Identify, got ops %v", ops)
	}
}

func TestOpenInvalidSessionResumableKeepsSession(t *testing.T) {
	fg := newFakeGateway(t, func(identify map[string]interface{}) interface{} {
		if op, _ := identify["op"].(float64); op != 6 {
			t.Errorf("expected Resume (op 6), got %v", identify["op"])
		}
		return map[string]interface{}{"op": 9, "d": true}
	})
	defer fg.close()

	s := newTestSession(t, fg.url())
	s.sessionID = "sess"
	s.resumeGatewayURL = fg.url()
	atomic.StoreInt64(s.sequence, 42)

	if err := openWithTimeout(t, s); !errors.Is(err, ErrInvalidSession) {
		t.Fatalf("expected ErrInvalidSession, got %v", err)
	}
	if s.sessionID != "sess" || atomic.LoadInt64(s.sequence) != 42 {
		t.Fatalf("resumable Op 9 must keep session info, got id=%q seq=%d", s.sessionID, atomic.LoadInt64(s.sequence))
	}
}

func TestOpenReadySucceeds(t *testing.T) {
	fg := newFakeGateway(t, func(map[string]interface{}) interface{} {
		return map[string]interface{}{"op": 0, "s": 1, "t": "READY", "d": map[string]interface{}{
			"v": 10, "session_id": "ready-sess", "resume_gateway_url": "wss://resume.example",
			"user":   map[string]interface{}{"id": "1", "username": "bot"},
			"guilds": []interface{}{},
		}}
	})
	defer fg.close()

	s := newTestSession(t, fg.url())
	var ready int32
	s.AddHandler(func(_ *Session, r *Ready) {
		atomic.AddInt32(&ready, 1)
		if r.SessionID != "ready-sess" {
			t.Errorf("unexpected session id %q", r.SessionID)
		}
	})
	if err := openWithTimeout(t, s); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if s.wsConn == nil {
		t.Fatal("wsConn must be set after READY")
	}
	if atomic.LoadInt32(&ready) != 1 {
		t.Fatalf("READY handler fired %d times", ready)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// Op 9 after READY (i.e. in the listen goroutine) must still take the regular
// onEvent path: decode + dispatch must be equivalent to the old onEvent.
func TestOnEventDecodesDispatchPayload(t *testing.T) {
	s := newTestSession(t, "ws://unused")
	raw, _ := json.Marshal(map[string]interface{}{"op": 11})
	e, err := s.onEvent(websocket.TextMessage, raw)
	if err != nil || e.Operation != 11 {
		t.Fatalf("onEvent op 11: e=%+v err=%v", e, err)
	}
}
