package discordgo

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// silentGateway accepts the websocket and then goes quiet at the requested
// stage: either never sends Hello, or sends Hello, reads the Identify and never
// answers it. It mimics a half-dead connection (proxy/TUN keeps the socket
// open, the peer never speaks again).
type silentGateway struct {
	srv       *httptest.Server
	identifys int32
}

func newSilentGateway(t *testing.T, sendHello bool) *silentGateway {
	sg := &silentGateway{}
	up := websocket.Upgrader{}
	sg.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			t.Logf("upgrade: %v", err)
			return
		}
		defer c.Close()
		if sendHello {
			_ = c.WriteJSON(map[string]interface{}{"op": 10, "d": map[string]int{"heartbeat_interval": 45000}})
			var identify map[string]interface{}
			if err := c.ReadJSON(&identify); err != nil {
				return
			}
			atomic.AddInt32(&sg.identifys, 1)
		}
		// Say nothing more; drain until the client gives up and closes.
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}))
	return sg
}

func (sg *silentGateway) url() string { return "ws" + sg.srv.URL[len("http"):] }
func (sg *silentGateway) close()      { sg.srv.Close() }

// withOpenReadTimeout shrinks OpenReadTimeout for the test and restores it.
func withOpenReadTimeout(t *testing.T, d time.Duration) {
	old := OpenReadTimeout
	OpenReadTimeout = d
	t.Cleanup(func() { OpenReadTimeout = old })
}

// assertSessionUsable checks the lock was released by a failed Open: Close()
// must return promptly and the Session must be reusable.
func assertSessionUsable(t *testing.T, s *Session) {
	t.Helper()
	closeDone := make(chan error, 1)
	go func() { closeDone <- s.Close() }()
	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Close() after failed Open() hangs: lock not released")
	}
}

func TestOpenTimesOutWaitingForHello(t *testing.T) {
	withOpenReadTimeout(t, 200*time.Millisecond)
	sg := newSilentGateway(t, false)
	defer sg.close()

	s := newTestSession(t, sg.url())
	start := time.Now()
	err := openWithTimeout(t, s)
	if !errors.Is(err, ErrOpenTimeout) {
		t.Fatalf("expected ErrOpenTimeout, got %v", err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("Open took %v, expected to give up around OpenReadTimeout", took)
	}
	if s.wsConn != nil {
		t.Fatal("wsConn must be nil after a timed-out Open")
	}
	assertSessionUsable(t, s)
}

func TestOpenTimesOutWaitingForReady(t *testing.T) {
	withOpenReadTimeout(t, 200*time.Millisecond)
	sg := newSilentGateway(t, true)
	defer sg.close()

	s := newTestSession(t, sg.url())
	var disconnects int32
	s.AddHandler(func(*Session, *Disconnect) { atomic.AddInt32(&disconnects, 1) })

	err := openWithTimeout(t, s)
	if !errors.Is(err, ErrOpenTimeout) {
		t.Fatalf("expected ErrOpenTimeout, got %v", err)
	}
	if n := atomic.LoadInt32(&sg.identifys); n != 1 {
		t.Fatalf("expected exactly 1 identify, got %d", n)
	}
	if s.wsConn != nil {
		t.Fatal("wsConn must be nil after a timed-out Open")
	}
	if atomic.LoadInt32(&disconnects) != 0 {
		t.Fatal("a failed Open must not emit Disconnect (no reconnect loop was ever started)")
	}
	assertSessionUsable(t, s)

	// A fresh gateway that does answer must now succeed on the same Session.
	fg := newFakeGateway(t, func(map[string]interface{}) interface{} {
		return map[string]interface{}{"op": 0, "s": 1, "t": "READY", "d": map[string]interface{}{
			"v": 10, "session_id": "after-timeout", "resume_gateway_url": "wss://resume.example",
			"user":   map[string]interface{}{"id": "1", "username": "bot"},
			"guilds": []interface{}{},
		}}
	})
	defer fg.close()
	s.gateway = fg.url()
	if err := openWithTimeout(t, s); err != nil {
		t.Fatalf("Open after timeout: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// Once READY arrived the connection belongs to listen(), which must be able to
// sit idle far longer than OpenReadTimeout: the deadline must have been cleared.
func TestOpenClearsReadDeadlineForListen(t *testing.T) {
	withOpenReadTimeout(t, 100*time.Millisecond)
	fg := newFakeGateway(t, func(map[string]interface{}) interface{} {
		return map[string]interface{}{"op": 0, "s": 1, "t": "READY", "d": map[string]interface{}{
			"v": 10, "session_id": "idle-sess", "resume_gateway_url": "wss://resume.example",
			"user":   map[string]interface{}{"id": "1", "username": "bot"},
			"guilds": []interface{}{},
		}}
	})
	defer fg.close()

	s := newTestSession(t, fg.url())
	var disconnects int32
	s.AddHandler(func(*Session, *Disconnect) { atomic.AddInt32(&disconnects, 1) })
	if err := openWithTimeout(t, s); err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Idle for several OpenReadTimeouts; a leftover deadline would make listen
	// see a read error, Close the session and emit Disconnect.
	time.Sleep(500 * time.Millisecond)

	s.RLock()
	open := s.wsConn != nil
	s.RUnlock()
	if !open {
		t.Fatal("connection was closed while idle: Open read deadline leaked into listen()")
	}
	if atomic.LoadInt32(&disconnects) != 0 {
		t.Fatal("Disconnect fired while idle: Open read deadline leaked into listen()")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}
