package aibot

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// dialPushConn spins up a WebSocket server that pushes the given frames to the
// client and then holds the connection open, and returns the client connection.
func dialPushConn(t *testing.T, frames ...string) *websocket.Conn {
	t.Helper()
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		for _, frame := range frames {
			if err := c.WriteMessage(websocket.TextMessage, []byte(frame)); err != nil {
				return
			}
		}
		// Keep the connection open so the client read loop is not woken by EOF.
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)

	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial test server: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// TestReadLoopSurvivesDisconnectedEvent covers the read loop's disconnected_event
// path. Handling that event closes the connection and clears the manager's conn
// reference from inside the read goroutine itself, so a loop that re-reads the
// shared field on every iteration dereferences nil and panics with
// "invalid memory address or nil pointer dereference" in gorilla NextReader.
//
// The panic happens on a spawned goroutine and takes the process down, so
// without the fix this test cannot fail gracefully — it crashes the run.
func TestReadLoopSurvivesDisconnectedEvent(t *testing.T) {
	m := NewWsConnectionManager(
		NewLoggerFunc(func(string, string, ...interface{}) {}),
		0, 0, 0, "", nil, 0, 0,
	)

	disconnected := make(chan string, 1)
	m.OnServerDisconnect = func(reason string) { disconnected <- reason }

	conn := dialPushConn(t, `{"cmd":"aibot_event_callback","headers":{"req_id":"r1"},"body":{"event":{"eventtype":"disconnected_event"}}}`)
	m.setConn(conn)
	m.setupEventHandlers(conn)

	select {
	case <-disconnected:
	case <-time.After(3 * time.Second):
		t.Fatal("OnServerDisconnect was not called")
	}

	// The event has been dispatched; give the read goroutine time to run one
	// more loop iteration, which is where the nil dereference happens.
	time.Sleep(200 * time.Millisecond)

	if m.IsConnected() {
		t.Error("manager still reports connected after disconnected_event")
	}
}
