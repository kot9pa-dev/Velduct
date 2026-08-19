// Package liveness detects a dead peer on a WebSocket connection when the
// transport itself stays silent — half-open links such as a frozen middlebox, a
// dropped UDP/WireGuard tunnel or an LB idle-eviction, where OS/TCP keepalive
// never fires (the kernel keeps ACKing, so the socket looks alive forever).
//
// It uses independent, one-way app-level heartbeats (not ping/pong request-
// reply): each side sends its own heartbeat on its own timer, and treats the
// peer's heartbeat — recorded via Mark() on any inbound frame — as proof the
// peer is alive. If no inbound frame arrives within idleTimeout, the connection
// is closed so the blocked reader unblocks and the caller reconnects. Liveness
// is decoupled from the transfer path — the only cost there is the Mark() call.
package liveness

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// Monitor watches one connection for peer liveness.
type Monitor struct {
	conn        *websocket.Conn
	idleTimeout time.Duration
	interval    time.Duration
	heartbeat   func() error

	lastRx   atomic.Int64 // UnixNano of last inbound activity
	stop     chan struct{}
	done     chan struct{} // closed when Run returns
	stopOnce sync.Once
}

// New wires a Monitor. heartbeat is sent every interval to tell the peer this
// side is alive (one-way, no reply expected); callers must call Mark() for every
// inbound frame they read. interval must be < idleTimeout (validate upstream).
func New(conn *websocket.Conn, idleTimeout, interval time.Duration, heartbeat func() error) *Monitor {
	m := &Monitor{
		conn:        conn,
		idleTimeout: idleTimeout,
		interval:    interval,
		heartbeat:   heartbeat,
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
	}
	m.Mark()
	return m
}

// Mark records inbound network activity. Lock-free; safe on the hot read path.
func (m *Monitor) Mark() { m.lastRx.Store(time.Now().UnixNano()) }

// Run sends heartbeats and checks for peer death until Stop or a liveness
// timeout. On timeout it closes conn, unblocking the reader so the caller
// reconnects. Blocks; run it in its own goroutine.
func (m *Monitor) Run() {
	defer close(m.done)
	if m.idleTimeout <= 0 || m.interval <= 0 {
		return // liveness disabled
	}
	t := time.NewTicker(m.interval)
	defer t.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-t.C:
			if time.Since(time.Unix(0, m.lastRx.Load())) >= m.idleTimeout {
				slog.Warn("[Liveness] no inbound traffic — closing dead connection",
					"timeout", m.idleTimeout)
				_ = m.conn.Close() // unblocks ReadMessage -> caller reconnects
				return
			}
			if m.heartbeat != nil {
				_ = m.heartbeat() // one-way: tell the peer we're alive
			}
		}
	}
}

// Stop signals the Monitor and blocks until Run has fully exited, so the caller
// owns the goroutine's lifetime (no overlap across reconnects). Idempotent.
// Must be called from a different goroutine than Run.
func (m *Monitor) Stop() {
	m.stopOnce.Do(func() { close(m.stop) })
	<-m.done
}
