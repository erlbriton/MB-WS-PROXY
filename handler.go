package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultDevicePort = 502
	dialTimeout       = 5 * time.Second
	tcpKeepAlive      = 30 * time.Second
	tcpBufSize        = 4096
	pongTimeout       = 60 * time.Second
	pingPeriod        = 30 * time.Second
)

type SessionHub struct {
	mu       sync.Mutex
	sessions map[*Session]struct{}
}

func NewSessionHub() *SessionHub {
	return &SessionHub{sessions: make(map[*Session]struct{})}
}

func (h *SessionHub) Add(s *Session) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sessions[s] = struct{}{}
}

func (h *SessionHub) Remove(s *Session) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.sessions, s)
}

func (h *SessionHub) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.sessions)
}

func (h *SessionHub) CloseAll() {
	h.mu.Lock()
	all := make([]*Session, 0, len(h.sessions))
	for s := range h.sessions {
		all = append(all, s)
	}
	h.mu.Unlock()
	for _, s := range all {
		s.Close()
	}
}

type controlMsg struct {
	Type    string `json:"type"`
	Message string `json:"message,omitempty"`
}

type Session struct {
	id   int64
	ws   *websocket.Conn
	tcp  net.Conn
	wsMu sync.Mutex

	closeOnce sync.Once
	done      chan struct{}
	errCh     chan error

	bytesW2T atomic.Int64
	bytesT2W atomic.Int64
}

func NewSession(id int64, ws *websocket.Conn) *Session {
	return &Session{
		id:    id,
		ws:    ws,
		done:  make(chan struct{}),
		errCh: make(chan error, 2),
	}
}

func (s *Session) Close() {
	s.closeOnce.Do(func() {
		close(s.done)
		if s.tcp != nil {
			_ = s.tcp.Close()
		}
		_ = s.ws.Close()
	})
}

func (s *Session) logf(format string, args ...any) {
	log.Printf("[sess %d] %s", s.id, fmt.Sprintf(format, args...))
}

func (s *Session) sendControl(m controlMsg) error {
	payload, err := json.Marshal(m)
	if err != nil {
		return err
	}
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	_ = s.ws.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return s.ws.WriteMessage(websocket.TextMessage, payload)
}

func (s *Session) sendBinary(payload []byte) error {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	_ = s.ws.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return s.ws.WriteMessage(websocket.BinaryMessage, payload)
}

type deviceAddr struct {
	host string
	port int
}

func (d deviceAddr) String() string {
	return net.JoinHostPort(d.host, strconv.Itoa(d.port))
}

func parseDeviceAddr(r *http.Request, ws *websocket.Conn) (deviceAddr, error) {
	q := r.URL.Query()
	host := q.Get("host")

	port := defaultDevicePort
	if p := q.Get("port"); p != "" {
		if v, err := strconv.Atoi(p); err == nil && v > 0 && v < 65536 {
			port = v
		} else {
			return deviceAddr{}, fmt.Errorf("некорректный порт %q", p)
		}
	}

	if host != "" {
		return deviceAddr{host: host, port: port}, nil
	}

	_ = ws.SetReadDeadline(time.Now().Add(10 * time.Second))
	msgType, data, err := ws.ReadMessage()
	if err != nil {
		return deviceAddr{}, fmt.Errorf("нет адреса устройства: %w", err)
	}
	if msgType != websocket.TextMessage {
		return deviceAddr{}, fmt.Errorf("первый фрейм должен быть текстовым JSON")
	}

	var hs struct {
		Host string `json:"host"`
		Port int    `json:"port"`
	}
	if err := json.Unmarshal(data, &hs); err != nil {
		return deviceAddr{}, fmt.Errorf("не удалось разобрать адрес: %w", err)
	}
	if hs.Host == "" {
		return deviceAddr{}, fmt.Errorf("пустой host в адресе устройства")
	}
	if hs.Port > 0 && hs.Port < 65536 {
		port = hs.Port
	}
	return deviceAddr{host: hs.Host, port: port}, nil
}

var sessionSeq atomic.Int64

func handleWebSocket(hub *SessionHub, upgrader websocket.Upgrader, w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("websocket upgrade отклонён: %v", err)
		return
	}

	id := sessionSeq.Add(1)
	remote := r.RemoteAddr
	configureConn(ws)

	dev, err := parseDeviceAddr(r, ws)
	if err != nil {
		log.Printf("[sess %d] отклонено %s: %v", id, remote, err)
		_ = ws.WriteMessage(websocket.TextMessage,
			[]byte(`{"type":"proxy_error","message":"`+err.Error()+`"}`))
		_ = ws.Close()
		return
	}

	dialer := net.Dialer{Timeout: dialTimeout, KeepAlive: tcpKeepAlive}
	tcp, err := dialer.DialContext(context.Background(), "tcp", dev.String())
	if err != nil {
		log.Printf("[sess %d] контроллер недоступен %s: %v", id, dev, err)
		_ = ws.WriteMessage(websocket.TextMessage,
			[]byte(`{"type":"proxy_error","message":"device unreachable: `+err.Error()+`"}`))
		_ = ws.Close()
		return
	}

	sess := NewSession(id, ws)
	sess.tcp = tcp
	hub.Add(sess)
	sess.logf("новое WebSocket-подключение от %s", remote)
	sess.logf("TCP-соединение установлено: %s -> %s", remote, dev)

	runSession(sess)

	hub.Remove(sess)
	sess.logf("сессия закрыта (передано: браузер->ПЛК %d Б, ПЛК->браузер %d Б)",
		sess.bytesW2T.Load(), sess.bytesT2W.Load())
}

func runSession(s *Session) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); pumpWS(s) }()
	go func() { defer wg.Done(); pumpTCP(s) }()
	go pinger(s)

	select {
	case err := <-s.errCh:
		s.logf("завершение сессии: %v", err)
		_ = notifyBrowser(s, "device link lost: "+err.Error())
	case <-s.done:
	}

	s.Close()
	wg.Wait()
}

func pumpWS(s *Session) {
	for {
		msgType, data, err := s.ws.ReadMessage()
		if err != nil {
			s.errCh <- fmt.Errorf("ws: %w", err)
			return
		}
		switch msgType {
		case websocket.BinaryMessage:
			if _, err := s.tcp.Write(data); err != nil {
				s.errCh <- fmt.Errorf("tcp write: %w", err)
				return
			}
			s.logf("W2T: %x", data)
			s.bytesW2T.Add(int64(len(data)))
		case websocket.TextMessage:
			s.logf("получен текстовый фрейм (%d Б), проигнорирован", len(data))
		default:
			s.errCh <- fmt.Errorf("неподдерживаемый тип фрейма: %d", msgType)
			return
		}
	}
}

func pumpTCP(s *Session) {
	buffer := make([]byte, tcpBufSize)
	for {
		n, err := s.tcp.Read(buffer)
		if n > 0 {
			s.logf("RAW_TCP_IN: %x", buffer[:n])
			if err := s.sendBinary(buffer[:n]); err != nil {
				s.errCh <- fmt.Errorf("ws write: %w", err)
				return
			}
			s.bytesT2W.Add(int64(n))
			s.logf("T2W: %x", buffer[:n])
		}
		if err != nil {
			s.errCh <- fmt.Errorf("tcp read: %w", err)
			return
		}
	}
}

func pinger(s *Session) {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.wsMu.Lock()
			_ = s.ws.SetWriteDeadline(time.Now().Add(5 * time.Second))
			err := s.ws.WriteMessage(websocket.PingMessage, nil)
			s.wsMu.Unlock()
			if err != nil {
				return
			}
		case <-s.done:
			return
		}
	}
}

func notifyBrowser(s *Session, message string) error {
	return s.sendControl(controlMsg{Type: "proxy_error", Message: message})
}

func configureConn(ws *websocket.Conn) {
	ws.SetPongHandler(func(string) error {
		_ = ws.SetReadDeadline(time.Now().Add(pongTimeout))
		return nil
	})
	_ = ws.SetReadDeadline(time.Now().Add(pongTimeout))
}
