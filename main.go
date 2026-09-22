package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

func buildMux(sessions *SessionHub) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/proxy", func(w http.ResponseWriter, r *http.Request) {
		handleWebSocket(sessions, upgrader, w, r)
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintf(w, "MB-WS-PROXY is running. Active sessions: %d\n", sessions.Count())
	})

	return mux
}

func main() {
	port := flag.Int("port", 8080, "порт WebSocket-слушателя")
	bind := flag.String("bind", "", "адрес привязки (по умолчанию — все интерфейсы)")
	flag.Parse()

	sessions := NewSessionHub()
	addr := net.JoinHostPort(*bind, strconv.Itoa(*port))

	srv := &http.Server{
		Addr:         addr,
		Handler:      buildMux(sessions),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  2 * time.Minute,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		log.Printf("старт: ws://%s/proxy  (health: %s/health)", addr, addr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		log.Fatalf("слушатель завершён с ошибкой: %v", err)
	case <-ctx.Done():
		log.Println("получен сигнал остановки, завершаюсь...")
		stop()
	}

	sessions.CloseAll()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("мягкое завершение слушателя: %v", err)
	}

	log.Println("остановлен")
}
