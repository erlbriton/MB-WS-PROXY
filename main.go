package main

import (
	"context"
	_ "embed"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"fyne.io/systray"
	"github.com/gorilla/websocket"
)

//go:embed icon.ico
var iconPNG []byte

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

func createTrayIcon() []byte {
	return iconPNG
}
func main() {
	port := flag.Int("port", 8080, "порт WebSocket-слушателя")
	bind := flag.String("bind", "127.0.0.1", "адрес привязки")
	flag.Parse()

	execPath, _ := os.Executable()
	logPath := filepath.Join(filepath.Dir(execPath), "mb-ws-proxy.log")
	logFile, _ := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	defer logFile.Close()

	// Направляем вывод логов одновременно в консоль (os.Stdout) и в файл логов
	log.SetOutput(io.MultiWriter(os.Stdout, logFile))

	sessions := NewSessionHub()
	addr := net.JoinHostPort(*bind, strconv.Itoa(*port))

	// Функция для создания нового сервера
	createServer := func() *http.Server {
		return &http.Server{
			Addr:         addr,
			Handler:      buildMux(sessions),
			ReadTimeout:  15 * time.Second,
			WriteTimeout: 15 * time.Second,
			IdleTimeout:  2 * time.Minute,
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := createServer()
	go func() {
		log.Printf("старт: ws://%s/proxy", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("ошибка: %v", err)
		}
	}()

	systray.Run(func() {
		systray.SetIcon(createTrayIcon())
		systray.SetTitle("MB-WS-PROXY")
		systray.SetTooltip("MB-WS-PROXY — мост WebSocket ⇄ TCP\nПорт: " + addr)

		mStatus := systray.AddMenuItem("Сервер запущен", "")
		mStatus.Disable()
		systray.AddSeparator()

		mStart := systray.AddMenuItem("Запустить сервер", "")
		mStart.Disable() // Изначально сервер запущен, кнопка неактивна
		mStop := systray.AddMenuItem("Остановить сервер", "")
		systray.AddSeparator()
		mQuit := systray.AddMenuItem("Выход", "")

		go func() {
			for {
				select {
				case <-mStart.ClickedCh:
					log.Println("запуск сервера")
					// Создаём новый сервер (старый уже закрыт)
					srv = createServer()
					go func() {
						log.Printf("старт: ws://%s/proxy", addr)
						if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
							log.Printf("ошибка запуска: %v", err)
							return
						}
					}()
					mStatus.SetTitle("Сервер запущен")
					mStart.Disable()
					mStop.Enable()
				case <-mStop.ClickedCh:
					log.Println("остановка сервера")
					sessions.CloseAll()
					shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					srv.Shutdown(shutdownCtx)
					cancel()
					mStatus.SetTitle("Сервер остановлен")
					mStart.Enable()
					mStop.Disable()
				case <-mQuit.ClickedCh:
					log.Println("выход")
					sessions.CloseAll()
					shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					srv.Shutdown(shutdownCtx)
					cancel()
					systray.Quit()
					return
				case <-ctx.Done():
					systray.Quit()
					return
				}
			}
		}()
	}, func() {
		log.Println("остановлен")
	})
}
