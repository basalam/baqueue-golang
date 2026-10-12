package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	baqueue "github.com/basalam/baqueue-golang"
	"github.com/basalam/baqueue-golang/dashboard"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9100", "listen address")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	driver := baqueue.NewMemoryDriver()
	q := baqueue.NewQueue(driver)
	if err := q.RegisterFunc("demo.ProcessOrder", func(ctx context.Context, data map[string]any) (any, error) {
		timer := time.NewTimer(12 * time.Second)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
		return map[string]any{"order": data["order"]}, nil
	}, baqueue.OnQueue("orders")); err != nil {
		log.Fatal(err)
	}
	if err := q.RegisterFunc("demo.SendEmail", func(ctx context.Context, data map[string]any) (any, error) {
		timer := time.NewTimer(3 * time.Second)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
		return nil, errors.New("Example delivery failure")
	}, baqueue.OnQueue("emails"), baqueue.WithMaxAttempts(2)); err != nil {
		log.Fatal(err)
	}
	seed := func() {
		for i := 0; i < 8; i++ {
			if _, err := q.Push(ctx, "demo.ProcessOrder", map[string]any{"order": fmt.Sprintf("ORD-%03d", i+1)}, baqueue.WithTags("demo")); err != nil {
				log.Print(err)
			}
		}
		_, err := q.Push(ctx, "demo.SendEmail", map[string]any{"to": "demo@example.com"})
		if err != nil {
			log.Print(err)
		}
		_, err = q.Later(ctx, "demo.ProcessOrder", 5*time.Minute, map[string]any{"order": "SCHEDULED"})
		if err != nil {
			log.Print(err)
		}
	}
	seed()
	config := baqueue.DefaultSupervisorConfig()
	config.Name = "demo"
	config.Queues = []string{"orders", "emails"}
	config.MinWorkers = 3
	config.MaxWorkers = 3
	config.Balance = "false"
	supervisor := baqueue.NewSupervisor(q, config)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := supervisor.Run(ctx); err != nil {
			log.Print(err)
			cancel()
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(45 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				seed()
			}
		}
	}()
	server := &http.Server{Addr: *addr, Handler: dashboard.New(driver, dashboard.WithContext(ctx)), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		_ = server.Shutdown(shutdown)
	}()
	log.Printf("BaQueue Go demo: http://%s", *addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Print(err)
	}
	cancel()
	wg.Wait()
}
