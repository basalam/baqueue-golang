package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	baqueue "github.com/basalam/baqueue-golang"
	"github.com/basalam/baqueue-golang/cli"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	register := func(q *baqueue.Queue) error {
		return q.RegisterFunc("send_email", func(ctx context.Context, data map[string]any) (any, error) {
			fmt.Printf("Sending email to %v\n", data["to"])
			return nil, nil
		}, baqueue.OnQueue("emails"), baqueue.WithMaxAttempts(3))
	}
	if err := cli.Run(ctx, os.Args[1:], os.Stdout, register); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
