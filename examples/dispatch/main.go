package main

import (
	"context"
	"fmt"
	"log"
	"time"

	baqueue "github.com/basalam/baqueue-golang"
)

func main() {
	ctx := context.Background()
	q, err := baqueue.Open(ctx, baqueue.DefaultConfig())
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close(ctx)
	id, err := q.Push(ctx, "send_email", map[string]any{"to": "hello@example.com"}, baqueue.OnQueue("emails"), baqueue.WithMaxAttempts(3))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Queued:", id)
	id, err = q.Later(ctx, "send_email", 10*time.Second, map[string]any{"to": "later@example.com"}, baqueue.OnQueue("emails"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Scheduled:", id)
}
