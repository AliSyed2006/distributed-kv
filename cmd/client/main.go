package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/AliSyed2006/distributed-kv/api/proto"
	"github.com/charmbracelet/lipgloss"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	headerStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#FAFAFA")).
			Background(lipgloss.Color("#FF5F87")).
			Padding(1, 4).
			MarginBottom(1).
			Render("KV-CLIENT (TUNNEL MODE)")

	successStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00FF00")).
			Border(lipgloss.RoundedBorder()).
			Padding(0, 1)

	errorStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FF0000")).
			Border(lipgloss.DoubleBorder()).
			Padding(0, 1)

	infoStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00FFFF")).
			Padding(0, 1)

	promptStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#F4E05D")).
			Bold(true)
)

func main() {
	// Address is localhost because of the 'gh' tunnel
	addr := flag.String("addr", "localhost:50051", "server address")
	workers := flag.Int("workers", 10, "number of concurrent workers for stress test")
	n := flag.Int("n", 1000, "total requests for stress test")
	flag.Parse()

	fmt.Println(headerStyle)

	// Use Insecure credentials because the GH tunnel is already encrypted.
	// This fixes the 'handshake failed' / 'connection reset' error.
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	client := proto.NewKVServiceClient(conn)

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print(promptStyle.Render("kv-client> "))
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "SET":
			if len(parts) < 3 {
				fmt.Println(errorStyle.Render("Usage: SET <key> <value>"))
				continue
			}
			setKV(client, parts[1], strings.Join(parts[2:], " "))

		case "GET":
			if len(parts) < 2 {
				fmt.Println(errorStyle.Render("Usage: GET <key>"))
				continue
			}
			getKV(client, parts[1])

		case "STATS":
			getStats(client)

		case "STRESS":
			stressTest(client, *workers, *n)

		case "EXIT", "QUIT":
			return

		default:
			fmt.Println(errorStyle.Render(fmt.Sprintf("Unknown command: %s", cmd)))
		}
	}
}

func setKV(client proto.KVServiceClient, key, val string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	resp, err := client.Put(ctx, &proto.PutRequest{
		Key:   []byte(key),
		Value: []byte(val),
	})

	if err != nil {
		fmt.Println(errorStyle.Render(fmt.Sprintf("RPC Error: %v", err)))
		return
	}

	if !resp.Success {
		fmt.Println(errorStyle.Render(fmt.Sprintf("Server Error: %s", resp.Error)))
	} else {
		fmt.Println(successStyle.Render("OK"))
	}
}

func getKV(client proto.KVServiceClient, key string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	resp, err := client.Get(ctx, &proto.GetRequest{
		Key: []byte(key),
	})

	if err != nil {
		fmt.Println(errorStyle.Render(fmt.Sprintf("RPC Error: %v", err)))
		return
	}

	if !resp.Found {
		fmt.Println(errorStyle.Render("NOT FOUND"))
	} else {
		fmt.Println(successStyle.Render(string(resp.Value)))
	}
}

func getStats(client proto.KVServiceClient) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	resp, err := client.Stats(ctx, &proto.StatsRequest{})
	if err != nil {
		fmt.Println(errorStyle.Render(fmt.Sprintf("RPC Error: %v", err)))
		return
	}

	statsBox := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		Padding(1).
		Foreground(lipgloss.Color("#00FFFF")).
		Render(fmt.Sprintf(
			"MemTable: %d bytes\nSSTables: %d\nMaxMem:   %d bytes",
			resp.MemTableSize, resp.SstableCount, resp.MaxMemSize,
		))
	fmt.Println(statsBox)
}

func stressTest(client proto.KVServiceClient, numWorkers, totalReqs int) {
	fmt.Println(infoStyle.Render(fmt.Sprintf("Starting Tunnel Stress Test: %d workers, %d total requests...", numWorkers, totalReqs)))

	var wg sync.WaitGroup
	reqPerWorker := totalReqs / numWorkers

	val := make([]byte, 8192)
	rand.Read(val)

	start := time.Now()

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < reqPerWorker; j++ {
				key := make([]byte, 32)
				rand.Read(key)

				ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
				_, _ = client.Put(ctx, &proto.PutRequest{
					Key:   key,
					Value: val,
				})
				cancel()
			}
		}()
	}

	wg.Wait()
	duration := time.Since(start)

	throughput := float64(totalReqs) / duration.Seconds()
	avgLatency := duration.Seconds() * 1000 / float64(totalReqs)

	resultBox := lipgloss.NewStyle().
		Border(lipgloss.DoubleBorder()).
		BorderForeground(lipgloss.Color("#00FF00")).
		Padding(1).
		Render(fmt.Sprintf(
			"TEST COMPLETE\n\nTotal Time: %v\nThroughput: %.2f Req/sec\nAvg Latency: %.4f ms",
			duration, throughput, avgLatency,
		))
	fmt.Println(resultBox)
}
