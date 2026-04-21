package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
			Render("KV-CLIENT (HONEST BENCHMARK MODE)")

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
	addr := flag.String("addr", "localhost:50051", "server address")
	workersFlag := flag.Int("workers", 10, "number of concurrent workers for stress test")
	nFlag := flag.Int("n", 10000, "total requests for stress test")
	flag.Parse()

	fmt.Println(headerStyle)

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
			w := *workersFlag
			n := *nFlag
			if len(parts) >= 3 {
				if parsedW, err := strconv.Atoi(parts[1]); err == nil {
					w = parsedW
				}
				if parsedN, err := strconv.Atoi(parts[2]); err == nil {
					n = parsedN
				}
			}
			stressTest(client, w, n)

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
	fmt.Println(infoStyle.Render(fmt.Sprintf("Generating %d unique keys for Two-Phase Benchmark...", totalReqs)))

	// Pre-generate keys so we can READ exactly what we WROTE
	keys := make([][]byte, totalReqs)
	for i := 0; i < totalReqs; i++ {
		k := make([]byte, 32)
		rand.Read(k)
		keys[i] = k
	}

	val := make([]byte, 8192) // 8KB payload
	rand.Read(val)

	reqPerWorker := totalReqs / numWorkers
	var writeErrors, readErrors uint32
	var notFound uint32

	// ==========================================
	// PHASE 1: WRITE (PUT)
	// ==========================================
	fmt.Println(infoStyle.Render(fmt.Sprintf("▶ Phase 1: WRTING %d payloads (8KB) using %d workers...", totalReqs, numWorkers)))
	var wg sync.WaitGroup
	startWrite := time.Now()

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerIdx int) {
			defer wg.Done()
			startIdx := workerIdx * reqPerWorker
			endIdx := startIdx + reqPerWorker

			for j := startIdx; j < endIdx; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
				resp, err := client.Put(ctx, &proto.PutRequest{
					Key:   keys[j],
					Value: val,
				})
				// Catch network errors OR server-side rejections
				if err != nil || (resp != nil && !resp.Success) {
					atomic.AddUint32(&writeErrors, 1)
				}
				cancel()
			}
		}(i)
	}
	wg.Wait()
	writeDuration := time.Since(startWrite)
	writeThroughput := float64(totalReqs) / writeDuration.Seconds()
	writeAvgLatency := writeDuration.Seconds() * 1000 / float64(totalReqs)

	// ==========================================
	// PHASE 2: READ (GET)
	// ==========================================
	fmt.Println(infoStyle.Render(fmt.Sprintf("▶ Phase 2: READING %d payloads using %d workers...", totalReqs, numWorkers)))
	startRead := time.Now()

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerIdx int) {
			defer wg.Done()
			startIdx := workerIdx * reqPerWorker
			endIdx := startIdx + reqPerWorker

			for j := startIdx; j < endIdx; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
				resp, err := client.Get(ctx, &proto.GetRequest{
					Key: keys[j],
				})
				if err != nil {
					atomic.AddUint32(&readErrors, 1)
				} else if !resp.Found {
					atomic.AddUint32(&notFound, 1)
				}
				cancel()
			}
		}(i)
	}
	wg.Wait()
	readDuration := time.Since(startRead)
	readThroughput := float64(totalReqs) / readDuration.Seconds()
	readAvgLatency := readDuration.Seconds() * 1000 / float64(totalReqs)

	// ==========================================
	// RENDER RESULTS
	// ==========================================

	errColor := "#00FF00"
	if writeErrors > 0 || readErrors > 0 || notFound > 0 {
		errColor = "#FF0000"
	}

	resultBox := lipgloss.NewStyle().
		Border(lipgloss.DoubleBorder()).
		BorderForeground(lipgloss.Color(errColor)).
		Padding(1).
		Render(fmt.Sprintf(
			"BENCHMARK COMPLETE\n"+
				"────────────────────────────────────\n"+
				"WRITE (PUT) STATS:\n"+
				"  Time:       %v\n"+
				"  Throughput: %.2f Req/sec\n"+
				"  Latency:    %.4f ms\n"+
				"  Errors:     %d\n\n"+
				"READ (GET) STATS:\n"+
				"  Time:       %v\n"+
				"  Throughput: %.2f Req/sec\n"+
				"  Latency:    %.4f ms\n"+
				"  Not Found:  %d\n"+
				"  Errors:     %d",
			writeDuration, writeThroughput, writeAvgLatency, writeErrors,
			readDuration, readThroughput, readAvgLatency, notFound, readErrors,
		))
	fmt.Println(resultBox)
}
