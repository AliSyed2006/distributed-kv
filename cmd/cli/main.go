package main

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/AliSyed2006/distributed-kv/internal/storage"
	"github.com/charmbracelet/lipgloss"
)

var (
	headerStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#FAFAFA")).
			Background(lipgloss.Color("#7D56F4")).
			Padding(1, 2).
			MarginBottom(1).
			Render("DISTRIBUTED-KV LOG-STRUCTURED ENGINE")

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
	fmt.Println(headerStyle)

	fmt.Println("Commands: (GET, SET, DEL, STATS, EXIT)")

	// Initialize StorageEngine
	opts := storage.EngineOptions{
		Dir:        "./data",
		MaxMemSize: 64 * 1024 * 1024, // 4KB for easy flush testing
	}
	engine, err := storage.NewStorageEngine(opts)
	if err != nil {
		fmt.Println(errorStyle.Render(fmt.Sprintf("Failed to initialize engine: %v", err)))
		os.Exit(1)
	}

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\n" + infoStyle.Render("Shutting down engine..."))
		if err := engine.Close(); err != nil {
			fmt.Println(errorStyle.Render(fmt.Sprintf("Error closing engine: %v", err)))
		}
		os.Exit(0)
	}()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print(promptStyle.Render("kv-db> "))
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
			key, val := []byte(parts[1]), []byte(strings.Join(parts[2:], " "))
			if err := engine.Put(key, val); err != nil {
				fmt.Println(errorStyle.Render(fmt.Sprintf("SET failed: %v", err)))
			} else {
				fmt.Println(successStyle.Render(fmt.Sprintf("OK: %s = %s", parts[1], parts[2])))
			}

		case "GET":
			if len(parts) < 2 {
				fmt.Println(errorStyle.Render("Usage: GET <key>"))
				continue
			}
			val, ok := engine.Get([]byte(parts[1]))
			if !ok {
				fmt.Println(errorStyle.Render("NOT FOUND"))
			} else {
				fmt.Println(successStyle.Render(string(val)))
			}

		case "DEL":
			if len(parts) < 2 {
				fmt.Println(errorStyle.Render("Usage: DEL <key>"))
				continue
			}
			if err := engine.Delete([]byte(parts[1])); err != nil {
				fmt.Println(errorStyle.Render(fmt.Sprintf("DEL failed: %v", err)))
			} else {
				fmt.Println(successStyle.Render("DELETED"))
			}

		case "STATS":
			stats := engine.Stats()
			fmt.Println(infoStyle.Render("Engine Statistics:"))
			fmt.Printf("Data Directory:  %s\n", opts.Dir)
			fmt.Printf("MemTable Size:   %d bytes / %d (max)\n", stats.MemTableSize, stats.MaxMemSize)
			fmt.Printf("Active SSTables: %d\n", stats.SSTableCount)

		case "EXIT", "QUIT":
			fmt.Println(infoStyle.Render("Exiting..."))
			if err := engine.Close(); err != nil {
				fmt.Println(errorStyle.Render(fmt.Sprintf("Error closing engine: %v", err)))
			}
			return

		default:
			fmt.Println(errorStyle.Render(fmt.Sprintf("Unknown command: %s", cmd)))
		}
	}
}
