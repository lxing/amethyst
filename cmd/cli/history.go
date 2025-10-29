package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const maxHistorySize = 1000

type History struct {
	commands []string
	file     string
}

func newHistory() (*History, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}

	histFile := filepath.Join(home, ".adb_history")
	h := &History{
		commands: make([]string, 0, maxHistorySize),
		file:     histFile,
	}

	// Load existing history
	if err := h.load(); err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	return h, nil
}

func (h *History) load() error {
	f, err := os.Open(h.file)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			h.commands = append(h.commands, line)
		}
	}

	return scanner.Err()
}

func (h *History) add(cmd string) {
	cmd = strings.TrimSpace(cmd)
	if cmd == "" {
		return
	}

	// Don't add duplicates of the last command
	if len(h.commands) > 0 && h.commands[len(h.commands)-1] == cmd {
		return
	}

	h.commands = append(h.commands, cmd)

	if len(h.commands) > maxHistorySize {
		h.commands = h.commands[len(h.commands)-maxHistorySize:]
	}
}

func (h *History) save() error {
	f, err := os.Create(h.file)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, cmd := range h.commands {
		if _, err := fmt.Fprintln(f, cmd); err != nil {
			return err
		}
	}

	return nil
}

func (h *History) list(n int) []string {
	if n <= 0 || n > len(h.commands) {
		n = len(h.commands)
	}

	start := len(h.commands) - n
	return h.commands[start:]
}
