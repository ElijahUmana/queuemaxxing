package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Process struct {
	Command   *exec.Cmd
	Stdout    *lockedBuffer
	Stderr    *lockedBuffer
	Artifacts string
}

type Operation struct {
	ID          string          `json:"id"`
	Kind        string          `json:"kind"`
	Request     json.RawMessage `json:"request,omitempty"`
	InvokedAt   time.Time       `json:"invoked_at"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
	Status      int             `json:"status,omitempty"`
	Response    json.RawMessage `json:"response,omitempty"`
	Error       string          `json:"error,omitempty"`
}

type OperationJournal struct {
	mu         sync.Mutex
	operations map[string]Operation
	order      []string
}

func StartProcess(ctx context.Context, binary string, args []string, environment map[string]string, artifacts string) (*Process, error) {
	if err := os.MkdirAll(artifacts, 0o700); err != nil {
		return nil, fmt.Errorf("create artifact directory: %w", err)
	}
	// #nosec G204 -- binary and args are supplied only by repository-owned integration tests.
	command := exec.CommandContext(ctx, binary, args...)
	command.Env = append([]string{}, os.Environ()...)
	for key, value := range environment {
		command.Env = append(command.Env, key+"="+value)
	}
	stdout := &lockedBuffer{}
	stderr := &lockedBuffer{}
	command.Stdout = stdout
	command.Stderr = stderr
	if err := command.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", binary, err)
	}
	return &Process{Command: command, Stdout: stdout, Stderr: stderr, Artifacts: artifacts}, nil
}

func (process *Process) WaitForHTTP(ctx context.Context, url string, expectedStatus int) error {
	client := &http.Client{Timeout: 500 * time.Millisecond}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		response, err := client.Do(request)
		if err == nil {
			_, copyErr := io.Copy(io.Discard, response.Body)
			closeErr := response.Body.Close()
			if copyErr != nil {
				return fmt.Errorf("read readiness response: %w", copyErr)
			}
			if closeErr != nil {
				return fmt.Errorf("close readiness response: %w", closeErr)
			}
			if response.StatusCode == expectedStatus {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for %s: %w\nstdout:\n%s\nstderr:\n%s", url, ctx.Err(), process.Stdout.String(), process.Stderr.String())
		case <-ticker.C:
		}
	}
}

func (process *Process) WaitForOutput(ctx context.Context, stream, substring string) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var text string
		switch stream {
		case "stdout":
			text = process.Stdout.String()
		case "stderr":
			text = process.Stderr.String()
		default:
			return fmt.Errorf("unknown stream %q", stream)
		}
		if strings.Contains(text, substring) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for %q in %s: %w\nstdout:\n%s\nstderr:\n%s", substring, stream, ctx.Err(), process.Stdout.String(), process.Stderr.String())
		case <-ticker.C:
		}
	}
}

func (process *Process) Kill() error {
	if process.Command.Process == nil {
		return errors.New("process has not started")
	}
	if runtime.GOOS == "windows" {
		return process.Command.Process.Kill()
	}
	return process.Command.Process.Signal(syscall.SIGKILL)
}

func (process *Process) Interrupt() error {
	if process.Command.Process == nil {
		return errors.New("process has not started")
	}
	return process.Command.Process.Signal(os.Interrupt)
}

func (process *Process) Wait() error {
	err := process.Command.Wait()
	writeErr := process.WriteArtifacts()
	if err != nil && writeErr != nil {
		return errors.Join(err, writeErr)
	}
	if err != nil {
		return err
	}
	return writeErr
}

func (process *Process) WriteArtifacts() error {
	if err := os.MkdirAll(process.Artifacts, 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(process.Artifacts, "stdout.log"), process.Stdout.Bytes(), 0o600); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(process.Artifacts, "stderr.log"), process.Stderr.Bytes(), 0o600); err != nil {
		return err
	}
	metadata := map[string]any{"pid": 0, "args": process.Command.Args}
	if process.Command.Process != nil {
		metadata["pid"] = process.Command.Process.Pid
	}
	encoded, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(process.Artifacts, "process.json"), encoded, 0o600)
}

func FreeTCPAddress() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		return "", err
	}
	return address, nil
}

func NewOperationJournal() *OperationJournal {
	return &OperationJournal{operations: make(map[string]Operation)}
}

func (journal *OperationJournal) Invoke(id, kind string, request json.RawMessage, at time.Time) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if _, exists := journal.operations[id]; exists {
		return fmt.Errorf("operation %q already exists", id)
	}
	journal.operations[id] = Operation{ID: id, Kind: kind, Request: cloneBytes(request), InvokedAt: at}
	journal.order = append(journal.order, id)
	return nil
}

func (journal *OperationJournal) Complete(id string, status int, response json.RawMessage, operationErr error, at time.Time) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	operation, exists := journal.operations[id]
	if !exists {
		return fmt.Errorf("operation %q was not invoked", id)
	}
	if operation.CompletedAt != nil {
		return fmt.Errorf("operation %q is already complete", id)
	}
	completedAt := at
	operation.CompletedAt = &completedAt
	operation.Status = status
	operation.Response = cloneBytes(response)
	if operationErr != nil {
		operation.Error = operationErr.Error()
	}
	journal.operations[id] = operation
	return nil
}

func (journal *OperationJournal) Snapshot() []Operation {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	operations := make([]Operation, 0, len(journal.order))
	for _, id := range journal.order {
		operation := journal.operations[id]
		operation.Request = cloneBytes(operation.Request)
		operation.Response = cloneBytes(operation.Response)
		operations = append(operations, operation)
	}
	return operations
}

func (journal *OperationJournal) Completed() []Operation {
	return filterOperations(journal.Snapshot(), func(operation Operation) bool { return operation.CompletedAt != nil })
}

func (journal *OperationJournal) Ambiguous() []Operation {
	return filterOperations(journal.Snapshot(), func(operation Operation) bool { return operation.CompletedAt == nil })
}

func (journal *OperationJournal) Write(path string) error {
	encoded, err := json.MarshalIndent(journal.Snapshot(), "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, encoded, 0o600)
}

func CopyAndTruncate(source, destination string, size int64) error {
	// #nosec G304,G703 -- source and destination are test-owned temporary artifact paths.
	contents, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	if size < 0 || size > int64(len(contents)) {
		return fmt.Errorf("truncation size %d outside [0,%d]", size, len(contents))
	}
	// #nosec G304,G703 -- destination is a test-owned temporary artifact path.
	return os.WriteFile(destination, contents[:size], 0o600)
}

func CopyAndFlipBit(source, destination string, byteOffset int64, bit uint8) error {
	if bit > 7 {
		return fmt.Errorf("bit %d outside [0,7]", bit)
	}
	// #nosec G304,G703 -- source and destination are test-owned temporary artifact paths.
	contents, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	if byteOffset < 0 || byteOffset >= int64(len(contents)) {
		return fmt.Errorf("byte offset %d outside [0,%d)", byteOffset, len(contents))
	}
	contents[byteOffset] ^= 1 << bit
	// #nosec G304,G703 -- destination is a test-owned temporary artifact path.
	return os.WriteFile(destination, contents, 0o600)
}

func ReadPhase(reader io.Reader) (string, error) {
	line, err := bufio.NewReader(reader).ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(line), nil
}

func ChildHelperCommand(testBinary, testName string, environment map[string]string) *exec.Cmd {
	// #nosec G204 -- testBinary and testName are fixed by repository-owned subprocess tests.
	command := exec.Command(testBinary, "-test.run", "^"+testName+"$")
	command.Env = append(os.Environ(), "QUEUE_TEST_CHILD=1")
	for key, value := range environment {
		command.Env = append(command.Env, key+"="+value)
	}
	return command
}

func ParsePhaseIndex(value string) (int, error) {
	phase, err := strconv.Atoi(value)
	if err != nil || phase < 0 {
		return 0, fmt.Errorf("invalid phase index %q", value)
	}
	return phase, nil
}

type lockedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (buffer *lockedBuffer) Write(contents []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.buffer.Write(contents)
}

func (buffer *lockedBuffer) String() string {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.buffer.String()
}

func (buffer *lockedBuffer) Bytes() []byte {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return cloneBytes(buffer.buffer.Bytes())
}

func filterOperations(operations []Operation, keep func(Operation) bool) []Operation {
	filtered := make([]Operation, 0, len(operations))
	for _, operation := range operations {
		if keep(operation) {
			filtered = append(filtered, operation)
		}
	}
	return filtered
}

func cloneBytes(contents []byte) []byte {
	if contents == nil {
		return nil
	}
	return append([]byte(nil), contents...)
}
