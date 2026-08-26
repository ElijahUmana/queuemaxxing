package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	queueapi "github.com/ElijahUmana/queuemaxxing/api"
	queueclient "github.com/ElijahUmana/queuemaxxing/client"
)

func TestRealBinarySIGKILLPreservesConfirmedMutations(t *testing.T) {
	if testing.Short() {
		t.Skip("real subprocess crash test")
	}
	if runtime.GOOS == "windows" {
		t.Skip("SIGKILL semantics are Unix-specific")
	}
	binary := buildServerBinary(t)
	dataDirectory := filepath.Join(t.TempDir(), "data")
	artifacts := filepath.Join(t.TempDir(), "artifacts")
	address, err := FreeTCPAddress()
	if err != nil {
		t.Fatal(err)
	}
	baseURL := "http://" + address

	process, err := StartProcess(context.Background(), binary, []string{"-listen", address, "-data-dir", dataDirectory}, nil, filepath.Join(artifacts, "before-kill"))
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := process.WaitForHTTP(waitCtx, baseURL+"/health/ready", 200); err != nil {
		_ = process.Kill()
		_ = process.Wait()
		t.Fatal(err)
	}
	client, err := queueclient.New(baseURL, queueclient.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	queueName := "crash"
	_, err = client.CreateQueue(ctx, queueapi.CreateQueueRequest{
		Name: queueName, Ordering: queueapi.FIFO, DefaultVisibilityTimeoutMS: 30_000, MaxDeliveries: 3,
	}, "create")
	if err != nil {
		t.Fatal(err)
	}
	confirmed, err := client.Enqueue(ctx, queueName, queueapi.EnqueueRequest{Payload: json.RawMessage(`{"kind":"survive"}`)}, "enqueue-survive")
	if err != nil {
		t.Fatal(err)
	}
	acked, err := client.Enqueue(ctx, queueName, queueapi.EnqueueRequest{Payload: json.RawMessage(`{"kind":"acked"}`)}, "enqueue-acked")
	if err != nil {
		t.Fatal(err)
	}
	first, err := client.Receive(ctx, queueName, queueapi.ReceiveRequest{VisibilityTimeoutMS: int64Pointer(30_000)})
	if err != nil || len(first.Messages) != 1 || first.Messages[0].Message.ID != confirmed.Data.ID {
		t.Fatalf("first delivery=%+v err=%v", first, err)
	}
	if _, err := client.Nack(ctx, queueName, confirmed.Data.ID, queueapi.NackRequest{ReceiptHandle: first.Messages[0].ReceiptHandle}, "nack-survive"); err != nil {
		t.Fatal(err)
	}
	second, err := client.Receive(ctx, queueName, queueapi.ReceiveRequest{VisibilityTimeoutMS: int64Pointer(30_000)})
	if err != nil || len(second.Messages) != 1 || second.Messages[0].Message.ID != confirmed.Data.ID {
		t.Fatalf("redelivery=%+v err=%v", second, err)
	}
	if _, err := client.Ack(ctx, queueName, confirmed.Data.ID, second.Messages[0].ReceiptHandle, "ack-survive"); err != nil {
		t.Fatal(err)
	}
	third, err := client.Receive(ctx, queueName, queueapi.ReceiveRequest{VisibilityTimeoutMS: int64Pointer(30_000)})
	if err != nil || len(third.Messages) != 1 || third.Messages[0].Message.ID != acked.Data.ID {
		t.Fatalf("third delivery=%+v err=%v", third, err)
	}
	if _, err := client.Ack(ctx, queueName, acked.Data.ID, third.Messages[0].ReceiptHandle, "ack-acked"); err != nil {
		t.Fatal(err)
	}
	if err := process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := process.Wait(); err == nil {
		t.Fatal("SIGKILL process unexpectedly exited successfully")
	}

	restarted, err := StartProcess(context.Background(), binary, []string{"-listen", address, "-data-dir", dataDirectory}, nil, filepath.Join(artifacts, "after-kill"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = restarted.Interrupt()
		_ = restarted.Wait()
	}()
	waitCtx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := restarted.WaitForHTTP(waitCtx, baseURL+"/health/ready", 200); err != nil {
		t.Fatal(err)
	}
	page, err := client.ListMessages(ctx, queueName, queueclient.ListFilter{Limit: 50})
	if err != nil {
		t.Fatal(err)
	}
	states := make(map[string]queueapi.MessageState)
	for _, message := range page.Messages {
		states[message.ID] = message.State
	}
	if states[confirmed.Data.ID] != queueapi.StateAcked || states[acked.Data.ID] != queueapi.StateAcked {
		t.Fatalf("post-crash states=%v", states)
	}
	empty, err := client.Receive(ctx, queueName, queueapi.ReceiveRequest{})
	if err != nil || len(empty.Messages) != 0 {
		t.Fatalf("post-crash receive=%+v err=%v", empty, err)
	}
}

func buildServerBinary(t *testing.T) string {
	t.Helper()
	repository := filepath.Clean(filepath.Join("..", ".."))
	binary := filepath.Join(t.TempDir(), "qmax")
	command := exec.Command("go", "build", "-trimpath", "-o", binary, "./cmd/qmax")
	command.Dir = repository
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("build real server: %v\n%s", err, output)
	}
	return binary
}

func int64Pointer(value int64) *int64 { return &value }

func TestRealBinaryHelpersRejectInvalidState(t *testing.T) {
	if _, err := os.Stat(filepath.Join("..", "..", "go.mod")); err != nil {
		t.Fatal(err)
	}
	if _, err := queueclient.New(":invalid", queueclient.Options{}); err == nil {
		t.Fatal("invalid client URL accepted")
	}
	var problem *queueclient.ProblemError
	if errors.As(fmt.Errorf("wrapped: %w", &queueclient.ProblemError{Problem: queueapi.Problem{Code: "x", Detail: "failure"}}), &problem) && problem.Problem.Code != "x" {
		t.Fatal("wrapped problem lost its code")
	}
}
