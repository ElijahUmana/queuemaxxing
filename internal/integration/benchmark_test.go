package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ElijahUmana/queuemaxxing/internal/journal"
)

func BenchmarkJournalDurableAppend(b *testing.B) {
	for _, payloadBytes := range []int{128, 1024, 64 * 1024} {
		b.Run(fmt.Sprintf("payload=%d", payloadBytes), func(b *testing.B) {
			store, err := journal.Open(journal.Config{Dir: filepath.Join(b.TempDir(), "journal")})
			if err != nil {
				b.Fatal(err)
			}
			defer store.Close()
			payload := make([]byte, payloadBytes)
			b.SetBytes(int64(payloadBytes))
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				var transactionID journal.TransactionID
				transactionID[0] = byte(index)
				if _, err := store.Append(context.Background(), transactionID, payload); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkJournalRecovery(b *testing.B) {
	for _, records := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("records=%d", records), func(b *testing.B) {
			source := filepath.Join(b.TempDir(), "source")
			store, err := journal.Open(journal.Config{Dir: source})
			if err != nil {
				b.Fatal(err)
			}
			payload := make([]byte, 256)
			for start := 0; start < records; start += 256 {
				end := min(start+256, records)
				batch := make([]journal.Record, 0, end-start)
				for index := start; index < end; index++ {
					var transactionID journal.TransactionID
					transactionID[0] = byte(index)
					batch = append(batch, journal.Record{TransactionID: transactionID, Payload: payload})
				}
				if _, err := store.AppendBatch(context.Background(), batch); err != nil {
					b.Fatal(err)
				}
			}
			if err := store.Close(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				b.StopTimer()
				directory := filepath.Join(b.TempDir(), fmt.Sprintf("copy-%d", iteration))
				copyJournalTreeForBenchmark(b, source, directory)
				b.StartTimer()
				recovered, err := journal.Open(journal.Config{Dir: directory})
				if err != nil {
					b.Fatal(err)
				}
				if got := len(recovered.Records()); got != records {
					b.Fatalf("recovered %d records, want %d", got, records)
				}
				if err := recovered.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func copyJournalTreeForBenchmark(b *testing.B, source, destination string) {
	b.Helper()
	if err := filepath.WalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o700)
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, contents, 0o600)
	}); err != nil {
		b.Fatal(err)
	}
}
