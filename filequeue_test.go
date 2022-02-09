package filequeue

import (
	"testing"
)

func BenchmarkFileQueue(b *testing.B) {
	q, err := NewFileQueue("file_queue", "recordlog", "logic", 1, 1000, true)
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push([]byte("1"))
	}
}

func BenchmarkFileQueuePop(b *testing.B) {
	log.ErrorF("")
	q, err := NewFileQueue("file_queue", "recordlog", "logic", 1, 1000, true)
	if err != nil {
		panic(err)
	}
	for i := 0; i < b.N; i++ {
		q.Push([]byte("1"))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Pop()
	}
}
