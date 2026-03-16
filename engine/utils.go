package engine

import (
	"encoding/json"
	"hash/fnv"

	"github.com/kordar/goetl"
)

func HashPartitionKey(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

func FormatMessageJSON(msg goetl.Message) string {
	b, _ := json.Marshal(msg)
	return string(b)
}
