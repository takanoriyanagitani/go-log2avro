package log2avro

import (
	"iter"

	_ "embed"
)

type StructuredLog map[string]any
type Logs iter.Seq2[StructuredLog, error]

//go:embed simple-log.avsc
var SimpleLogSchema string
