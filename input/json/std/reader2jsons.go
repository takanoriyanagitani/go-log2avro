package reader2jsons

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"

	la "github.com/takanoriyanagitani/go-log2avro"
	util "github.com/takanoriyanagitani/go-log2avro/util"
)

func ReaderToJsons(r io.Reader) la.Logs {
	return func(yield func(la.StructuredLog, error) bool) {
		var br io.Reader = bufio.NewReader(r)
		var err error = nil
		var dec *json.Decoder = json.NewDecoder(br)

		var buf map[string]any
		for {
			clear(buf)

			err = dec.Decode(&buf)
			if io.EOF == err {
				return
			}

			if !yield(buf, err) {
				return
			}
		}
	}
}

func StdinToJsons(_ context.Context) (la.Logs, error) {
	return ReaderToJsons(os.Stdin), nil
}

var LogSourceStdin util.IO[la.Logs] = StdinToJsons
