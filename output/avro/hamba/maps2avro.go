package maps2avro

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"time"

	ha "github.com/hamba/avro/v2"
	ho "github.com/hamba/avro/v2/ocf"

	la "github.com/takanoriyanagitani/go-log2avro"
	util "github.com/takanoriyanagitani/go-log2avro/util"
)

var (
	ErrNoTime  error = errors.New("no time found in the log")
	ErrNoLevel error = errors.New("no level found in the log")

	ErrInvalidLevel error = errors.New("invalid level")
	ErrInvalidTime  error = errors.New("invalid time")
)

func mapsToAvro(
	ctx context.Context,
	w io.Writer,
	s ha.Schema,
	m la.Logs,
	map2time func(map[string]any) (time.Time, error),
	map2level func(map[string]any) (string, error),
	map2message func(map[string]any) (any, error),
) error {
	enc, e := ho.NewEncoderWithSchema(s, w)
	if nil != e {
		return e
	}
	defer enc.Close()

	var buf map[string]any = map[string]any{}
	var attributes []any

	for row, e := range m {
		if nil != e {
			return e
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		clear(buf)
		clear(attributes)
		attributes = attributes[:0]

		timestamp, e := map2time(row)
		if nil != e {
			return e
		}
		buf["time"] = timestamp.UnixMicro()

		level, e := map2level(row)
		if nil != e {
			return e
		}
		buf["level"] = level

		body, e := map2message(row)
		if nil != e {
			return e
		}
		buf["body"] = body

		for key, val := range maps.All(row) {
			attributes = append(attributes, map[string]any{
				"key": key,
				"val": val,
			})
		}

		buf["attributes"] = attributes

		e = enc.Encode(buf)
		if nil != e {
			return e
		}
	}

	return enc.Flush()
}

func MapsToAvro(
	ctx context.Context,
	w io.Writer,
	schema string,
	m la.Logs,
	mapper Mapper,
) error {
	s, e := ha.Parse(schema)
	if nil != e {
		return e
	}
	return mapsToAvro(
		ctx,
		w,
		s,
		m,
		mapper.MapToTime,
		mapper.MapToLevel,
		mapper.MapToBody,
	)
}

type Mapper struct {
	MapToTime  func(map[string]any) (time.Time, error)
	MapToLevel func(map[string]any) (string, error)
	MapToBody  func(map[string]any) (any, error)
}

type MapperConfig struct {
	TimeConfig  util.IO[string]
	LevelConfig util.IO[string]
	BodyConfig  util.IO[string]

	AnyToTime  func(any) (time.Time, error)
	AnyToLevel func(any) (string, error)
}

type TimeConfig struct {
	Layout string
}

var TimeConfigDefault TimeConfig = TimeConfig{Layout: time.RFC3339Nano}

func (c TimeConfig) ToParser() func(string) (time.Time, error) {
	return func(s string) (time.Time, error) {
		return time.Parse(c.Layout, s)
	}
}

func (c TimeConfig) ToParserAny() func(any) (time.Time, error) {
	var s2t func(string) (time.Time, error) = c.ToParser()
	return func(a any) (time.Time, error) {
		switch t := a.(type) {
		case string:
			return s2t(t)
		default:
			return time.Time{}, fmt.Errorf("%w: %v", ErrInvalidTime, t)
		}
	}
}

func ParseLevel(a any) (string, error) {
	switch t := a.(type) {
	case string:
		return t, nil
	default:
		return "", fmt.Errorf("%w: %v", ErrInvalidLevel, t)
	}
}

func (c MapperConfig) ToMapper() util.IO[Mapper] {
	return func(ctx context.Context) (Mapper, error) {
		t, te := c.TimeConfig(ctx)
		l, le := c.LevelConfig(ctx)
		b, be := c.BodyConfig(ctx)

		return Mapper{
			MapToTime: func(m map[string]any) (time.Time, error) {
				a, found := m[t]
				if !found {
					return time.Time{}, ErrNoTime
				}
				parsed, e := c.AnyToTime(a)
				delete(m, t)
				return parsed, e
			},
			MapToLevel: func(m map[string]any) (string, error) {
				a, found := m[l]
				if !found {
					return "", ErrNoLevel
				}
				parsed, e := c.AnyToLevel(a)
				delete(m, l)
				return parsed, e
			},
			MapToBody: func(m map[string]any) (any, error) {
				a, found := m[b]
				if !found {
					return "", nil
				}
				delete(m, b)
				return a, nil
			},
		}, errors.Join(te, le, be)
	}
}

func SchemaToWriterToAvro(
	schema string,
) func(io.Writer) func(Mapper) func(la.Logs) util.IO[util.Void] {
	return func(w io.Writer) func(Mapper) func(la.Logs) util.IO[util.Void] {
		return func(m Mapper) func(la.Logs) util.IO[util.Void] {
			return func(l la.Logs) util.IO[util.Void] {
				return func(ctx context.Context) (util.Void, error) {
					return util.Empty, MapsToAvro(
						ctx,
						w,
						schema,
						l,
						m,
					)
				}
			}
		}
	}
}

func SchemaToStdoutToAvro(
	schema string,
) func(Mapper) func(la.Logs) util.IO[util.Void] {
	return SchemaToWriterToAvro(schema)(os.Stdout)
}

type ConvertConfig struct {
	Schema string
	Mapper
}

func (c ConvertConfig) ToLogsToStdoutToAvro() func(la.Logs) util.IO[util.Void] {
	return SchemaToStdoutToAvro(c.Schema)(c.Mapper)
}

func MapperConfigNewDefault(
	timeKey util.IO[string],
	levelKey util.IO[string],
	bodyKey util.IO[string],
) MapperConfig {
	return MapperConfig{
		TimeConfig:  timeKey,
		LevelConfig: levelKey,
		BodyConfig:  bodyKey,
		AnyToTime:   TimeConfigDefault.ToParserAny(),
		AnyToLevel:  ParseLevel,
	}
}

var TimeKeyDefault util.IO[string] = util.Of("time")
var LevelKeyDefault util.IO[string] = util.Of("level")
var BodyKeyDefault util.IO[string] = util.Of("body")

var MapperConfigDefault MapperConfig = MapperConfigNewDefault(
	TimeKeyDefault,
	LevelKeyDefault,
	BodyKeyDefault,
)

var MapperDefault util.IO[Mapper] = MapperConfigDefault.ToMapper()

var SimpleSchemaDefault util.IO[string] = util.Of(la.SimpleLogSchema)

var convertConfigDefault util.IO[ConvertConfig] = util.Bind(
	SimpleSchemaDefault,
	func(schema string) util.IO[ConvertConfig] {
		return func(ctx context.Context) (ConvertConfig, error) {
			mapper, e := MapperDefault(ctx)
			return ConvertConfig{
				Schema: schema,
				Mapper: mapper,
			}, e
		}
	},
)

var ConvertConfigDefault util.IO[ConvertConfig] = convertConfigDefault
