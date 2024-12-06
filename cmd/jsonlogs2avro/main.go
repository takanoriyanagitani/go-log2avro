package main

import (
	"context"
	"fmt"
	"log"
	"os"

	la "github.com/takanoriyanagitani/go-log2avro"
	util "github.com/takanoriyanagitani/go-log2avro/util"

	js "github.com/takanoriyanagitani/go-log2avro/input/json/std"
	ah "github.com/takanoriyanagitani/go-log2avro/output/avro/hamba"
)

func GetEnvByKey(key string) util.IO[string] {
	return func(_ context.Context) (string, error) {
		val, found := os.LookupEnv(key)
		if !found {
			return "", fmt.Errorf("no value got for this key: %s", key)
		}
		return val, nil
	}
}

var timeConfig util.IO[string] = GetEnvByKey("ENV_TIME_KEY").OrElse(
	ah.TimeKeyDefault,
)

var levelConfig util.IO[string] = GetEnvByKey("ENV_LEVEL_KEY").OrElse(
	ah.LevelKeyDefault,
)

var bodyConfig util.IO[string] = GetEnvByKey("ENV_BODY_KEY").OrElse(
	ah.BodyKeyDefault,
)

var mapperConfig ah.MapperConfig = ah.MapperConfigNewDefault(
	timeConfig,
	levelConfig,
	bodyConfig,
)

var mapper util.IO[ah.Mapper] = mapperConfig.ToMapper()
var schemaString string = la.SimpleLogSchema

var logsSource util.IO[la.Logs] = js.LogSourceStdin

var convertCfg util.IO[ah.ConvertConfig] = util.Bind(
	mapper,
	util.Lift(func(m ah.Mapper) (ah.ConvertConfig, error) {
		return ah.ConvertConfig{
			Schema: schemaString,
			Mapper: m,
		}, nil
	}),
)

var stdin2logs2avro2stdout util.IO[util.Void] = util.Bind(
	logsSource,
	func(l la.Logs) util.IO[util.Void] {
		return func(ctx context.Context) (util.Void, error) {
			cfg, e := convertCfg(ctx)
			if nil != e {
				return util.Empty, e
			}
			return cfg.ToLogsToStdoutToAvro()(l)(ctx)
		}
	},
)

func sub(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	_, e := stdin2logs2avro2stdout(ctx)
	return e
}

func main() {
	e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
