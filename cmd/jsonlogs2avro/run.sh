#!/bin/sh

export ENV_TIME_KEY=time
export ENV_LEVEL_KEY=level
export ENV_BODY_KEY=msg

cat ./sample.d/sample.jsonl |
	./jsonlogs2avro |
	~/avro2jsons
