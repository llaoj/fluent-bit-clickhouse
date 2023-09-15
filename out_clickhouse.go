package main

import (
	"C"
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/sirupsen/logrus"
)

type Clickhouse struct {
	logger     *logrus.Logger
	hosts      []string
	database   string
	username   string
	password   string
	table      string
	columns    []string
	conn       driver.Conn
	insertSQL  string
	buffer     []map[interface{}]interface{}
	bufferSize int
}

const InsertSQLExpr = "INSERT INTO %s (%s) VALUES (%s)"

var config = make(map[string]*Clickhouse, 0)

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	return output.FLBPluginRegister(def, "clickhouse", "Clickhouse Output Plugin.")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	ch := &Clickhouse{
		logger: logrus.New(),
	}

	id := output.FLBPluginConfigKey(plugin, "id")
	if id != "" {
		ch.logger.Infof("plugin clickhouse context id = %s", id)
		output.FLBPluginSetContext(plugin, id)
	} else {
		ch.logger.Error("you must set id of clickhouse!")
		return output.FLB_ERROR
	}

	logLevel := output.FLBPluginConfigKey(plugin, "clickhouse_log_level")
	if logLevel != "" {
		ch.logger.Infof("plugin clickhouse_log_level = %s", logLevel)
		ch.logger = initLogger(id, logLevel)
	} else {
		ch.logger.Error("you must set log_level of clickhouse!")
		return output.FLB_ERROR
	}

	hosts := output.FLBPluginConfigKey(plugin, "clickhouse_hosts")
	if hosts != "" {
		ch.logger.Infof("plugin clickhouse_hosts = %s", hosts)
		ch.hosts = strings.Split(hosts, ",")
	} else {
		ch.logger.Error("you must set hosts of clickhouse!")
		return output.FLB_ERROR
	}

	database := output.FLBPluginConfigKey(plugin, "clickhouse_database")
	if database != "" {
		ch.database = database
		ch.logger.Infof("plugin clickhouse_database = %s", ch.database)
	} else {
		ch.logger.Error("you must set database of clickhouse!")
		return output.FLB_ERROR
	}

	username := output.FLBPluginConfigKey(plugin, "clickhouse_username")
	if username != "" {
		ch.username = username
		ch.logger.Infof("plugin clickhouse_username = %s", ch.username)
	} else {
		ch.logger.Error("you must set username of clickhouse!")
		return output.FLB_ERROR
	}

	password := output.FLBPluginConfigKey(plugin, "clickhouse_password")
	if password != "" {
		ch.password = password
		ch.logger.Infof("plugin clickhouse_password = %s", ch.password)
	} else {
		ch.logger.Error("you must set password of clickhouse!")
		return output.FLB_ERROR
	}

	table := output.FLBPluginConfigKey(plugin, "clickhouse_table")
	if table != "" {
		ch.table = table
		ch.logger.Infof("plugin clickhouse_table = %s", ch.table)
	} else {
		ch.logger.Error("you must set table of clickhouse!")
		return output.FLB_ERROR
	}

	columns := output.FLBPluginConfigKey(plugin, "clickhouse_columns")
	if columns != "" {
		ch.columns = strings.Split(columns, ",")
		ch.logger.Infof("plugin clickhouse_columns = %s", ch.columns)
		// init insertSQL
		ch.insertSQL = formatInsertSQL(ch.columns, ch.table)
		ch.logger.Infof("insertSQL = %s", ch.insertSQL)
	} else {
		ch.logger.Errorf("you must set columns of %v!", ch.table)
		return output.FLB_ERROR
	}

	bufferSize := output.FLBPluginConfigKey(plugin, "clickhouse_buffer_size")
	if bufferSize != "" {
		intBufferSize, err := strconv.Atoi(bufferSize)
		if err != nil {
			ch.logger.Errorf("convert clickhouse_buffer_size to int error: %v!", err)
		}
		ch.bufferSize = intBufferSize
		ch.logger.Infof("plugin clickhouse_buffer_size = %v", ch.bufferSize)
	}

	// init conn
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: ch.hosts,
		Auth: clickhouse.Auth{
			Database: ch.database,
			Username: ch.username,
			Password: ch.password,
		},
		DialContext: func(ctx context.Context, addr string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr)
		},
		Debug: true,
		Debugf: func(format string, v ...any) {
			ch.logger.Debugf(format, v)
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		DialTimeout:      time.Duration(10) * time.Second,
		MaxOpenConns:     5,
		MaxIdleConns:     5,
		ConnMaxLifetime:  time.Duration(10) * time.Minute,
		ConnOpenStrategy: clickhouse.ConnOpenInOrder,
		BlockBufferSize:  10,
	})
	if err != nil {
		ch.logger.Errorf("failed to open clickhouse: %v", err)
		return output.FLB_ERROR
	}
	err = conn.Ping(context.Background())
	if err != nil {
		ch.logger.Errorf("failed to ping clickhouse: %v", err)
		return output.FLB_ERROR
	}
	ch.conn = conn

	ch.buffer = make([]map[interface{}]interface{}, 0)
	config[id] = ch

	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	id := output.FLBPluginGetContext(ctx).(string)
	ch, ok := config[id]
	if !ok {
		return output.FLB_ERROR
	}
	ch.logger.Infof("flush called for id = %s", id)

	// create Fluent Bit decoder
	dec := output.NewDecoder(data, int(length))

	// extract record and buffer
	for {
		ret, _, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}
		ch.buffer = append(ch.buffer, record)
	}
	// sink data
	if len(ch.buffer) < ch.bufferSize {
		return output.FLB_OK
	}

	// post them to db all at once
	start := time.Now()
	batch, err := ch.conn.PrepareBatch(context.Background(), ch.insertSQL)
	if err != nil {
		ch.logger.Errorf("prepare batch failure: %v", err)
		return output.FLB_OK
	}
	for _, record := range ch.buffer {
		values := fieldValues(record, ch.columns)
		ch.logger.Tracef("values: %v", values)
		err = batch.Append(values...)
		if err != nil {
			ch.logger.Errorf("batch append failure: %v", err)
			return output.FLB_OK
		}
	}
	err = batch.Send()
	if err != nil {
		ch.logger.Errorf("batch send failure: %v", err)
		return output.FLB_OK
	}
	end := time.Now()
	ch.logger.Infof("exported %d logs to clickhouse in %s", len(ch.buffer), end.Sub(start))

	// clear buffer
	ch.buffer = make([]map[interface{}]interface{}, 0)

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

func initLogger(id, logLevel string) *logrus.Logger {
	logger := logrus.New()
	logger.SetOutput(os.Stdout)
	logger.WithFields(logrus.Fields{"id": id})
	// trace, debug, info, warning, error, fatal and panic
	switch logLevel {
	case "trace":
		logger.SetLevel(logrus.TraceLevel)
	case "debug":
		logger.SetLevel(logrus.DebugLevel)
	case "info":
		logger.SetLevel(logrus.InfoLevel)
	case "warning":
		logger.SetLevel(logrus.WarnLevel)
	case "error":
		logger.SetLevel(logrus.ErrorLevel)
	case "fatal":
		logger.SetLevel(logrus.FatalLevel)
	case "panic":
		logger.SetLevel(logrus.PanicLevel)
	default:
		logger.SetLevel(logrus.WarnLevel)
	}
	return logger
}

func formatInsertSQL(columns []string, table string) string {
	values := ""
	for i := range columns {
		values += "?"
		if i < len(columns)-1 {
			values += ","
		}
	}
	return fmt.Sprintf(InsertSQLExpr, table, strings.Join(columns, ","), values)
}

func fieldValues(record map[interface{}]interface{}, fields []string) (values []interface{}) {
	values = make([]interface{}, len(fields))
	var row = make(map[string]interface{})
	for k, v := range record {
		row[k.(string)] = v
	}
	for index, field := range fields {
		if v, ok := row[field]; ok {
			switch tv := v.(type) {
			case string:
				values[index] = tv
			case []byte:
				values[index] = string(tv)
			default:
				values[index] = fmt.Sprintf("%v", v)
			}
		} else {
			values[index] = nil
		}
	}
	return
}

func main() {
}
