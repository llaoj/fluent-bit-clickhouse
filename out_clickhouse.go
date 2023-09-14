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

var (
	logger *logrus.Logger

	hosts    []string
	database string
	username string
	password string
	table    string
	columns  []string
	conn     driver.Conn

	insertSQL  = "INSERT INTO %s (%s) VALUES (%s)"
	buffer     = make([]map[interface{}]interface{}, 0)
	bufferSize = 1024
)

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	return output.FLBPluginRegister(def, "clickhouse", "Clickhouse Output Plugin.")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	id := output.FLBPluginConfigKey(plugin, "id")
	logger.Infof("[multiinstance] id = %q", id)

	logLevel := output.FLBPluginConfigKey(plugin, "clickhouse_log_level")
	if logLevel != "" {
		initLogger(logLevel)
		logger.Infof("plugin clickhouse_log_level = %s", logLevel)
	} else {
		logger.Error("you must set log_level of clickhouse!")
		return output.FLB_ERROR
	}

	clickhouseHosts := output.FLBPluginConfigKey(plugin, "clickhouse_hosts")
	if clickhouseHosts != "" {
		logger.Infof("plugin clickhouse_hosts = %s", clickhouseHosts)
		hosts = strings.Split(clickhouseHosts, ",")
	} else {
		logger.Error("you must set hosts of clickhouse!")
		return output.FLB_ERROR
	}

	database = output.FLBPluginConfigKey(plugin, "clickhouse_database")
	if database != "" {
		logger.Infof("plugin clickhouse_database = %s", database)
	} else {
		logger.Error("you must set database of clickhouse!")
		return output.FLB_ERROR
	}

	username = output.FLBPluginConfigKey(plugin, "clickhouse_username")
	if username != "" {
		logger.Infof("plugin clickhouse_username = %s", username)
	} else {
		logger.Error("you must set username of clickhouse!")
		return output.FLB_ERROR
	}

	password = output.FLBPluginConfigKey(plugin, "clickhouse_password")
	if password != "" {
		logger.Infof("plugin clickhouse_password = %s", password)
	} else {
		logger.Error("you must set password of clickhouse!")
		return output.FLB_ERROR
	}

	table = output.FLBPluginConfigKey(plugin, "clickhouse_table")
	if table != "" {
		logger.Infof("plugin clickhouse_table = %s", table)
	} else {
		logger.Error("you must set table of clickhouse!")
		return output.FLB_ERROR
	}

	clickhouseColumns := output.FLBPluginConfigKey(plugin, "clickhouse_columns")
	if clickhouseColumns != "" {
		logger.Infof("plugin clickhouse_columns = %s", clickhouseColumns)
		// init insertSQL
		columns = strings.Split(clickhouseColumns, ",")
		insertSQL = formatInsertSQL(columns)
		logger.Trace(insertSQL)
	} else {
		logger.Errorf("you must set columns of %v!", table)
		return output.FLB_ERROR
	}

	var err error
	clickhouseBufferSize := output.FLBPluginConfigKey(plugin, "clickhouse_buffer_size")
	if clickhouseBufferSize != "" {
		logger.Infof("plugin clickhouse_buffer_size = %s", clickhouseBufferSize)
		bufferSize, err = strconv.Atoi(clickhouseBufferSize)
		if err != nil {
			logger.Errorf("convert clickhouse_buffer_size to int error: %v!", err)
		}
	}

	// init conn
	conn, err = clickhouse.Open(&clickhouse.Options{
		Addr: hosts,
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		DialContext: func(ctx context.Context, addr string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr)
		},
		Debug: true,
		Debugf: func(format string, v ...any) {
			logger.Debugf(format, v)
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
		logger.Errorf("failed to open clickhouse: %v", err)
		return output.FLB_ERROR
	}
	err = conn.Ping(context.Background())
	if err != nil {
		logger.Errorf("failed to ping clickhouse: %v", err)
		return output.FLB_ERROR
	}

	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	id := output.FLBPluginGetContext(ctx).(string)
	logger.Infof("[multiinstance] Flush called for id: %s", id)

	// create Fluent Bit decoder
	dec := output.NewDecoder(data, int(length))

	// extract record and buffer
	for {
		ret, _, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}
		buffer = append(buffer, record)
	}
	// sink data
	if len(buffer) < bufferSize {
		return output.FLB_OK
	}

	// post them to db all at once
	start := time.Now()
	batch, err := conn.PrepareBatch(context.Background(), insertSQL)
	if err != nil {
		logger.Errorf("prepare batch failure: %v", err)
		return output.FLB_OK
	}
	for _, record := range buffer {
		values := fieldValues(record, columns)
		logger.Tracef("values: %v", values)
		err = batch.Append(values...)
		if err != nil {
			logger.Errorf("batch append failure: %v", err)
			return output.FLB_OK
		}
	}
	err = batch.Send()
	if err != nil {
		logger.Errorf("batch send failure: %v", err)
		return output.FLB_OK
	}
	end := time.Now()
	logger.Infof("exported %d logs to clickhouse in %s", len(buffer), end.Sub(start))

	// clear buffer
	buffer = make([]map[interface{}]interface{}, 0)

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

func initLogger(logLevel string) {
	logger = logrus.New()
	logger.SetOutput(os.Stdout)
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
}

func formatInsertSQL(columns []string) string {
	values := ""
	for i := range columns {
		values += "?"
		if i < len(columns)-1 {
			values += ","
		}
	}
	return fmt.Sprintf(insertSQL, table, strings.Join(columns, ","), values)
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
