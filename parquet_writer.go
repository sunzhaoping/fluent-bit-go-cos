package main

import (
	"bytes"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/gzip"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/compress/uncompressed"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

// ParquetWriter 主结构
type ParquetWriter struct {
	cfg      *Config
	uploader *COSUploader
	mu       sync.Mutex
	rows     []map[string]interface{}
	timer    *time.Timer
	closing  bool
	columns  []string
	colTypes map[string]parquet.Node
	schema   parquet.Group
}

// NewParquetWriter 创建实例
func NewParquetWriter(cfg *Config, uploader *COSUploader) *ParquetWriter {
	pw := &ParquetWriter{
		cfg:      cfg,
		uploader: uploader,
		rows:     make([]map[string]interface{}, 0, cfg.BatchSize),
		colTypes: make(map[string]parquet.Node),
		columns:  make([]string, 0, len(cfg.FieldTypes)),
	}

	pw.schema = make(parquet.Group)
	log.Printf("%s\n", strings.Repeat("#", 32))
	log.Printf("[parquet] %v\n", pw.cfg.FieldTypes)
	for col := range pw.cfg.FieldTypes {
		pw.columns = append(pw.columns, col)
		pw.colTypes[col] = pw.GetFieldType(col)
		pw.schema[col] = pw.colTypes[col]
		log.Printf("[parquet] field=%s type=%v\n", col, pw.colTypes[col])
	}
	log.Printf("%s\n", strings.Repeat("#", 32))
	pw.resetTimer()
	return pw
}

func (pw *ParquetWriter) WriteRow(row map[string]interface{}) error {
	if pw.closing {
		return fmt.Errorf("writer is closed")
	}
	pw.rows = append(pw.rows, row)
	pw.mu.Lock()
	defer pw.mu.Unlock()
	if len(pw.rows) >= pw.cfg.BatchSize {
		err := pw.flushLocked()
		return err
	}
	return nil
}

func (pw *ParquetWriter) resetTimer() {
	if pw.closing {
		return
	}
	if pw.timer != nil {
		pw.timer.Stop()
	}
	d := time.Duration(pw.cfg.BatchTimeout) * time.Second
	pw.timer = time.AfterFunc(d, func() {
		pw.mu.Lock()
		defer pw.mu.Unlock()
		if len(pw.rows) > 0 {
			if err := pw.flushLocked(); err != nil {
				log.Printf("[parquet] timer flush error: %v\n", err)
			}
		}
		pw.resetTimer()
	})
}

// flushLocked 刷新缓冲区（调用时已持有锁，但会临时释放）
func (pw *ParquetWriter) flushLocked() error {
	if len(pw.rows) == 0 {
		return nil
	}
	rows := pw.rows
	pw.rows = make([]map[string]interface{}, 0, pw.cfg.BatchSize)

	// 释放锁执行 IO，完成后重新加锁
	buf, err := pw.encode(rows)
	key := pw.objectKey()
	var uploadErr error
	if err == nil {
		uploadErr = pw.uploader.Upload(key, buf)
	}

	if err != nil || uploadErr != nil {
		// 失败时将数据回写队列头部，不丢数据
		pw.rows = append(rows, pw.rows...)
		if err != nil {
			return fmt.Errorf("parquet encode: %w", err)
		}
		return fmt.Errorf("cos upload: %w", uploadErr)
	}

	log.Printf("[parquet] flushed %d rows → %s (%d bytes)\n", len(rows), key, len(buf))
	return nil
}

// encode 编码为 Parquet 字节流
func (pw *ParquetWriter) encode(
	rows []map[string]interface{},
) ([]byte, error) {
	schema := parquet.NewSchema("root", pw.schema)
	var buf bytes.Buffer
	writer := parquet.NewWriter(
		&buf,
		schema,
		parquet.DataPageVersion(1),
		parquet.Compression(pw.compressionCodec()),
	)
	for _, row := range rows {
		builder := parquet.NewRowBuilder(pw.schema)
		for index, col := range pw.columns {
			v := row[col]
			builder.Add(index, pw.convertToParquetValue(v, col))
		}
		if err := writer.Write(builder.Row()); err != nil {
			return nil, fmt.Errorf("write row: %w", err)
		}
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close writer: %w", err)
	}

	return buf.Bytes(), nil
}

func (pw *ParquetWriter) convertToParquetValue(v interface{}, name string) parquet.Value {
	if v == nil {
		return parquet.Value{}
	}
	if t, ok := pw.cfg.FieldTypes[name]; ok {
		switch t {
		case "timestamp_nano", "timestamp_milli", "timestamp_micro":
			switch val := v.(type) {
			case int64:
				return parquet.ValueOf(val)
			case int:
				return parquet.ValueOf(int64(val))
			case float64:
				return parquet.ValueOf(int64(val))
			case string:
				if i, err := strconv.ParseInt(val, 10, 64); err == nil {
					return parquet.ValueOf(i)
				}
			}
			return parquet.NullValue()
		case "int":
			switch val := v.(type) {
			case int32:
				return parquet.ValueOf(val)
			case int:
				return parquet.ValueOf(int32(val))
			case int64:
				return parquet.ValueOf(int32(val))
			case float64:
				return parquet.ValueOf(int32(val))
			case string:
				if i, err := strconv.ParseInt(val, 10, 32); err == nil {
					return parquet.ValueOf(int32(i))
				}
			}
			return parquet.NullValue()

		case "bigint":
			switch val := v.(type) {
			case int64:
				return parquet.ValueOf(val)
			case int:
				return parquet.ValueOf(int64(val))
			case float64:
				return parquet.ValueOf(int64(val))
			case string:
				if i, err := strconv.ParseInt(val, 10, 64); err == nil {
					return parquet.ValueOf(i)
				}
			}
			return parquet.NullValue()

		case "float":
			switch val := v.(type) {
			case float32:
				return parquet.ValueOf(val)
			case float64:
				return parquet.ValueOf(float32(val))
			case int:
				return parquet.ValueOf(float32(val))
			case int64:
				return parquet.ValueOf(float32(val))
			case string:
				if f, err := strconv.ParseFloat(val, 32); err == nil {
					return parquet.ValueOf(float32(f))
				}
			}
			return parquet.NullValue()

		case "double":
			switch val := v.(type) {
			case float64:
				return parquet.ValueOf(val)
			case float32:
				return parquet.ValueOf(float64(val))
			case int:
				return parquet.ValueOf(float64(val))
			case int64:
				return parquet.ValueOf(float64(val))
			case string:
				if f, err := strconv.ParseFloat(val, 64); err == nil {
					return parquet.ValueOf(f)
				}
			}
			return parquet.NullValue()

		case "boolean":
			switch val := v.(type) {
			case bool:
				return parquet.ValueOf(val)
			case string:
				if b, err := strconv.ParseBool(val); err == nil {
					return parquet.ValueOf(b)
				}
			}
			return parquet.NullValue()
		case "string":
			switch val := v.(type) {
			case string:
				return parquet.ByteArrayValue([]byte(val))
			case []byte:
				return parquet.ByteArrayValue(val)
			default:
				s := fmt.Sprintf("%v", v)
				return parquet.ByteArrayValue([]byte(s))
			}
		}
	}
	return parquet.ByteArrayValue([]byte(fmt.Sprintf("%v", v)))
}

// inferField 推断字段类型
func (pw *ParquetWriter) GetFieldType(name string) parquet.Node {
	if t, ok := pw.cfg.FieldTypes[name]; ok {
		switch t {
		case "timestamp_nanos":
			return parquet.Optional(
				parquet.Timestamp(parquet.Nanosecond),
			)

		case "timestamp_millis":
			return parquet.Optional(
				parquet.Timestamp(parquet.Millisecond),
			)

		case "timestamp_micros":
			return parquet.Optional(
				parquet.Timestamp(parquet.Microsecond),
			)

		case "int":
			return parquet.Optional(
				parquet.Leaf(parquet.Int32Type),
			)

		case "bigint":
			return parquet.Optional(
				parquet.Leaf(parquet.Int64Type),
			)

		case "float":
			return parquet.Optional(
				parquet.Leaf(parquet.FloatType),
			)

		case "double":
			return parquet.Optional(
				parquet.Leaf(parquet.DoubleType),
			)
		case "boolean":
			return parquet.Optional(
				parquet.Leaf(parquet.BooleanType),
			)

		case "string":
			return parquet.Optional(
				parquet.String(),
			)

		case "uuid":
			return parquet.Optional(
				parquet.UUID(),
			)

		case "json":
			return parquet.Optional(
				parquet.JSON(),
			)

		case "bytearray":
			return parquet.Optional(
				parquet.Leaf(parquet.ByteArrayType),
			)

		case "date":
			return parquet.Optional(
				parquet.Date(),
			)
		}
	}
	return parquet.Optional(parquet.String())
}

// objectKey 生成对象键
func (pw *ParquetWriter) objectKey() string {
	loc, err := time.LoadLocation(pw.cfg.TimeZone)
	if err != nil {
		panic(err)
	}
	now := time.Now().In(loc)

	key := pw.cfg.PathPrefix
	replacements := map[string]string{
		"%Y": fmt.Sprintf("%04d", now.Year()),
		"%m": fmt.Sprintf("%02d", now.Month()),
		"%d": fmt.Sprintf("%02d", now.Day()),
		"%H": fmt.Sprintf("%02d", now.Hour()),
		"%M": fmt.Sprintf("%02d", now.Minute()),
		"%S": fmt.Sprintf("%02d", now.Second()),
	}
	for k, v := range replacements {
		key = strings.ReplaceAll(key, k, v)
	}

	filename := uuid.New().String() + ".parquet"
	return key + filename
}

// compressionCodec 压缩编解码器
func (pw *ParquetWriter) compressionCodec() compress.Codec {
	switch pw.cfg.Compression {
	case "gzip":
		return &gzip.Codec{}
	case "zstd":
		return &zstd.Codec{}
	case "none", "uncompressed":
		return &uncompressed.Codec{}
	default:
		return &snappy.Codec{}
	}
}

// Close 关闭并刷新
func (pw *ParquetWriter) Close() error {
	pw.mu.Lock()
	defer pw.mu.Unlock()
	pw.closing = true
	if pw.timer != nil {
		pw.timer.Stop()
	}
	if len(pw.rows) == 0 {
		return nil
	}
	// flushLocked 内部会 Unlock 再 Lock，结束时持有锁
	err := pw.flushLocked()
	return err
}
