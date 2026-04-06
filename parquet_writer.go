package main

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"time"

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

	mu        sync.Mutex
	rows      []map[string]interface{}
	schema    []string
	schemaSet map[string]struct{}
	timer     *time.Timer
}

// NewParquetWriter 创建实例
func NewParquetWriter(cfg *Config, uploader *COSUploader) *ParquetWriter {
	pw := &ParquetWriter{
		cfg:       cfg,
		uploader:  uploader,
		rows:      make([]map[string]interface{}, 0, cfg.BatchSize),
		schemaSet: make(map[string]struct{}),
	}
	pw.resetTimer()
	return pw
}

// WriteRow 写入一行
func (pw *ParquetWriter) WriteRow(row map[string]interface{}) error {
	pw.mu.Lock()
	defer pw.mu.Unlock()

	for k := range row {
		if _, ok := pw.schemaSet[k]; !ok {
			pw.schemaSet[k] = struct{}{}
			pw.schema = append(pw.schema, k)
		}
	}

	pw.rows = append(pw.rows, row)

	if len(pw.rows) >= pw.cfg.BatchSize {
		return pw.flushLocked()
	}
	return nil
}

// resetTimer 重置定时器
func (pw *ParquetWriter) resetTimer() {
	if pw.timer != nil {
		pw.timer.Stop()
	}
	d := time.Duration(pw.cfg.BatchTimeout) * time.Second
	pw.timer = time.AfterFunc(d, func() {
		pw.mu.Lock()
		defer pw.mu.Unlock()
		if len(pw.rows) > 0 {
			if err := pw.flushLocked(); err != nil {
				fmt.Printf("[parquet] timer flush error: %v\n", err)
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
	schema := pw.sortedSchema()
	pw.rows = make([]map[string]interface{}, 0, pw.cfg.BatchSize)

	pw.mu.Unlock()
	defer pw.mu.Lock()

	buf, err := pw.encode(rows, schema)
	if err != nil {
		return fmt.Errorf("parquet encode: %w", err)
	}

	key := pw.objectKey()
	if err := pw.uploader.Upload(key, buf); err != nil {
		return fmt.Errorf("cos upload: %w", err)
	}

	fmt.Printf("[parquet] flushed %d rows → %s (%d bytes)\n", len(rows), key, len(buf))
	return nil
}

// encode 编码为 Parquet 字节流
func (pw *ParquetWriter) encode(rows []map[string]interface{}, columns []string) ([]byte, error) {
	root := make(parquet.Group)
	for _, col := range columns {
		root[col] = inferField(col, rows)
	}

	schema := parquet.NewSchema("record", parquet.Group{
		"A": parquet.Optional(parquet.Decimal(0, 38, parquet.FixedLenByteArrayType(16))),
	})

	var buf bytes.Buffer
	writer := parquet.NewWriter(
		&buf,
		schema,
		parquet.DataPageVersion(1),
		parquet.Compression(pw.compressionCodec()),
	)

	for _, row := range rows {
		parquetRow := make(parquet.Row, 0, len(columns))
		for _, col := range columns {
			v := row[col]
			defLevel := 1 // 类型为 int
			if v == nil {
				defLevel = 0
			}
			// 关键修正：defLevel 已经是 int，直接使用
			parquetRow = append(parquetRow, parquet.ValueOf(normalize(v)).Level(0, defLevel, 0))
		}
		if _, err := writer.WriteRows([]parquet.Row{parquetRow}); err != nil {
			_ = writer.Close()
			return nil, err
		}
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// inferField 推断字段类型（使用最新的 API）
func inferField(name string, rows []map[string]interface{}) parquet.Node {
	for _, row := range rows {
		if v, ok := row[name]; ok && v != nil {
			switch v.(type) {
			case int, int8, int16, int32, int64:
				return parquet.Optional(parquet.Leaf(parquet.Int64Type))
			case uint, uint8, uint16, uint32, uint64:
				return parquet.Optional(parquet.Leaf(parquet.Int64Type))
			case float32, float64:
				return parquet.Optional(parquet.Leaf(parquet.DoubleType))
			case bool:
				return parquet.Optional(parquet.Leaf(parquet.BooleanType))
			default:
				return parquet.Optional(parquet.Leaf(parquet.ByteArrayType))
			}
		}
	}
	return parquet.Optional(parquet.Leaf(parquet.ByteArrayType))
}

// normalize 类型归一化
func normalize(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case []byte:
		return val
	case string:
		return []byte(val)
	case int:
		return int64(val)
	case int8:
		return int64(val)
	case int16:
		return int64(val)
	case int32:
		return int64(val)
	case uint:
		return int64(val)
	case uint8:
		return int64(val)
	case uint16:
		return int64(val)
	case uint32:
		return int64(val)
	case uint64:
		return int64(val)
	case float32:
		return float64(val)
	default:
		return []byte(fmt.Sprintf("%v", val))
	}
}

// sortedSchema 排序字段
func (pw *ParquetWriter) sortedSchema() []string {
	cols := make([]string, 0, len(pw.schema))
	hasTSField := false
	for _, c := range pw.schema {
		if c == pw.cfg.TimestampField {
			hasTSField = true
			continue
		}
		cols = append(cols, c)
	}
	sort.Strings(cols)
	if hasTSField {
		cols = append([]string{pw.cfg.TimestampField}, cols...)
	}
	return cols
}

// objectKey 生成对象键
func (pw *ParquetWriter) objectKey() string {
	now := time.Now().UTC()
	return fmt.Sprintf("%s%d/%02d/%02d/%s.parquet",
		pw.cfg.PathPrefix,
		now.Year(), now.Month(), now.Day(),
		now.Format("20060102T150405.000000000Z"),
	)
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
	if pw.timer != nil {
		pw.timer.Stop()
	}
	if len(pw.rows) > 0 {
		return pw.flushLocked()
	}
	return nil
}
