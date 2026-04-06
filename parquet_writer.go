package main

import (
	"bytes"
	"fmt"
	"sort"
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
		root[col] = pw.inferField(col, rows)
	}

	schema := parquet.NewSchema("record", root)

	var buf bytes.Buffer
	writer := parquet.NewWriter(
		&buf,
		schema,
		parquet.DataPageVersion(2),
		parquet.Compression(pw.compressionCodec()),
	)
	defer writer.Close()

	for _, row := range rows {
		parquetRow := make(parquet.Row, 0, len(columns))
		for colIdx, col := range columns {
			v := row[col]
			if v == nil {
				parquetRow = append(parquetRow, parquet.ValueOf(nil).Level(0, 0, colIdx))
			} else if pw.cfg.isTimestamp(col) {
				t := normalize(v).(int64)
				parquetRow = append(parquetRow, parquet.ValueOf(t).Level(0, 1, colIdx))
			} else {
				parquetRow = append(parquetRow, parquet.ValueOf(normalize(v)).Level(0, 1, colIdx))
			}
		}
		if _, err := writer.WriteRows([]parquet.Row{parquetRow}); err != nil {
			return nil, err
		}
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// inferField 推断字段类型（使用最新的 API）
func (pw *ParquetWriter) inferField(name string, rows []map[string]interface{}) parquet.Node {
	for _, row := range rows {
		if v, ok := row[name]; ok && v != nil {
			if pw.cfg.isTimestamp(name) {
				return parquet.Optional(parquet.Timestamp(parquet.Millisecond))
			}
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
	hasSortedField := false
	for _, c := range pw.schema {
		if c == pw.cfg.SortedField {
			hasSortedField = true
			continue
		}
		cols = append(cols, c)
	}
	sort.Strings(cols)
	if hasSortedField {
		cols = append([]string{pw.cfg.SortedField}, cols...)
	}
	return cols
}

// objectKey 生成对象键
func (pw *ParquetWriter) objectKey() string {
	loc, err := time.LoadLocation(pw.cfg.TimeZone)
	if err != nil {
		panic(err)
	}
	now := time.Now().In(loc)

	// 先替换常用占位符
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
	if pw.timer != nil {
		pw.timer.Stop()
	}
	if len(pw.rows) > 0 {
		return pw.flushLocked()
	}
	return nil
}
