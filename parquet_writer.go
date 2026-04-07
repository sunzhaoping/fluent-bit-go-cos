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
	closing   bool

	// 列定义锁定：第一次 flush 后不再变更
	colOrder     []string
	colTypes     map[string]parquet.Node
	schemaFrozen bool
}

// NewParquetWriter 创建实例
func NewParquetWriter(cfg *Config, uploader *COSUploader) *ParquetWriter {
	pw := &ParquetWriter{
		cfg:       cfg,
		uploader:  uploader,
		rows:      make([]map[string]interface{}, 0, cfg.BatchSize),
		schemaSet: make(map[string]struct{}),
		colTypes:  make(map[string]parquet.Node),
	}
	pw.resetTimer()
	return pw
}

// WriteRow 写入一行
func (pw *ParquetWriter) WriteRow(row map[string]interface{}) error {
	pw.mu.Lock()
	if pw.closing {
		pw.mu.Unlock()
		return fmt.Errorf("writer is closed")
	}

	// schema 锁定前才登记新列
	if !pw.schemaFrozen {
		for k := range row {
			if _, ok := pw.schemaSet[k]; !ok {
				pw.schemaSet[k] = struct{}{}
				pw.schema = append(pw.schema, k)
			}
		}
	}

	pw.rows = append(pw.rows, row)

	if len(pw.rows) >= pw.cfg.BatchSize {
		err := pw.flushLocked() // 结束时持有锁
		pw.mu.Unlock()          // 显式解锁
		return err
	}
	pw.mu.Unlock()
	return nil
}

// resetTimer 重置定时器
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
				fmt.Printf("[parquet] timer flush error: %v\n", err)
			}
		}
		pw.resetTimer()
	})
}

// buildSchema 第一次 flush 时锁定列顺序和类型，之后不再变更
func (pw *ParquetWriter) buildSchema(rows []map[string]interface{}) {
	// 列顺序：SortedField 置首，其余字母序
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
	pw.colOrder = cols

	// 基于第一批数据推断并锁定每列类型
	for _, col := range pw.colOrder {
		pw.colTypes[col] = pw.inferField(col, rows)
	}

	pw.schemaFrozen = true
}

// flushLocked 刷新缓冲区（调用时已持有锁，但会临时释放）
func (pw *ParquetWriter) flushLocked() error {
	if len(pw.rows) == 0 {
		return nil
	}

	rows := pw.rows
	pw.rows = make([]map[string]interface{}, 0, pw.cfg.BatchSize)

	// 第一次 flush 时锁定 schema
	if !pw.schemaFrozen {
		pw.buildSchema(rows)
	}

	// 快照列定义（encode 在无锁阶段执行，不能直接引用 pw 字段）
	columns := make([]string, len(pw.colOrder))
	copy(columns, pw.colOrder)
	colTypes := make(map[string]parquet.Node, len(pw.colTypes))
	for k, v := range pw.colTypes {
		colTypes[k] = v
	}

	// 释放锁执行 IO，完成后重新加锁
	pw.mu.Unlock()
	buf, err := pw.encode(rows, columns, colTypes)
	key := pw.objectKey()
	var uploadErr error
	if err == nil {
		uploadErr = pw.uploader.Upload(key, buf)
	}
	pw.mu.Lock()

	if err != nil || uploadErr != nil {
		// 失败时将数据回写队列头部，不丢数据
		pw.rows = append(rows, pw.rows...)
		if err != nil {
			return fmt.Errorf("parquet encode: %w", err)
		}
		return fmt.Errorf("cos upload: %w", uploadErr)
	}

	fmt.Printf("[parquet] flushed %d rows → %s (%d bytes)\n", len(rows), key, len(buf))
	return nil
}

// encode 编码为 Parquet 字节流
func (pw *ParquetWriter) encode(
	rows []map[string]interface{},
	columns []string,
	colTypes map[string]parquet.Node,
) ([]byte, error) {
	root := make(parquet.Group)
	for _, col := range columns {
		root[col] = colTypes[col]
	}

	schema := parquet.NewSchema("record", root)

	var buf bytes.Buffer
	writer := parquet.NewWriter(
		&buf,
		schema,
		parquet.DataPageVersion(1),
		parquet.Compression(pw.compressionCodec()),
	)

	for _, row := range rows {
		parquetRow := make(parquet.Row, 0, len(columns))
		for i, col := range columns {
			v := row[col]
			var val parquet.Value

			if v == nil {
				// null：definition level = 0
				val = parquet.Value{}.Level(0, 0, i)
			} else if pw.cfg.isTimestamp(col) {
				val = parquet.ValueOf(normalize(v).(int64)).Level(0, 1, i)
			} else {
				val = parquet.ValueOf(normalize(v)).Level(0, 1, i)
			}

			parquetRow = append(parquetRow, val)
		}

		if _, err := writer.WriteRows([]parquet.Row{parquetRow}); err != nil {
			return nil, fmt.Errorf("write row: %w", err)
		}
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close writer: %w", err)
	}
	return buf.Bytes(), nil
}

// inferField 推断字段类型
func (pw *ParquetWriter) inferField(name string, rows []map[string]interface{}) parquet.Node {
	for _, row := range rows {
		if v, ok := row[name]; ok && v != nil {
			if pw.cfg.isTimestamp(name) {
				return parquet.Optional(parquet.Timestamp(parquet.Millisecond))
			}
			switch v.(type) {
			case int, int8, int16, int32, uint, uint8, uint16:
				return parquet.Optional(parquet.Leaf(parquet.Int32Type))
			case uint32, int64:
				return parquet.Optional(parquet.Leaf(parquet.Int64Type))
			case uint64:
				return parquet.Optional(parquet.Leaf(parquet.Int96Type))
			case float32:
				return parquet.Optional(parquet.Leaf(parquet.FloatType))
			case float64:
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
		return int(val)
	case int8:
		return int(val)
	case int16:
		return int(val)
	case int32:
		return int(val)
	case uint:
		return int64(val)
	case uint8:
		return int32(val)
	case uint16:
		return int32(val)
	case uint32:
		return int64(val)
	case uint64:
		return val
	case float32:
		return float32(val)
	case bool:
		return val
	default:
		return []byte(fmt.Sprintf("%v", val))
	}
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
	pw.closing = true
	if pw.timer != nil {
		pw.timer.Stop()
	}
	if len(pw.rows) == 0 {
		pw.mu.Unlock()
		return nil
	}
	// flushLocked 内部会 Unlock 再 Lock，结束时持有锁
	err := pw.flushLocked()
	pw.mu.Unlock() // 显式解锁
	return err
}
