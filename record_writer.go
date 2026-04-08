package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/gzip"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/compress/uncompressed"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

type JsonWriter struct {
	cfg      *Config
	uploader *COSUploader
	mu       sync.Mutex
	rows     []map[string]interface{}
	timer    *time.Timer
	compress compress.Codec
	closing  bool
}

// NewParquetWriter 创建实例
func NewJsonWriter(cfg *Config, uploader *COSUploader) *JsonWriter {
	jw := &JsonWriter{
		cfg:      cfg,
		uploader: uploader,
		rows:     make([]map[string]interface{}, 0, cfg.BatchSize),
	}
	jw.compress = jw.compressionCodec()
	jw.resetTimer()
	return jw
}

func (jw *JsonWriter) WriteRow(row map[string]interface{}) error {
	if jw.closing {
		return fmt.Errorf("writer is closed")
	}
	jw.rows = append(jw.rows, row)
	jw.mu.Lock()
	defer jw.mu.Unlock()
	if len(jw.rows) >= jw.cfg.BatchSize {
		err := jw.flushLocked()
		return err
	}
	return nil
}

func (jw *JsonWriter) resetTimer() {
	if jw.closing {
		return
	}
	if jw.timer != nil {
		jw.timer.Stop()
	}
	d := time.Duration(jw.cfg.BatchTimeout) * time.Second
	jw.timer = time.AfterFunc(d, func() {
		jw.mu.Lock()
		defer jw.mu.Unlock()
		if len(jw.rows) > 0 {
			if err := jw.flushLocked(); err != nil {
				log.Printf("[parquet] timer flush error: %v\n", err)
			}
		}
		jw.resetTimer()
	})
}

// flushLocked 刷新缓冲区（调用时已持有锁，但会临时释放）
func (jw *JsonWriter) flushLocked() error {
	if len(jw.rows) == 0 {
		return nil
	}
	rows := jw.rows
	jw.rows = make([]map[string]interface{}, 0, jw.cfg.BatchSize)

	// 释放锁执行 IO，完成后重新加锁
	buf, err := jw.encode(rows)
	key := jw.cfg.objectKey()
	var uploadErr error
	if err == nil {
		uploadErr = jw.uploader.Upload(key, buf)
	}

	if err != nil || uploadErr != nil {
		// 失败时将数据回写队列头部，不丢数据
		jw.rows = append(rows, jw.rows...)
		if err != nil {
			return fmt.Errorf("parquet encode: %w", err)
		}
		return fmt.Errorf("cos upload: %w", uploadErr)
	}

	log.Printf("[parquet] flushed %d rows → %s (%d bytes)\n", len(rows), key, len(buf))
	return nil
}

func (jw *JsonWriter) encode(rows []map[string]interface{}) ([]byte, error) {
	var buf bytes.Buffer
	for _, row := range rows {
		val, _ := json.Marshal(row)
		buf.WriteString(string(val))
	}
	out, err := jw.compress.Encode(nil, buf.Bytes())
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (jw *JsonWriter) compressionCodec() compress.Codec {
	switch jw.cfg.Compression {
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
