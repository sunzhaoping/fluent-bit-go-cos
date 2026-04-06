package main

import (
	"fmt"
	"strconv"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
)

// Config holds all plugin configuration parameters
type Config struct {
	// COS settings
	Role       string
	Region     string
	BucketName string
	PathPrefix string

	// Parquet / batching settings
	BatchSize    int    // number of records per parquet file
	BatchTimeout int    // seconds before flushing incomplete batch
	Compression  string // snappy | gzip | zstd | none

	// Schema settings
	TimestampField string // name of the timestamp column
}

// PluginContext carries per-instance state
type PluginContext struct {
	cfg      *Config
	writer   *ParquetWriter
	uploader *COSUploader
}

// NewPluginContext reads config from the Fluent Bit plugin handle and wires up subsystems.
func NewPluginContext(plugin unsafe.Pointer) (*PluginContext, error) {
	cfg, err := loadConfig(plugin)
	if err != nil {
		return nil, fmt.Errorf("config error: %w", err)
	}
	uploader := NewCOSUploader(cfg.Role, cfg.BucketName, cfg.Region)
	writer := NewParquetWriter(cfg, uploader)

	return &PluginContext{
		cfg:      cfg,
		writer:   writer,
		uploader: uploader,
	}, nil
}

// Flush is called by Fluent Bit whenever records are ready to be sent.
func (p *PluginContext) Flush(data unsafe.Pointer, length int, tag string) int {
	dec := output.NewDecoder(data, length)

	for {
		ret, ts, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}

		// Convert the Fluent Bit map to a plain Go map[string]interface{}
		row := make(map[string]interface{}, len(record)+1)
		for k, v := range record {
			row[fmt.Sprintf("%v", k)] = v
		}

		// Attach timestamp
		row[p.cfg.TimestampField] = fluentTimestampToUnixMilli(ts)
		row["_tag"] = tag

		if err := p.writer.WriteRow(row); err != nil {
			fmt.Printf("[cos_parquet] WriteRow error: %v\n", err)
			return output.FLB_RETRY
		}
	}

	return output.FLB_OK
}

// loadConfig reads all plugin parameters with sensible defaults.
func loadConfig(plugin unsafe.Pointer) (*Config, error) {
	cfg := &Config{
		BatchSize:      10000,
		BatchTimeout:   60,
		Compression:    "snappy",
		TimestampField: "timestamp",
		PathPrefix:     "fluent-bit/",
	}

	get := func(key string) string {
		return output.FLBPluginConfigKey(plugin, key)
	}

	if v := get("Role"); v != "" {
		cfg.Role = v
	} else {
		return nil, fmt.Errorf("SecretID is required")
	}

	if v := get("Region"); v != "" {
		cfg.Region = v
	} else {
		return nil, fmt.Errorf("Region is required")
	}

	if v := get("BucketName"); v != "" {
		cfg.BucketName = v
	} else {
		return nil, fmt.Errorf("BucketName is required")
	}

	if v := get("PathPrefix"); v != "" {
		cfg.PathPrefix = v
	}

	if v := get("Compression"); v != "" {
		cfg.Compression = v
	}

	if v := get("TimestampField"); v != "" {
		cfg.TimestampField = v
	}

	if v := get("BatchSize"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("invalid BatchSize: %s", v)
		}
		cfg.BatchSize = n
	}

	if v := get("BatchTimeout"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("invalid BatchTimeout: %s", v)
		}
		cfg.BatchTimeout = n
	}

	return cfg, nil
}

// fluentTimestampToUnixMilli converts a Fluent Bit FLBTime (or plain time.Time) to Unix milliseconds.
func fluentTimestampToUnixMilli(ts interface{}) int64 {
	switch v := ts.(type) {
	case output.FLBTime:
		return v.Time.UnixMilli()
	default:
		return 0
	}
}
