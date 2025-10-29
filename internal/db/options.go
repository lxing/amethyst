package db

import "time"

type Options struct {
	DBPath                 string
	MemtableFlushThreshold int
	MaxSSTableLevel        int
	MaxBatchSize           int
	BatchTimeout           time.Duration
	BloomFilterFPR         float64
}

var DefaultOptions = Options{
	DBPath:                 "bin",
	MemtableFlushThreshold: 256,
	MaxSSTableLevel:        3,
	MaxBatchSize:           50,
	BatchTimeout:           5 * time.Millisecond,
	BloomFilterFPR:         0.01,
}

type Option func(*Options)

func WithDBPath(path string) Option {
	return func(o *Options) {
		o.DBPath = path
	}
}

func WithMemtableFlushThreshold(n int) Option {
	return func(o *Options) {
		o.MemtableFlushThreshold = n
	}
}

func WithMaxSSTableLevel(n int) Option {
	return func(o *Options) {
		o.MaxSSTableLevel = n
	}
}

func WithMaxBatchSize(n int) Option {
	return func(o *Options) {
		o.MaxBatchSize = n
	}
}

func WithBatchTimeout(d time.Duration) Option {
	return func(o *Options) {
		o.BatchTimeout = d
	}
}

func WithBloomFilterFPR(fpr float64) Option {
	return func(o *Options) {
		o.BloomFilterFPR = fpr
	}
}
