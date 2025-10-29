package db

import "time"

type Options struct {
	MemtableFlushThreshold int
	MaxSSTableLevel        int
	MaxBatchSize           int
	BatchTimeout           time.Duration
}

var DefaultOptions = Options{
	MemtableFlushThreshold: 256,
	MaxSSTableLevel:        3,
	MaxBatchSize:           50,
	BatchTimeout:           5 * time.Millisecond,
}

type Option func(*Options)

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
