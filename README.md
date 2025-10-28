**Option A: Build the Core of a High-Throughput, In-Memory Time-Series Database (TSDB)**

Description: We need to build the core of a high-throughput, in-memory, time-series database (TSDB) in Go or Java.
Core Requirements:
Data Model: A "metric" consists of a name (string), tags (a map of key/value strings), a timestamp (int64, Unix epoch milliseconds), and a value (float64).
Write Function: Design a function Write(metric Metric) that efficiently ingests a new data point. This function must be safe for concurrent calls.
Query Function: Design a function Query(name string, tags map[string]string, start int64, end int64) that retrieves all data points for a series matching the name and all specified tags within the time range.
