package main

import (
	"sort"
	"strings"
	"sync"
)

/*
Option A: Build the Core of a High-Throughput, In-Memory Time-Series Database (TSDB)

Description: We need to build the core of a high-throughput, in-memory, time-series database (TSDB) in Go or Java.
Core Requirements:
Data Model: A "metric" consists of a name (string), tags (a map of key/value strings), a timestamp (int64, Unix epoch milliseconds), and a value (float64).
Write Function: Design a function Write(metric Metric) that efficiently ingests a new data point. This function must be safe for concurrent calls.
Query Function: Design a function Query(name string, tags map[string]string, start int64, end int64) that retrieves all data points for a series matching the name and all specified tags within the time range.
*/

type metric struct {
	name      string
	value     float64
	timeStamp int64
	tags      map[string]string
}

type timeSeries struct {
	series map[string]*series
	mu     sync.RWMutex
}

type series struct {
	dataPoints []*metric
	mu         sync.RWMutex
}

func getSeriesIdentifier(name string, tags map[string]string) string {
	tagsArray := make([]string, 0, len(tags))
	for k, v := range tags {
		tagsArray = append(tagsArray, k+"="+v)
	}
	sort.Strings(tagsArray)
	return name + "|" + strings.Join(tagsArray, "|")
}

func (s *timeSeries) Write(name string, tags map[string]string, value float64, timestamp int64) {
	seriesidentifier := getSeriesIdentifier(name, tags)
	// check if series exist already or not
	dataPoint := &metric{
		name:      name,
		value:     value,
		timeStamp: timestamp,
		tags:      tags,
	}
	timeseries, ok := s.series[seriesidentifier]
	if !ok {
		s.mu.Lock()
		timeseries, ok := s.series[seriesidentifier]
		if !ok {
			// check for last element and do insertion
			timeseries.mu.Lock()
			latestDataPoint := timeseries.dataPoints[len(timeseries.dataPoints)-1]
			if latestDataPoint.timeStamp < timestamp {
				timeseries.dataPoints = append(timeseries.dataPoints, dataPoint)
			} else {
				// do binary search to figure out insertion point
				indexToInsertAt := sort.Search(len(timeseries.dataPoints), func(i int) bool {
					return timeseries.dataPoints[i].timeStamp >= timestamp
				})
				timeseries.dataPoints = append(timeseries.dataPoints, dataPoint)
				copy(timeseries.dataPoints[indexToInsertAt+1:], timeseries.dataPoints[indexToInsertAt:])
				timeseries.dataPoints[indexToInsertAt] = latestDataPoint
			}
			timeseries.mu.Unlock()
		}
		s.series[seriesidentifier] = &series{
			dataPoints: []*metric{dataPoint},
		}
		s.mu.Unlock()
	} else {
		// check for last element and do insertion
		timeseries.mu.Lock()
		latestDataPoint := timeseries.dataPoints[len(timeseries.dataPoints)-1]
		if latestDataPoint.timeStamp < timestamp {
			timeseries.dataPoints = append(timeseries.dataPoints, dataPoint)
		} else {
			// do binary search to figure out insertion point
			indexToInsertAt := sort.Search(len(timeseries.dataPoints), func(i int) bool {
				return timeseries.dataPoints[i].timeStamp >= timestamp
			})
			timeseries.dataPoints = append(timeseries.dataPoints, dataPoint)
			copy(timeseries.dataPoints[indexToInsertAt+1:], timeseries.dataPoints[indexToInsertAt:])
			timeseries.dataPoints[indexToInsertAt] = latestDataPoint
		}
		timeseries.mu.Unlock()
	}
	return
}

func (s *timeSeries) Query(name string, tags map[string]string, start int64, end int64) []*metric {
	seriesIdentifier := getSeriesIdentifier(name, tags)
	timeseries, ok := s.series[seriesIdentifier]
	if !ok {
		return []*metric{}
	} else {
		timeseries.mu.RLock()
		defer timeseries.mu.RUnlock()
		indexToStartAt := sort.Search(len(timeseries.dataPoints), func(i int) bool {
			return timeseries.dataPoints[i].timeStamp >= start
		})
		indexToEndAt := sort.Search(len(timeseries.dataPoints), func(i int) bool {
			return timeseries.dataPoints[i].timeStamp <= end
		})
		return timeseries.dataPoints[indexToStartAt : indexToEndAt+1]
	}
}
