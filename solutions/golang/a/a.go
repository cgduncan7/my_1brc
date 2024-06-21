package a

import (
	"encoding/json"
	"io"
	"os"
	"strconv"
	"unicode/utf8"
)

var OUTPUT_FILE = "../../../data/output_golang_a.txt"
var CHUNK_SIZE int64 = 1024 * 1024 * 1

type StationAggregate struct {
	Count int64
	Min   float32
	Max   float32
	Mean  float32
}

func (agg *StationAggregate) Combine(toCombine *StationAggregate) {
	agg.Mean = ((agg.Mean * float32(agg.Count)) + (toCombine.Mean * float32(toCombine.Count))) / float32(agg.Count+toCombine.Count)
	if agg.Min > toCombine.Min {
		agg.Min = toCombine.Min
	}
	if agg.Max < toCombine.Max {
		agg.Max = toCombine.Max
	}
	agg.Count = agg.Count + toCombine.Count
}

func addStationAggregate(aggs map[string]StationAggregate, key string, value float32) {
	ex := aggs[key]
	newAgg := StationAggregate{
		Count: 1,
		Min:   value,
		Max:   value,
		Mean:  value,
	}
	if ex.Count == 0 {
		aggs[key] = StationAggregate{
			Count: 1,
			Min:   value,
			Max:   value,
			Mean:  value,
		}
	} else {
		ex.Combine(&newAgg)
		aggs[key] = ex
	}
}

func A(inputFile string) {
	file, err := os.Open(inputFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	stationAggregates := make(map[string]StationAggregate)

	bytes := make([]byte, CHUNK_SIZE)
	stationNameBuilder := make([]rune, 0)
	temperatureBuilder := make([]rune, 0)
	readingStationName := true
	done := false
	for err != io.EOF {
		_, err = file.Read(bytes)
		for i := 0; i < len(bytes) && !done; {
			r, size := utf8.DecodeRune(bytes[i:])
			switch r {
			case 0: // <null>
				done = true
				fallthrough
			case 10: // \n
				readingStationName = true
				stationName := string(stationNameBuilder)
				temp, err := strconv.ParseFloat(string(temperatureBuilder), 32)
				if err == nil {
					addStationAggregate(stationAggregates, stationName, float32(temp))
				} else if stationName != "" {
					panic(err)
				}
				stationNameBuilder = make([]rune, 0)
				temperatureBuilder = make([]rune, 0)
			case 59: // ;
				readingStationName = false
			default:
				if readingStationName {
					stationNameBuilder = append(stationNameBuilder, r)
				} else {
					temperatureBuilder = append(temperatureBuilder, r)
				}
			}
			i += size
		}
		bytes = make([]byte, CHUNK_SIZE)
	}

	output, err := json.Marshal(stationAggregates)
	if err != nil {
		panic(err)
	}
	os.WriteFile(OUTPUT_FILE, output, 0744)
}
