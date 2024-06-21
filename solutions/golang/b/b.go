package b

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"unicode/utf8"
)

var OUTPUT_FILE = "../../data/output_golang_b.txt"
var CHUNK_SIZE = 1024 * 1024 * 32
var NUM_WORKERS = 16

type Measurement struct {
	StationName string
	Temperature float32
}

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

func addStationAggregate(aggs *sync.Map, key string, value float32) {
	newAgg := StationAggregate{
		Count: 1,
		Min:   value,
		Max:   value,
		Mean:  value,
	}
	ex, loaded := aggs.LoadOrStore(key, newAgg)
	if loaded {
		sa, ok := ex.(StationAggregate)
		if !ok {
			panic("Failed to assert type")
		}
		sa.Combine(&newAgg)
		aggs.Store(key, sa)
	}
}

func splitLeftoverBytes(chunk *[]byte) []byte {
	leftoverBytes := []byte{}
	done := false
	for !done {
		if len(*chunk) == 0 {
			done = true
		} else {
			tail := (*chunk)[len(*chunk)-1:]
			if tail[0] != 0x0a {
				leftoverBytes = append(tail, leftoverBytes...)
				*chunk = (*chunk)[:len(*chunk)-1]
			} else {
				done = true
			}
		}
	}
	return leftoverBytes
}

func chunkHandler(chunk []byte, syncMap *sync.Map) {
	readingStationName := true
	stationNameBuilder := []rune{}
	temperatureBuilder := []rune{}
	done := false
	for i := 0; i < len(chunk) && !done; {
		r, size := utf8.DecodeRune(chunk[i:])
		switch r {
		case 0: // <null>
			done = true
			fallthrough
		case 10: // \n
			readingStationName = true
			stationName := string(stationNameBuilder)
			temp, err := strconv.ParseFloat(string(temperatureBuilder), 32)
			if err == nil {
				measurement := Measurement{StationName: stationName, Temperature: float32(temp)}
				addStationAggregate(syncMap, measurement.StationName, measurement.Temperature)
			} else if stationName != "" {
				fmt.Printf("Failed when station name: '%s'\n", stationName)
				panic(err)
			}
			stationNameBuilder = []rune{}
			temperatureBuilder = []rune{}
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
}

func spawnWorker(chunkChannel chan []byte, syncMap *sync.Map, wg *sync.WaitGroup) {
	defer wg.Done()
	for chunk := range chunkChannel {
		chunkHandler(chunk, syncMap)
	}
}

func B(inputFile string) {
	file, err := os.Open(inputFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	var waitGroup sync.WaitGroup
	var stationAggregates sync.Map
	chunkChannel := make(chan []byte)

	for i := 0; i < NUM_WORKERS; i++ {
		waitGroup.Add(1)
		go spawnWorker(chunkChannel, &stationAggregates, &waitGroup)
	}

	leftoverBytes := []byte{}
	for err != io.EOF {
		bytes := make([]byte, CHUNK_SIZE+len(leftoverBytes))
		copy(bytes, leftoverBytes)
		_, err = file.Read(bytes[len(leftoverBytes):])
		leftoverBytes = splitLeftoverBytes(&bytes)
		chunkChannel <- bytes
	}

	close(chunkChannel)
	waitGroup.Wait()

	results := make(map[string]StationAggregate)
	stationAggregates.Range(func(key any, value any) bool {
		ks := key.(string)
		sa := value.(StationAggregate)
		results[ks] = sa
		return true
	})

	output, err := json.Marshal(results)
	if err != nil {
		panic(err)
	}
	os.WriteFile(OUTPUT_FILE, output, 0744)
}
