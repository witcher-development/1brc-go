package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)


func cpuProfiling(flag string) func() {
	if flag != "cpu" {
		return func() {}
	}
	f, err := os.Create("cpu.pprof")
	if err != nil {
		panic(err)
	}
	pprof.StartCPUProfile(f)
	return pprof.StopCPUProfile
}

func memProfiling(flag string, profileName string) {
	if flag != "mem" {
		return
	}
	f, err := os.Create(profileName + ".pprof")
	if err != nil {
		panic(err)
	}
	pprof.WriteHeapProfile(f)
	f.Close()
}

func round(x float64) float64 {
	return math.Floor((x+0.05)*10) / 10
}

func encode(city string, min float64, mean float64, max float64, last bool) string {
	if (!last) {
		return fmt.Sprintf("%s=%.1f/%.1f/%.1f", city, min, mean, max)
	} else {
		return fmt.Sprintf("%s=%.1f/%.1f/%.1f, ", city, min, mean, max)
	}
}

func main() {
	profFlag := flag.String("p", "", "profiling")
	flag.Parse()

	stopCPUProfliling := cpuProfiling(*profFlag)
	defer stopCPUProfliling()

	start := time.Now()

	content_raw, err := os.ReadFile("../1brc/measurements.txt")
	if err != nil {
		panic(err)
	}
	content := string(content_raw)

	var ranges [][2]int
	cursor := 0

	for true {
		index := strings.Index(content[cursor:], "\n")
		ranges = append(ranges, [2]int{cursor, index})
		cursor += index + 1

		if cursor > len(content) - 1 {
			break
		}
	}

	memProfiling(*profFlag, "mem_after_file_read")

	type Aggregate struct {
		data map[string][]float64
		cities []string
		mu sync.Mutex
	}
	ag := Aggregate{data: make(map[string][]float64)}

	var wg sync.WaitGroup

	perRange := len(ranges) / runtime.NumCPU()
	fmt.Println(perRange)
	for i := 0; i < len(ranges); i += perRange {
		wg.Add(1)
		
		go func() {
			defer wg.Done()
			lagg := make(map[string][]float64)
			for j := i; j < i + perRange; j++ {
				line := content[ranges[j][0]:ranges[j][0]+ranges[j][1]]

				sep := strings.Index(line, ";")
				city := line[:sep]
				temp_raw := line[sep+1:]

				temp, err := strconv.ParseFloat(temp_raw, 64)
				if err != nil {
					panic(err)
				}
				
				lagg[city] = append(lagg[city], temp)
			}

			ag.mu.Lock()
			defer ag.mu.Unlock()

			for city, temps := range lagg {
				if len(ag.data[city]) == 0 {
					ag.cities = append(ag.cities, city)
				}
				ag.data[city] = append(ag.data[city], temps...)
			}
		}()
	}

	wg.Wait()

	memProfiling(*profFlag, "mem_after_aggregate")

	slices.Sort(ag.cities)

	memProfiling(*profFlag, "mem_after_sort")

	output := "{"

	for i, city := range ag.cities {
		temps := ag.data[city]
		var sum float64 = 0
		for _, temp := range temps {
			sum += temp
		}
		// mean := sum / float64(len(temps))
		mean := round(round(sum)/float64(len(temps)))
		output += encode(city, slices.Min(temps), mean, slices.Max(temps), i < len(ag.cities) - 1)
	}


	output = output + "}"

	memProfiling(*profFlag, "mem_after_output")


	fmt.Println(time.Since(start))
}
