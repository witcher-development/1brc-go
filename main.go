package main

import (
	"fmt"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
	"runtime/pprof"
	"flag"
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

	content, err := os.ReadFile("../1brc/measurements.txt")
	if err != nil {
		panic(err)
	}

	memProfiling(*profFlag, "mem_after_file_read")

	aggregate := make(map[string][]float64)
	var cities []string

	lines := strings.Split(string(content), "\n")
	for i, line := range lines {
		if i == len(lines) - 1 {
			break;
		}
		segments := strings.Split(line, ";")
		city := segments[0]
		temp, err := strconv.ParseFloat(segments[1], 64)
		if err != nil {
			panic(err)
		}

		if !slices.Contains(cities, city) {
			cities = append(cities, city)
		}
		aggregate[city] = append(aggregate[city], temp)
	}

	memProfiling(*profFlag, "mem_after_aggregate")

	slices.Sort(cities)

	memProfiling(*profFlag, "mem_after_sort")

	output := "{"

	for i, city := range cities {
		temps := aggregate[city]
		var sum float64 = 0
		for _, temp := range temps {
			sum += temp
		}
		// mean := sum / float64(len(temps))
		mean := round(round(sum)/float64(len(temps)))
		output += encode(city, slices.Min(temps), mean, slices.Max(temps), i < len(temps) - 1)
	}

	
	output = output + "}"

	memProfiling(*profFlag, "mem_after_output")


	fmt.Println(time.Since(start))
}
