package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
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

func printMemStats(message string, rtm runtime.MemStats){
	fmt.Println("\n===", message, "===")
	fmt.Println("Mallocs: ", rtm.Mallocs)
	fmt.Println("Frees: ", rtm.Frees)
	fmt.Println("LiveObjects: ", rtm.Mallocs - rtm.Frees)
	fmt.Println("PauseTotalNs: ", rtm.PauseTotalNs)
	fmt.Println("NumGC: ", rtm.NumGC)
	fmt.Println("LastGC: ", time.UnixMilli(int64(rtm.LastGC/1_000_000)))
	fmt.Println("HeapObjects: ", rtm.HeapObjects)
	fmt.Println("HeapAlloc: ", rtm.HeapAlloc)
}

// func lineCounter(r io.Reader) (int, error) {
//     buf := make([]byte, 14*1024*1024*1024)
//     count := 0
//     lineSep := []byte{'\n'}
//
//     for {
//         c, err := r.Read(buf)
//         count += bytes.Count(buf[:c], lineSep)
//
//         switch {
//         case err == io.EOF:
//             return count, nil
//
//         case err != nil:
//             return count, err
//         }
//     }
// }

func main() {
	profFlag := flag.String("p", "", "profiling")
	flag.Parse()

	var rtm runtime.MemStats
	runtime.ReadMemStats(&rtm)

	stopCPUProfliling := cpuProfiling(*profFlag)
	defer stopCPUProfliling()

	start := time.Now()

	file, err := os.Open("../1brc/measurements_1b.txt")
	if err != nil {
		panic(err)
	}

	stat, err := file.Stat()
	perRange := stat.Size() / int64(runtime.NumCPU())
	file.Close()
	var wg sync.WaitGroup
	var cursor int64 = 0

	type Aggregate struct {
		data map[string][]float64
		cities []string
		mu sync.Mutex
	}
	ag := Aggregate{data: make(map[string][]float64)}

	for cursor < stat.Size() - 1 {
		wg.Add(1)

		go func(cursor int64, perRange int64) {
			file, err := os.Open("../1brc/measurements_1b.txt")
			if err != nil {
				panic(err)
			}
			defer func () {
				file.Close()
				wg.Done()
				runtime.GC()
				fmt.Println("freed")
			}()

			file.Seek(cursor, 0)
			reader := bufio.NewReader(file)

			if cursor != 0 {
				_, err := reader.ReadBytes('\n')
				if err == io.EOF {
					fmt.Println("EOF")
					return
				}

				if err != nil {
					panic(err)
				}
			}

			scanner := bufio.NewScanner(reader)

			lagg := make(map[string][]float64)

			var cum int64 = 0
			for scanner.Scan() {
				if cum > cursor + perRange {
					break
				}
				line := scanner.Text()
				cum += int64(len(line))

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

		}(cursor, perRange)

		cursor += perRange
	}
	wg.Wait()

	// count, err := lineCounter(file)

	// fmt.Println(time.Since(start))
	// fmt.Println(count)
	// return
	// content_raw, err := os.ReadFile("../1brc/measurements_1b.txt")
	// if err != nil {
	// 	panic(err)
	// }
	//
	// content := string(content_raw)
	// fmt.Println("after file read")
	//
	// var ranges []int64
	// var cursor int64 = 0
	// cl := int64(len(content))
	//
	// printMemStats("Start", rtm)
	//
	// for true {
	// 	index := int64(strings.Index(content[cursor:], "\n"))
	// 	ranges = append(ranges, cursor+index)
	// 	cursor += index + 1
	//
	// 	// if cursor % 10000000 == 0 {
	// 	// 	memProfiling(*profFlag, "mem_in_loop")
	// 		// runtime.GC()
	// 		// fmt.Println("creating ranges", cursor, cl)
	// 		// printMemStats(string(cursor), rtm)
	// 	// }
	// 	if cursor > cl - 1 {
	// 		break
	// 	}
	// }
	//
	// memProfiling(*profFlag, "mem_after_file_read")
	// printMemStats("Start", rtm)

	memProfiling(*profFlag, "mem_after_aggregate")

	slices.Sort(ag.cities)

	memProfiling(*profFlag, "mem_after_sort")

	output := "{"

	fmt.Println("we got here")
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
	//
	// memProfiling(*profFlag, "mem_after_output")
	//
	//
	fmt.Println(time.Since(start))
	// fmt.Println(output)
}
