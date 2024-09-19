package main

import (
	"bufio"
	"flag"
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
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

func round(x float32) float32 {
	return float32(math.Floor(float64((x+0.05)*10)) / 10)
}

func encode(city string, min float32, mean float32, max float32, last bool) string {
	if (!last) {
		return fmt.Sprintf("%s=%.1f/%.1f/%.1f", city, min, mean, max)
	} else {
		return fmt.Sprintf("%s=%.1f/%.1f/%.1f, ", city, min, mean, max)
	}
}

func printMemStats(rtm runtime.MemStats){
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

	// var rtm runtime.MemStats

	stopCPUProfliling := cpuProfiling(*profFlag)
	defer stopCPUProfliling()

	start := time.Now()

	file, err := os.Open("../1brc/measurements.txt")
	if err != nil {
		panic(err)
	}

	stat, err := file.Stat()
	routinesCount := runtime.NumCPU()
	perRange := stat.Size() / int64(routinesCount)
	file.Close()

	var cursor int64 = 0

	var mx sync.Mutex
	// var cities []string
	type City struct {
		min float32
		mean float32
		max float32
	}
	ag := make(map[string]string)
	var wg sync.WaitGroup

	dict := make(map[int8][2]int)
	dict[0] = [2]int{65, 65}
	dict[1] = [2]int{66, 66}
	dict[2] = [2]int{67, 67}
	dict[3] = [2]int{68, 68}
	dict[4] = [2]int{69, 69}
	dict[5] = [2]int{70, 70}
	dict[7] = [2]int{71, 71}
	dict[8] = [2]int{72, 72}
	dict[9] = [2]int{73, 73}
	dict[10] = [2]int{74, 74}
	dict[11] = [2]int{75, 76}
	dict[12] = [2]int{77, 78}
	dict[13] = [2]int{79, 80}
	dict[14] = [2]int{81, 90}
	dict[15] = [2]int{91, 2000}

	for cursor < stat.Size() - 1 {
		wg.Add(1)

		var index int8 = 0
		if cursor != 0 {
			index = int8(cursor / perRange)
		}
		go func(index int8) {
			// fmt.Println("new routine", cursor / perRange, stat.Size() - cursor)
			file, err := os.Open("../1brc/measurements_1b.txt")
			if err != nil {
				panic(err)
			}
			defer func () {
				file.Close()
				wg.Done()
			}()

			reader := bufio.NewReader(file)
			scanner := bufio.NewScanner(reader)

			lag := make(map[string]map[float32]int8)

			lmin := byte(dict[index][0])
			lmax := byte(dict[index][1])
			for scanner.Scan() {
				line := scanner.Bytes()
				first := line[0]
				// fmt.Println(first, lmin, lmax, first >= byte(lmin) && byte(lmax) >= first, string(first), string(lmin), string(lmax))

				if first < lmin && lmax < first {
					continue
				}
				text := string(line)

				// break

				sep := strings.Index(text, ";")
				city := text[:sep]
				temp_raw := text[sep+1:]

				temp, err := strconv.ParseFloat(temp_raw, 32)
				if err != nil {
					panic(err)
				}
				temp_32 := float32(temp)

				_, has := lag[city]
				if (has) {
					lag[city][temp_32]++
				} else {
					lag[city] = make(map[float32]int8)
					lag[city][temp_32] = 1
				}
				// lag[city] = append(lag[city], float32(temp))

				// ag[key] = struct{}{}
			}
			// fmt.Println("old routine", cursor / perRange, time.Since(start))


			lag2 := make(map[string]string)
			for city, temps := range lag {
				var sum float32 = 0
				var min float32 = 100
				var max float32 = 0
				for temp, count := range temps {
					if (temp < min) {
						min = temp
					}
					if (temp > max) {
						max = temp
					}
					sum += temp * float32(count)
				}
				mean := round(round(sum)/float32(len(temps)))
				lag2[city] = encode(city, min, mean, max, false)
			}

			fmt.Println(index, time.Since(start))
			mx.Lock()
			defer mx.Unlock()

			//
			// for city, temps := range lag {
			// 	var sum float32 = 0
			// 	for _, temp := range temps {
			// 		sum += temp
			// 	}
			// 	mean := round(round(sum)/float32(len(temps)))
			// 	ag[city] = encode(city, slices.Min(temps), mean, slices.Max(temps), false)
			// }
			for city, str := range lag2 {
				ag[city] = str
			}
		}(index)

		cursor += perRange
	}

	wg.Wait()

	// memProfiling(*profFlag, "mem_after_aggregate")
	//
	// slices.Sort(ag.cities)
	//
	// memProfiling(*profFlag, "mem_after_sort")
	//
	// output := "{"
	//
	// for i, city := range ag.cities {
	// 	temps := ag.data[city]
	// 	var sum float32 = 0
	// 	for _, temp := range temps {
	// 		sum += temp
	// 	}
	// 	// mean := sum / float64(len(temps))
	// 	mean := round(round(sum)/float32(len(temps)))
	// 	output += encode(city, slices.Min(temps), mean, slices.Max(temps), i < len(ag.cities) - 1)
	// }


	// output = output + "}"
	//
	// memProfiling(*profFlag, "mem_after_output")
	//
	//
	fmt.Println(time.Since(start))
	// fmt.Println(output)
}
