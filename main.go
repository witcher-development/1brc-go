package main

import (
	"fmt"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
)

func round(x float64) float64 {
	return math.Floor((x+0.05)*10) / 10
}

func main() {
	content, err := os.ReadFile("measurments.txt")
	if err != nil {
		panic(err)
	}

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
	slices.Sort(cities)

	calc := make(map[string][]float64)

	for _, city := range cities {
		temps := aggregate[city]
		var sum float64 = 0
		for _, temp := range temps {
			sum += temp
		}
		// mean := sum / float64(len(temps))
		mean := round(round(sum)/float64(len(temps)))

		values := []float64{slices.Min(temps), mean, slices.Max(temps)}
		calc[city] = values
	}

	// fmt.Println(calc)

	output := "{"

	for i, city := range cities {
		values := calc[city]
		if (i == len(cities) - 1) {
			output = output + fmt.Sprintf("%s=%.1f/%.1f/%.1f", city, values[0], values[1], values[2])
		} else {
			output = output + fmt.Sprintf("%s=%.1f/%.1f/%.1f, ", city, values[0], values[1], values[2])
		}
	}
	
	output = output + "}"

	fmt.Println(output)
	// fmt.Println("--------------------")
	// fmt.Println(aggregate)
}
