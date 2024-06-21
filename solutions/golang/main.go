package main

import (
	"fmt"
	"os"
	"solutions/a"
	"solutions/b"
)

var INPUT_FILE = "../../data/measurements.txt"
var INPUT_FILE_SMALL = "../../data/measurements_small.txt"

func main() {
	if len(os.Args) == 1 {
		panic("no solution argument")
	}
	if len(os.Args) == 2 {
		panic("no full argument")
	}
	sln := os.Args[1]
	useFullInput := os.Args[2] == "full"
	inputFile := INPUT_FILE
	if !useFullInput {
		inputFile = INPUT_FILE_SMALL
	}
	switch sln {
	case "a":
		a.A(inputFile)
	case "b":
		b.B(inputFile)
	default:
		panic("unregistered solution")
	}
	fmt.Println(sln)
}
