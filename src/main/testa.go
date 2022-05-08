package main

import (
	"fmt"
	"os"
)

func main() {
	for _, filename := range os.Args[:] {
		fmt.Println(filename)
	}
}
