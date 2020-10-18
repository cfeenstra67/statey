package main

import "fmt"

var a *int = nil

func main() {
	fmt.Printf("A: %v\n", GetA())
	a = new(int)
	*a = 1
	fmt.Printf("A2: %v\n", GetA())
}

func GetA() *int { return a }
