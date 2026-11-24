//go:build !linux || !crew_drop

package main

func runDrop([]string) (bool, int) { return false, 0 }
