package main

import "time"

func indexBuildStart() time.Time            { return time.Now() }
func indexBuildElapsed(t time.Time) float64 { return float64(time.Since(t).Microseconds()) / 1000 }
