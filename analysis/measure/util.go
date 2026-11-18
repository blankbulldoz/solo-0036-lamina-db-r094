package main

import "os"

func mkdtemp() (string, error) { return os.MkdirTemp("", "lamina-limits-") }
func removeall(d string)       { os.RemoveAll(d) }
