package main

import (
	"github.com/ledgerwatch/turbo-geth/cmd/state/commands"
)

func main() {
	commands.Execute()
}

// go tool pprof -lines -http=: http://127.0.0.1:6060/debug/pprof/heap?seconds=20
//go tool pprof -lines -http=: http://127.0.0.1:6060/debug/pprof/profile?seconds=20