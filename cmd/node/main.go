package main

import (
    "flag"
    "log"

    "github.com/0xh4ty/libp2p-test/internal/app"
)

func main() {
    relay := flag.Bool("relay", false, "run as a public relay / DHT server")
    flag.Parse()

    log.SetFlags(log.LstdFlags | log.Lmicroseconds)
    app.RunNode(*relay)
}
