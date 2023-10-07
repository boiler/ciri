package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/boiler/ciri/config"
	"github.com/boiler/ciri/handler"
)

func main() {

	cfg := config.NewConfig()
	err := cfg.Parse()
	if err != nil {
		log.Fatal(err.Error())
	}

	h := handler.New(cfg)
	h.Start()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for _ = range sigc {
			fmt.Println("TERM signal received")
			h.Terminate()
			fmt.Println("EXIT")
			os.Exit(0)
		}
	}()

	httpServer := &http.Server{
		Addr:    cfg.Listen,
		Handler: http.HandlerFunc(h.ServeHTTP),
	}
	httpServer.SetKeepAlivesEnabled(false)
	log.Print("started")
	if err := httpServer.ListenAndServe(); err != nil {
		panic(err)
	}
}
