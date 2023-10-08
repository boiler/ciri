package main

import (
	"fmt"
	"log"
	"net/http"

	//_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/boiler/ciri/config"
	"github.com/boiler/ciri/handler"
	"github.com/boiler/ciri/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	cfg := config.NewConfig()
	err := cfg.Parse()
	if err != nil {
		log.Fatal(err.Error())
	}
	log.Print("Start")

	metrics.Init(cfg)
	h := handler.New(cfg)
	h.Init()

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

	httpMux := http.NewServeMux()
	httpMux.HandleFunc("/", http.HandlerFunc(h.ServeHTTP))
	httpMux.Handle("/metrics", promhttp.Handler())
	httpServer := &http.Server{
		Addr:    cfg.Listen,
		Handler: httpMux,
	}
	httpServer.SetKeepAlivesEnabled(false)
	log.Printf("http server starts listening on %s", cfg.Listen)
	if err := httpServer.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
