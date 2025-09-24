package main

import (
	"os"
	"os/signal"
	"syscall"

	_ "github.com/datazip-inc/olake-helm/worker/executor/docker"
	"github.com/datazip-inc/olake-helm/worker/logger"
	"github.com/datazip-inc/olake-helm/worker/temporal"
	"github.com/datazip-inc/olake-helm/worker/types"
)

func main() {
	logConfig := &types.LoggingConfig{
		Level:  "info", // or get from env var
		Format: "console",
	}
	logger.Init(logConfig)

	tClient, err := temporal.NewClient()
	if err != nil {
		logger.Fatalf("Failed to create Temporal client: %v", err)
	}
	defer tClient.Close()

	worker := temporal.NewWorker(tClient)
	go func() {
		err := worker.Start()
		if err != nil {
			logger.Fatalf("Failed to start Temporal worker: %v", err)
			return
		}
	}()

	// setup signal handling for graceful shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	// wait for termination signal
	sig := <-signalChan
	logger.Infof("Received signal %v, shutting down worker.", sig)

	// stop the worker
	worker.Stop()
	logger.Info("Worker stopped!")
}
