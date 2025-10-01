package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/datazip-inc/olake-helm/worker/executor"
	_ "github.com/datazip-inc/olake-helm/worker/executor/docker"
	_ "github.com/datazip-inc/olake-helm/worker/executor/kubernetes"
	"github.com/datazip-inc/olake-helm/worker/logger"
	"github.com/datazip-inc/olake-helm/worker/temporal"
	"github.com/datazip-inc/olake-helm/worker/utils"
)

func main() {
	logger.Init()

	tClient, err := temporal.NewClient()
	if err != nil {
		logger.Fatalf("Failed to create Temporal client: %v", err)
	}
	defer tClient.Close()

	// init log cleaner
	utils.InitLogCleaner(utils.GetConfigDir(), utils.GetLogRetentionPeriod())

	worker := temporal.NewWorker(tClient)
	go func() {
		err := worker.Start()
		if err != nil {
			logger.Fatalf("Failed to start Temporal worker: %v", err)
			return
		}
	}()

	// start health server for kubernetes environment
	if utils.GetExecutorEnvironment() == string(executor.Kubernetes) {
		healthServer := temporal.NewHealthServer(worker, utils.GetHealthPort())
		go func() {
			err := healthServer.Start()
			if err != nil {
				logger.Fatalf("Failed to start Kubernetes health server: %v", err)
			}
		}()
	}

	// setup signal handling for graceful shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	// wait for termination signal
	sig := <-signalChan
	logger.Infof("Received signal %v, shutting down worker.", sig)

	// clean up executor
	if instance, _ := executor.GetExecutor(); instance != nil {
		if err := instance.Close(); err != nil {
			logger.Fatalf("Failed to close executor: %v", err)
		}
	}

	// stop the worker
	worker.Stop()
	logger.Info("Worker stopped!")
}
