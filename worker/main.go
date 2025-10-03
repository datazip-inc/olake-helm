package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/datazip-inc/olake-helm/worker/config"
	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/executor"
	_ "github.com/datazip-inc/olake-helm/worker/executor/docker"
	_ "github.com/datazip-inc/olake-helm/worker/executor/kubernetes"
	"github.com/datazip-inc/olake-helm/worker/logger"
	"github.com/datazip-inc/olake-helm/worker/temporal"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/spf13/viper"
)

func main() {
	logger.Init()
	config.Init()

	logger.Infof("Starting OLake worker")
	logger.Infof("Executor environment: %s", utils.GetExecutorEnvironment())

	db, err := database.NewDatabase()
	if err != nil {
		logger.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Initialize executor
	exec, err := executor.NewExecutor(db)
	if err != nil {
		logger.Fatalf("Failed to create executor: %v", err)
	}
	defer exec.Close()

	tClient, err := temporal.NewClient()
	if err != nil {
		logger.Fatalf("Failed to create Temporal client: %v", err)
	}
	defer tClient.Close()

	// init log cleaner
	utils.InitLogCleaner(utils.GetConfigDir(), viper.GetInt(constants.EnvLogRetentionPeriod))

	worker := temporal.NewWorker(tClient, exec)
	go func() {
		err := worker.Start()
		if err != nil {
			logger.Fatalf("Failed to start Temporal worker: %v", err)
			return
		}
	}()

	// start health server for kubernetes environment
	if utils.GetExecutorEnvironment() == string(executor.Kubernetes) {
		healthServer := temporal.NewHealthServer(worker, viper.GetInt(constants.EnvHealthPort))
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

	// stop the worker
	worker.Stop()
	logger.Info("Worker stopped!")
}
