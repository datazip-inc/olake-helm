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

	logger.Infof("starting OLake worker")
	logger.Infof("executor environment: %s", executor.GetExecutorEnvironment())

	// Initialize database
	db := database.GetDB()
	logger.Infof("database initialized")
	defer db.Close()

	// Initialize executor
	exec, err := executor.NewExecutor()
	if err != nil {
		logger.Fatalf("failed to create executor: %s", err)
	}
	defer exec.Close()

	tClient, err := temporal.NewClient()
	if err != nil {
		logger.Fatalf("failed to create Temporal client: %s", err)
	}
	defer tClient.Close()

	// init log cleaner
	utils.InitLogCleaner(utils.GetConfigDir(), viper.GetInt(constants.EnvLogRetentionPeriod))

	worker := temporal.NewWorker(tClient, exec)

	// start health server for kubernetes environment
	if executor.GetExecutorEnvironment() == string(executor.Kubernetes) {
		healthServer := temporal.NewHealthServer(worker)
		go func() {
			err := healthServer.Start()
			if err != nil {
				logger.Fatalf("failed to start Kubernetes health server: %s", err)
			}
		}()
	}

	go func() {
		err := worker.Start()
		if err != nil {
			logger.Fatalf("failed to start Temporal worker: %s", err)
			return
		}
	}()

	// setup signal handling for graceful shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	// wait for termination signal
	sig := <-signalChan
	logger.Infof("received signal %v, shutting down worker.", sig)

	// stop the worker
	worker.Stop()
	logger.Info("worker stopped!")
}
