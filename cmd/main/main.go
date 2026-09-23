package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"job4j_go_share_trip_notification/config"
	"job4j_go_share_trip_notification/internal/api"
	"job4j_go_share_trip_notification/internal/app"
	"job4j_go_share_trip_notification/internal/domain/notification/repository"
	"job4j_go_share_trip_notification/internal/events"
	"job4j_go_share_trip_notification/internal/storage"

	"github.com/gofiber/fiber/v2"
	"github.com/joho/godotenv"
)

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	if err := godotenv.Load("./.env"); err != nil {
		log.Println("No .env file found, using system environment variables")
	}

	cfg := config.GetAppConfig()

	storageCfg := storage.Config{
		Host:     cfg.Database.Host,
		Port:     cfg.Database.Port,
		User:     cfg.Database.User,
		Password: cfg.Database.Password,
		DBName:   cfg.Database.Name,
		SSLMode:  cfg.Database.SSLMode,
	}

	logger, logFile, err := app.NewLogger()
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := logFile.Close(); err != nil {
			log.Printf("failed to close log file: %v", err)
		}
	}()

	pool, err := storage.NewPool(ctx, storageCfg.DSN())
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	server := api.NewServer(pool)

	fiberApp := fiber.New()
	server.Route(fiberApp.Group("/api"))

	consumer := events.NewConsumer(
		cfg.Kafka.Brokers,
		cfg.Kafka.Topic,
		cfg.Kafka.GroupID,
	)
	defer func() {
		if err := consumer.Close(); err != nil {
			logger.Error("failed to close kafka consumer", "error", err)
		}
	}()

	processor := events.NewProcessor(
		pool,
		repository.NewNotificationRepository(pool),
		repository.NewProcessedEventRepository(),
	)

	go func() {
		logger.Info(
			"kafka consumer started",
			"brokers", cfg.Kafka.Brokers,
			"topic", cfg.Kafka.Topic,
			"group_id", cfg.Kafka.GroupID,
		)
		if err := consumer.Run(ctx, processor.Handle); err != nil &&
			!errors.Is(err, context.Canceled) {
			logger.Error("kafka consumer stopped", "error", err)
			stop()
		}
	}()

	go func() {
		addr := fmt.Sprintf(":%d", cfg.Server.Port)
		if err := fiberApp.Listen(addr); err != nil {
			logger.Error("http server stopped", "error", err)
			stop()
		}
	}()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := fiberApp.ShutdownWithContext(shutdownCtx); err != nil {
		logger.Error("http server shutdown failed", "error", err)
	}
}
