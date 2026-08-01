package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"job4j_go_share_trip_notification/config"
	"job4j_go_share_trip_notification/internal/api"
	"job4j_go_share_trip_notification/internal/app"
	"job4j_go_share_trip_notification/internal/storage"

	"github.com/gofiber/fiber/v2"
	"github.com/joho/godotenv"
)

func main() {
	ctx := context.Background()

	// Загружаем .env файл
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

    if err != nil {
        logger.Error("init tracing failed", "error", err)
        os.Exit(1)
    }

	pool, err := storage.NewPool(ctx, storageCfg.DSN())
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	server := api.NewServer(pool)

	app := fiber.New()

	server.Route(app.Group("/api"))

	err = app.Listen(fmt.Sprintf(":%d", cfg.Server.Port))
	if err != nil {
		log.Fatal(err)
	}
}
