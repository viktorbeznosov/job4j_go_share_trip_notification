package config

type AppConfig struct {
	App      AppConfigSection
	Database DatabaseConfigSection
	Server   ServerConfigSection
}

type AppConfigSection struct {
	Name    string `json:"name"`
	Env     string `json:"env"`
	Debug   bool   `json:"debug"`
	Version string `json:"version"`
}

type DatabaseConfigSection struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Name     string `json:"name"`
	SSLMode  string `json:"sslMode"`
	MaxConns int    `json:"maxConns"`
}

type ServerConfigSection struct {
	Port int `json:"port"`
}

func GetAppConfig() AppConfig {
	return AppConfig{
		App: AppConfigSection{
			Name:    Env("APP_NAME", "sharetrip-api"),
			Env:     Env("APP_ENV", "local"),
			Debug:   EnvBool("APP_DEBUG", true),
			Version: Env("APP_VERSION", "1.0.0"),
		},
		Database: DatabaseConfigSection{
			Host:     Env("DB_HOST", "localhost"),
			Port:     EnvInt("DB_PORT", 6543),
			User:     Env("DB_USER", "postgres"),
			Password: Env("DB_PASSWORD", "password"),
			Name:     Env("DB_NAME", "share_trip"),
			SSLMode:  Env("DB_SSLMODE", "disable"),
			MaxConns: EnvInt("DB_MAX_CONNS", 10),
		},
		Server: ServerConfigSection{
			Port: EnvInt("SERVER_PORT", 8080),
		},
	}
}