package utils

import "os"

func Load() (*os.File, error) {
	configFileName := "s3.json"
	if cfgFile := os.Getenv("CONFIG_FILE"); cfgFile != "" {
		configFileName = cfgFile
	}
	return os.Open(configFileName)
}

