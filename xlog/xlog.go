package xlog

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger *zap.Logger

func GetLogger() *zap.Logger {
	if logger == nil {
		logger, _ = zap.NewProduction()
	}
	return logger
}

func SetLevel(lvl zapcore.Level) {
	logger = GetLogger().WithOptions(zap.IncreaseLevel(lvl))
}
