package common

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func NewZapConsoleEncoder() zapcore.Encoder {
	cfg := zap.NewProductionConfig()
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	return zapcore.NewConsoleEncoder(cfg.EncoderConfig)
}
