package util

import (
    "os"

    "go.uber.org/zap"
    "go.uber.org/zap/zapcore"
)

var Logger *zap.SugaredLogger

func init() {
    cfg := zap.NewProductionConfig()
    cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
    writeStdout := zapcore.AddSync(os.Stdout)
    encoder := zapcore.NewConsoleEncoder(cfg.EncoderConfig)
    core := zapcore.NewCore(
        encoder,
        writeStdout,
        zap.DebugLevel,
    )
    zapLogger := zap.New(
        core,
        zap.AddStacktrace(zap.ErrorLevel),
    )
    Logger = zapLogger.Sugar()
}
