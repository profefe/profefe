package log

import "go.uber.org/zap"

type Logger struct {
	base *zap.SugaredLogger
}

func New(log *zap.Logger) *Logger {
	return &Logger{
		base: log.Sugar(),
	}
}

func NewNop() *Logger {
	return New(zap.NewNop())
}

func (logger *Logger) With(args ...interface{}) *Logger {
	return &Logger{base: logger.base.With(args...)}
}

func (logger *Logger) Debug(args ...interface{}) {
	logger.base.Debug(args...)
}

func (logger *Logger) Debugf(format string, args ...interface{}) {
	logger.base.Debugf(format, args...)
}

func (logger *Logger) Debugw(msg string, pairs ...interface{}) {
	logger.base.Debugw(msg, pairs...)
}

func (logger *Logger) Info(args ...interface{}) {
	logger.base.Info(args...)
}

func (logger *Logger) Infof(format string, args ...interface{}) {
	logger.base.Infof(format, args...)
}

func (logger *Logger) Infow(msg string, pairs ...interface{}) {
	logger.base.Infow(msg, pairs...)
}

func (logger *Logger) Error(args ...interface{}) {
	logger.base.Error(args...)
}

func (logger *Logger) Errorf(format string, args ...interface{}) {
	logger.base.Errorf(format, args...)
}

func (logger *Logger) Errorw(msg string, pairs ...interface{}) {
	logger.base.Errorw(msg, pairs...)
}

func (logger *Logger) Fatal(args ...interface{}) {
	logger.base.Fatal(args...)
}

func (logger *Logger) Fatalf(format string, args ...interface{}) {
	logger.base.Fatalf(format, args...)
}

func (logger *Logger) Fatalw(msg string, pairs ...interface{}) {
	logger.base.Fatalw(msg, pairs...)
}
