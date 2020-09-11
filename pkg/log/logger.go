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

func (logger *Logger) Debugw(msg string, pairs ...interface{}) {
	logger.base.Debugw(msg, pairs...)
}

func (logger *Logger) Infow(msg string, pairs ...interface{}) {
	logger.base.Infow(msg, pairs...)
}

func (logger *Logger) Errorw(msg string, pairs ...interface{}) {
	logger.base.Errorw(msg, pairs...)
}
