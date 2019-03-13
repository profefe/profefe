package logger

import "go.uber.org/zap"

type Logger struct {
	base *zap.SugaredLogger
}

func New(log *zap.Logger) *Logger {
	zap.RedirectStdLog(log)

	return &Logger{
		base: log.Sugar(),
	}
}

func NewNop() *Logger {
	return New(zap.NewNop())
}

func (log *Logger) With(args ...interface{}) *Logger {
	return &Logger{base: log.base.With(args...)}
}

func (log *Logger) Debug(args ...interface{}) {
	log.base.Debug(args...)
}

func (log *Logger) Debugf(format string, args ...interface{}) {
	log.base.Debugf(format, args...)
}

func (log *Logger) Debugw(msg string, pairs ...interface{}) {
	log.base.Debugw(msg, pairs...)
}

func (log *Logger) Info(args ...interface{}) {
	log.base.Info(args...)
}

func (log *Logger) Infof(format string, args ...interface{}) {
	log.base.Infof(format, args...)
}

func (log *Logger) Infow(msg string, pairs ...interface{}) {
	log.base.Infow(msg, pairs...)
}

func (log *Logger) Error(args ...interface{}) {
	log.base.Error(args...)
}

func (log *Logger) Errorf(format string, args ...interface{}) {
	log.base.Errorf(format, args...)
}

func (log *Logger) Errorw(msg string, pairs ...interface{}) {
	log.base.Errorw(msg, pairs...)
}

func (log *Logger) Fatal(args ...interface{}) {
	log.base.Fatal(args...)
}

func (log *Logger) Fatalf(format string, args ...interface{}) {
	log.base.Fatalf(format, args...)
}

func (log *Logger) Fatalw(msg string, pairs ...interface{}) {
	log.base.Fatalf(msg, pairs...)
}
