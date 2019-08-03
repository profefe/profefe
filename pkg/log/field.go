package log

import (
	"regexp"
	"strings"

	"go.uber.org/zap/zapcore"
)

var whitespaceRe = regexp.MustCompile("\\s+")

type multiLineString struct {
	string
}

func (s multiLineString) String() string {
	str := whitespaceRe.ReplaceAllString(s.string, " ")
	return strings.TrimSpace(str)
}

// MultiLine construct zapcore.Field that caries a multi line string.
func MultiLine(key, val string) zapcore.Field {
	return zapcore.Field{Key: key, Type: zapcore.StringerType, Interface: multiLineString{val}}
}
