package log

import (
	"encoding/hex"
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

type byteString struct {
	val []byte
}

func (b byteString) String() string {
	return hex.EncodeToString(b.val)
}

// ByteString construct zapcore.Field that caries hex encoded data as []byte.
func ByteString(key string, val []byte) zapcore.Field {
	return zapcore.Field{Key: key, Type: zapcore.StringerType, Interface: byteString{val}}
}
