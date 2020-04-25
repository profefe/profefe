package pprofutil

import (
	"io"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProfileParser(t *testing.T) {
	data, err := ioutil.ReadFile("../../testdata/collector_cpu_1.prof")
	require.NoError(t, err)

	t.Run("ParseProfile", func(t *testing.T) {
		parser := NewProfileParser(data)
		prof1, err := parser.ParseProfile()
		require.NoError(t, err)
		require.NotEmpty(t, prof1.Sample)

		prof2, err := parser.ParseProfile()
		require.NoError(t, err)

		require.True(t, ProfilesEqual(prof1, prof2))
	})

	t.Run("Reader", func(t *testing.T) {
		parser := NewProfileParser(data)

		gotData, err := ioutil.ReadAll(parser)
		require.NoError(t, err)

		require.Equal(t, data, gotData)
	})

	t.Run("ReadSeeker", func(t *testing.T) {
		parser := NewProfileParser(data)

		gotData1, err := ioutil.ReadAll(parser)
		require.NoError(t, err)
		require.NotEmpty(t, gotData1)

		_, err = parser.Seek(0, io.SeekStart)
		require.NoError(t, err)

		gotData2, err := ioutil.ReadAll(parser)
		require.NoError(t, err)

		require.Equal(t, gotData1, gotData2)
	})
}
