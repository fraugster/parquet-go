package floor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTime(t *testing.T) {
	testData := []struct {
		h, m, s, ns   int
		expectErr     bool
		expectedValue Time
		expectedStr   string
	}{
		{h: 0, m: 0, s: 30, ns: 0, expectErr: false, expectedValue: Time{nsec: 30000000000}, expectedStr: "00:00:30"},
		{h: -1, expectErr: true},
		{h: 24, expectErr: true},
		{m: -30, expectErr: true},
		{m: 76, expectErr: true},
		{s: -100, expectErr: true},
		{s: 200, expectErr: true},
		{ns: -1, expectErr: true},
		{ns: 30000000000, expectErr: true},
		{h: 13, m: 45, s: 35, expectErr: false, expectedValue: Time{nsec: 49535000000000}, expectedStr: "13:45:35"},
		{h: 13, m: 45, s: 35, ns: 300, expectErr: false, expectedValue: Time{nsec: 49535000000300}, expectedStr: "13:45:35.000000300"},
		{h: 13, m: 45, s: 35, ns: 300000000, expectErr: false, expectedValue: Time{nsec: 49535300000000}, expectedStr: "13:45:35.300000000"},
	}

	for idx, tt := range testData {
		toime, err := NewTime(tt.h, tt.m, tt.s, tt.ns)
		if tt.expectErr {
			assert.Error(t, err, "%d. expected error but got none", idx)
		} else {
			assert.NoError(t, err, "%d. expected no error but got one", idx)
			assert.Equal(t, tt.expectedValue, toime, "%d. doesn't match", idx)
			assert.Equal(t, tt.expectedStr, toime.String(), "%d. string doesn't match", idx)
			assert.Equal(t, tt.ns/1000, toime.Microsecond(), "%d. microsecond doesn't match", idx)
			assert.Equal(t, tt.ns/1000000, toime.Millisecond(), "%d. millisecond doesn't match", idx)

			today := toime.Today()
			assert.Equal(t, tt.h, today.Hour(), "%d. hour doesn't match", idx)
			assert.Equal(t, tt.m, today.Minute(), "%d. minute doesn't match", idx)
			assert.Equal(t, tt.s, today.Second(), "%d. second doesn't match", idx)
		}
	}
}
