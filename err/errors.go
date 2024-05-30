package err

import (
	"fmt"
	"strings"
)

func FormatError(err error, format string, args ...any) error {
	head := fmt.Sprintf(format, args...)

	if err == nil {
		return fmt.Errorf(head)
	}

	prefix := "\n\t| "

	errPrefixed := strings.ReplaceAll(err.Error(), "\n", prefix)

	errPrefixed = prefix + errPrefixed

	return fmt.Errorf(head+":%v\n", errPrefixed)
}
