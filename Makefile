SOURCE_FILE := `find . -name "*.go" | grep -v "vendor/*"`

fmt:
	gofmt -s -w ${SOURCE_FILE} > /dev/null