TARGET=gowfs

GOOS = linux
GOARCH = amd64

$(TARGET):
	GOOS=${GOOS} GOARCH=${GOARCH} go build -o $@ test-hdfs/main.go
