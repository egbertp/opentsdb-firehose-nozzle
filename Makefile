BINARY=opentsdb-firehose-nozzle
VERSION_TAG=`git describe 2>/dev/null | cut -f 1 -d '-' 2>/dev/null`
COMMIT_HASH=`git rev-parse --short=8 HEAD 2>/dev/null`
BUILD_TIME=`date +%FT%T%z`
LDFLAGS=-ldflags "-s -w \
	-X main.CommitHash=${COMMIT_HASH} \
	-X main.BuildTime=${BUILD_TIME} \
	-X main.VersionTag=${VERSION_TAG}"

all: build

clean:
	go clean
	if [ -f ${BINARY} ] ; then rm ${BINARY} ; fi
	rm -rf ./target || true

release: clean linux darwin windows

# Installs our project: copies binaries
install:
	go install ${LDFLAGS}

build:
	if [ -f ${BINARY} ] ; then rm ${BINARY} ; fi
	go build -o ${BINARY} ${LDFLAGS}

linux:
	GOOS=linux GOARCH=386 go build ${LDFLAGS} -o ./target/linux_386/${BINARY}
	GOOS=linux GOARCH=amd64 go build ${LDFLAGS} -o ./target/linux_amd64/${BINARY}

darwin:
	GOOS=darwin GOARCH=386 go build ${LDFLAGS} -o ./target/darwin_386/${BINARY}
	GOOS=darwin GOARCH=amd64 go build ${LDFLAGS} -o ./target/darwin_amd64/${BINARY}

windows:
	GOOS=windows GOARCH=386 go build ${LDFLAGS} -o ./target/windows_386/${BINARY}.exe
	GOOS=windows GOARCH=amd64 go build ${LDFLAGS} -o ./target/windows_amd64/${BINARY}.exe


.PHONY: build