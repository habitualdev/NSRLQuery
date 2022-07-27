build:
	[ ! -d "bin" ] && mkdir bin || echo "dir bin exists"
	go build -o bin/nsrlferret -ldflags "-s -w"
	GOOS=windows go build -o bin/nsrlferret.exe -ldflags "-s -w"
