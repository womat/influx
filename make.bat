set GOARCH=arm
set GOOS=linux

go build -o .\bin\influx config.go influx.go


set GOARCH=386
set GOOS=windows

go build -o .\bin\influx.exe config.go influx.go


rem go tool dist install -v pkg/runtime
rem go install -v -a std