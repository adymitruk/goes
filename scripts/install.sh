#!/usr/bin/env bash
# Install Goes
go get github.com/satori/go.uuid
go get github.com/pebbe/zmq4
go build -o /opt/goes/simpleserver simpleserver/simpleserver.go
cp -R scripts /opt/goes

# Reminders
echo Goes reminders:
echo - Set the system TimeZone for expected day projection
echo - Configure start script (/opt/goes/scripts/start.sh)