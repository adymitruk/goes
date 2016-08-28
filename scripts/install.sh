#!/usr/bin/env bash
# Install Goes
go get github.com/satori/go.uuid
go get github.com/pebbe/zmq4
go build -o bin/goes

sudo mkdir /opt/goes
sudo cp -R bin /opt/goes
sudo cp -R scripts /opt/goes

# Reminders
echo Goes reminders:
echo - Set the system TimeZone for expected day projection
echo '- Configure start script (/opt/goes/scripts/start.sh)'
