#!/usr/bin/env bash
cd /opt/goes
./simpleserver --db /var/events &>/var/log/goes.log &