GoLang implementation of a simple EventStore

# Getting started

## Pre-requisites

- Install [GoLang](https://golang.org/doc/install) version 1.6+
- Install [libsodium](https://download.libsodium.org/libsodium/releases/)\* version 1.0.10+ (Linux only^)
- Install [zeromq](http://zeromq.org/intro:get-the-software)\* version 4.0+ (Linux only^)
- Install [msys2](https://msys2.github.io/) (Windows only)

\* On Linux libsodium and zeromq are installed from source (`./configure && make && sudo make install && sudo ldconfig`)  
^ On Window libzmq and libsodium are installed using pacman in MSYS2 shell (`pacman -S mingw-w64-x86_64-zeromq`)

You can look at [scripts/bootstrap.sh](https://github.com/adymitruk/goes/blob/master/scripts/bootstrap.sh) to get an idea on how to install all the pre-requisites.

## Build

### Fetching GO packages

In your GOPATH folder, execute the following commands:

  `go get github.com/adymitruk/goes`  
  `go get github.com/pebbe/zmq4`  
  `go get github.com/satori/go.uuid`  
  
### Compiling the binary

In your GOPATH folder, execute the following command:

  `go build -o bin/goes src/github.com/adymitruk/goes/simpleserver/simpleserver.go`
  
\* Use `-o bin/goes.exe` on Windows

## Running the server

In your GOPATH folder, execute the following command:

  `./bin/goes --db=./events --addr=tcp://127.0.0.1:12345`

Both flags are optional and their default values are the same as the example.
