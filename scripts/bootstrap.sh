#!/usr/bin/env bash
# Server installation
pushd .

sudo apt-get install -y git build-essential pkg-config

# Install Golang
cd /usr/local
echo 'Downloading and installing Go 1.6 ...'
curl -s https://storage.googleapis.com/golang/go1.6.linux-amd64.tar.gz | tar xz
export GOROOT=/usr/local/go
echo 'export GOROOT=/usr/local/go' > /etc/profile.d/go.sh
export GOPATH=~/go
echo 'export GOPATH=~/go' >> /etc/profile.d/go.sh
export PATH=$PATH:/usr/local/go/bin
echo 'export PATH=$PATH:/usr/local/go/bin' >> /etc/profile.d/go.sh

# Install zeromq
cd ~
echo 'Downloading libsodium-1.0.10 ...'
curl -s https://download.libsodium.org/libsodium/releases/libsodium-1.0.10.tar.gz | tar xz
cd libsodium-1.0.10
./configure
make && make check && sudo make install

sudo ldconfig

cd ~
echo 'Downloading zeromq-4.1.4 ...'
curl -s http://download.zeromq.org/zeromq-4.1.4.tar.gz | tar xz
cd zeromq-4.1.4
./configure
make && make check && sudo make install

sudo ldconfig

popd
