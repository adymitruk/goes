#!/usr/bin/env bash
# Server installation
sudo -i
cd ~

apt-get install git buildessential

# Install Golang
pushd .
wget -q https://storage.googleapis.com/golang/go1.6.linux-amd64.tar.gz

cd /usr/local
tar -xvf ~/go1.6.linux-amd64.tar.gz
export GOROOT=/usr/local/go
echo 'export GOROOT=/usr/local/go' > /etc/profile.d/go.sh
export PATH=$PATH:/usr/local/go/bin
echo 'export PATH=$PATH:/usr/local/go/bin' >> /etc/profile.d/go.sh
popd

# Install zeromq
pushd .
wget -q http://download.zeromq.org/zeromq-4.1.4.tar.gz

tar -xvf zeromq-4.1.4.tar.gz
cd zeromq-4.1.4
./configure
make && make check && make install
popd

# Install Goes
pushd .
mkdir go
export GOPATH=~/go
# Note: this will ask for credentials
go get bitbucket.org/nicdex/adaptech-goes
cd $GOPATH/src/bitbucket.org/nicdex/adaptech-goes
./scripts/install.sh
popd
