#!/bin/bash
#
# This script preps AWS Linux with packages and config files
# for running the Disclosure Avoidance System.
# It can be re-run without problem.
#

GUROBI_INSTALLER=/usr/local/src/gurobi7.5.2_linux64.tar.gz
GUROBI_BASE=/usr/local/gurobi752
GUROBI_HOME=$GUROBI_BASE/linux64
GRB_LICENSE_FILE=$HOME/gurobi.lic

if [ ! -r $GUROBI_INSTALLER ]; then
  echo Gurobi installer not found, will try downloading
  sudo curl -o $GUROBI_INSTALLER \
  https://packages.gurobi.com/7.5/gurobi7.5.2_linux64.tar.gz
fi

if [ ! -r $GUROBI_INSTALLER ]; then
  echo GUROBI INSTALLER NOT DETECTED
  echo You will need to manually download and install gurobi.
  echo Instructions here: http://www.gurobi.com/registration/download-reg
  echo Please put the file in /usr/local/src/`basename $GUROBI_INSTALLER`
  echo and re-run this script.
  exit 1
fi

# Install Gurobi
if [ ! -d $GUROBI_BASE ]; then
    echo Installing gurobi
    cd `dirname $GUROBI_BASE`
    sudo tar xfz $GUROBI_INSTALLER
    sudo chown -R $USER $GUROBI_BASE
else
    echo Gurobi already installed
fi

GUROBI_LIBDIR=$GUROBI_BASE/linux64/lib
if ! grep LD_LIBRARY_PATH $HOME/.bashrc >/dev/null ; then
  echo "export LD_LIBRARY_PATH=$GUROBI_LIBDIR" >> $HOME/.bashrc
  echo added Gurobi to LD_LIBRARY_PATH
else
  echo Gurobi already on LD_LIBRARY_PATH
fi

echo Setting up /etc/ld.so.conf.d/gurobi.conf
sudo cp /dev/stdin /etc/ld.so.conf.d/gurobi.conf <<EOF
$GUROBI_LIBDIR
EOF
echo Gurobi library installed:
sudo ldconfig -v | grep -i gurobi


# add Gurobi executables to path and update bashrc
if ! grep "GUROBI_HOME" $HOME/.bashrc >/dev/null ; then
  echo "export GUROBI_HOME=$GUROBI_BASE/linux64" >> $HOME/.bashrc
  echo added GUROBI_HOME to .bashrc
else
  echo GUROBI_HOME already in .bashrc
fi

if ! grep "PATH.*${GUROBI_HOME}/bin" $HOME/.bashrc >/dev/null ; then
  echo "export PATH=\$PATH:${GUROBI_HOME}/bin" >> $HOME/.bashrc
  echo added Gurobi bin to PATH
else
  echo Gurobi already on PATH
fi

if [ ! -r $GRB_LICENSE_FILE ]; then
  echo Gurobi license is not found
  echo For information on obtaining an academic license and installing it on EC2
  echo Please see the following resources:
  echo http://www.gurobi.com/academia/academia-center
  echo https://github.com/snuspl/cruise/wiki/A-guide-to-validate-Gurobi-Academic-License-on-EC2
  echo https://stackoverflow.com/questions/2241063/bash-script-to-setup-a-temporary-ssh-tunnel
  echo After obtaining a license, rerun this script
  exit 1
else
  echo Gurobi license found: $GRB_LICENSE_FILE
fi


# todo: clean this up
ANACONDA_ROOT=/usr/local/anaconda3
if [ ! -d $ANACONDA_ROOT ]; then
  echo Please install Anaconda and rerun this script
  exit 1
else
  echo Installing Gurobi python3.6 support
  cd $GUROBI_HOME
  $ANACONDA_ROOT/bin/python3.6 setup.py install
fi
