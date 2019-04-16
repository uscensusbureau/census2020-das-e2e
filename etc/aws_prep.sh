#!/bin/bash
#
# This script preps AWS Linux with packages and config files
# for running the Disclosure Avoidance System.
# It can be re-run without problem.
#

# Install necessary packages
sudo yum install -y git emacs tmux isomd5sum || exit 1
sudo yum update -y || exit 1
sudo yum upgrade -y || exit 1

# Change Java
sudo yum install -y java-1.8.0 || exit 1
sudo yum remove -y java-1.7.0-openjdk || exit 1

# Configure syslog
sudo touch /var/log/local1.log
sudo cp /dev/stdin /etc/rsyslog.d/census.conf <<EOF
local1.*					/var/log/local1.log
EOF
sudo service rsyslog restart

if [ ! -e /usr/bin/md5 ]; then
  sudo ln /usr/bin/md5sum /usr/bin/md5
fi

# set up dynamic DNS. Be sure that a username and password are configured

cat > /tmp/cron$$ <<EOF
@reboot  bash $HOME/bin/ddns-init
EOF
crontab /tmp/cron$$


# Get Spark if we don't have it
SPARK_DIST=http://apache.mirrors.hoobly.com/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz
SPARK_FILE=`basename $SPARK_DIST`
SPARK_DIR=/usr/local/spark
SPARK_TAR=$SPARK_DIR/$SPARK_FILE
if [ ! -d $SPARK_DIR ]; then
  sudo mkdir $SPARK_DIR
  sudo chown $USER $SPARK_DIR
fi
if [ ! -r $SPARK_TAR  ]; then
  wget -O $SPARK_TAR $SPARK_DIST
  cd $SPARK_DIR
  tar xfvz $SPARK_FILE
fi

# Update the user's profile
SPARK_ROOT=$SPARK_DIR/`basename $SPARK_FILE .tgz`
if ! grep $SPARK_ROOT $HOME/.bashrc >/dev/null 2>&1 ; then
  echo "export PATH=\$PATH:$SPARK_ROOT/bin" >> $HOME/.bashrc
fi

# Get Anaconda
ANACONDA_DIST=https://repo.anaconda.com/archive/Anaconda3-5.2.0-Linux-x86_64.sh
ANACONDA_FILE=`basename $ANACONDA_DIST`
ANACONDA_ROOT=/usr/local/anaconda3
ANACONDA_SH=$ANACONDA_ROOT/$ANACONDA_FILE
if [ ! -d $ANACONDA_ROOT ]; then
  sudo mkdir $ANACONDA_ROOT
  sudo chown $USER $ANACONDA_ROOT
fi
if [ ! -r $ANACONDA_SH ]; then
  wget -O $ANACONDA_SH $ANACONDA_DIST
fi
if [ ! -e $ANACONDA_ROOT/bin/python3 ]; then
  sh $ANACONDA_SH -f -b -p $ANACONDA_ROOT
fi


# Update the user's profile
if ! grep /usr/local/anaconda3/bin $HOME/.bashrc >/dev/null 2>&1 ; then
  echo 'export PATH=$PATH:/usr/local/anaconda3/bin' >> $HOME/.bashrc
  echo 'export PYSPARK_PYTHON=/usr/local/anaconda3/bin/python3.6' >> $HOME/.bashrc
  echo 'export PYSPARK_DRIVER_PYTHON=/usr/local/anaconda3/bin/python3.6' >> $HOME/.bashrc
fi
