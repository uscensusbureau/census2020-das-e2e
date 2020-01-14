FROM amazonlinux:2017.03

ENV HOME /home/root
ENV DEBIAN_FRONTEND noninteractive

RUN yum install -y sudo wget cronie openssh-clients screen bzip2

RUN mkdir -p /libx32
RUN mkdir -p /usr/libx32
RUN mkdir -p $HOME/das_files
RUN mkdir -p $HOME/das_decennial
RUN touch $HOME/.bashrc

WORKDIR $HOME

COPY . $HOME/das_decennial/
COPY ./data/EXT1940USCB_small.dat.gz $HOME/das_files/

RUN gunzip $HOME/das_files/EXT1940USCB_small.dat.gz
RUN mv $HOME/das_files/EXT1940USCB_small.dat $HOME/das_files/EXT1940USCB.dat

RUN $HOME/das_decennial/etc/bashrc_prep.sh

ENTRYPOINT ["/bin/bash"]
