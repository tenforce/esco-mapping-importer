FROM registry.tenforce.com/docker/esco/scala-template:2.1.1
MAINTAINER Cecile Tonglet <cecile.tonglet@tenforce.com>

ENV GRAPH http://mu.semte.ch/application
ENV DATA_DIR /data

VOLUME /data
