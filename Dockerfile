FROM debian:buster-slim

ADD ./bin/mydumper /usr/bin/mydumper
ADD ./bin/myloader /usr/bin/