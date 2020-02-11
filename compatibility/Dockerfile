FROM maven:3-jdk-8-alpine AS buildjava

ENV VERSION=1.11.0

RUN apk add --no-cache ca-certificates git libc6-compat tini
RUN git clone --single-branch --depth=1 --branch=apache-parquet-$VERSION https://github.com/apache/parquet-mr.git

WORKDIR /parquet-mr/parquet-tools
ADD compatibility/shaded-fasterxml-jackson.patch /

## I found this patch from https://aur.archlinux.org/packages/parquet-tools/
RUN patch -Np1 -i "/shaded-fasterxml-jackson.patch"
RUN mvn --batch-mode clean package -Plocal

FROM golang:1.13-alpine AS buildgo

ADD . /go/src/github.com/fraugster/parquet-go
RUN go build -o /buildfile /go/src/github.com/fraugster/parquet-go/compatibility/build.go \
                           /go/src/github.com/fraugster/parquet-go/compatibility/data_model.go
RUN go build -o /compare /go/src/github.com/fraugster/parquet-go/compatibility/compare.go \
                         /go/src/github.com/fraugster/parquet-go/compatibility/data_model.go


FROM openjdk:8-jdk-alpine

ENV VERSION=1.11.0

RUN apk update && apk add --no-cache libc6-compat tini bash
# For an unknown reason, Java ignores the file in the lib64 folder
RUN ln -s /lib64/ld-linux-x86-64.so.2 /lib/ld-linux-x86-64.so.2

COPY --from=buildjava /parquet-mr/parquet-tools/target/parquet-tools-$VERSION.jar /parquet-tools.jar
COPY --from=buildgo /buildfile /buildfile
COPY --from=buildgo /compare /compare
ADD compatibility/data.json /data.json
ADD compatibility/run_tests.bash /run_tests.bash
RUN chmod a+x /run_tests.bash
RUN /run_tests.bash
