FROM golang:1.20.2 as gobuilder

WORKDIR /root

ENV GOOS=linux
ENV GOARCH=amd64

COPY . /root
# RUN go mod edit -replace github.com/fluent/fluent-bit-go=github.com/fluent/fluent-bit-go@master
# RUN go mod download
RUN go build -buildmode=c-shared -o clickhouse.so .

FROM fluent/fluent-bit:2.1.8-debug

COPY --from=gobuilder /root/clickhouse.so /fluent-bit/bin/
COPY --from=gobuilder /root/fluent-bit.conf /fluent-bit/etc/

EXPOSE 2020

CMD ["/fluent-bit/bin/fluent-bit", "--plugin", "/fluent-bit/bin/clickhouse.so", "--config", "/fluent-bit/etc/fluent-bit.conf"]