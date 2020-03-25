FROM golang:1.13-alpine

RUN mkdir -p /app

WORKDIR /app

ADD . /app

RUN go build ./.

CMD ["./app"]