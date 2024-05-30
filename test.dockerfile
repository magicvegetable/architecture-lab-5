FROM golang:1.22 as build

WORKDIR /app
COPY . .

RUN go test -c ./integration

FROM archlinux:latest

WORKDIR /app

COPY --from=build /app/integration.test /app/test

ENV INTEGRATION_TEST=1

CMD ["./test", "-test.bench", ".", "-test.benchtime", "1000x", "&&", "./test"]

