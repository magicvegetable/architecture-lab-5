FROM golang:1.22 as build

WORKDIR /app
COPY ../../Desktop/silver-fiesta-last-in-4-lab/silver-fiesta-last-in-4-lab .

RUN go test -c ./integration

FROM archlinux:latest

WORKDIR /app

COPY --from=build /app/integration.test /app/test

ENV INTEGRATION_TEST=1

CMD ["./test", "-test.bench", ".", "-test.benchtime", "1000x", "&&", "./test"]

