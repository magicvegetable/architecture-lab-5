FROM golang:1.22 as build

WORKDIR /app
COPY ../../Desktop/silver-fiesta-last-in-4-lab/silver-fiesta-last-in-4-lab .

RUN go test -c ./cmd/lb/...

FROM gcr.io/distroless/base-debian12:latest

WORKDIR /app

ENV UNIT_TEST=1

COPY --from=build /app/lb.test /app

CMD ["./lb.test"]

