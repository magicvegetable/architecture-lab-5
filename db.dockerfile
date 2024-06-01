FROM golang:1.22 as build

WORKDIR /app
COPY . .

RUN go build -o db ./cmd/db/...

RUN mkdir -p /app/store/

FROM gcr.io/distroless/base-debian12:latest

WORKDIR /app

ENV UNIT_TEST=1

COPY --from=build /app/db /app

COPY --from=build /app/store/ /app/store/

CMD ["./db", "--db-dir=/app/store"]

