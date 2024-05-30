FROM golang:1.22 as build

WORKDIR /go/src/practice-4
COPY ../../Desktop/silver-fiesta-last-in-4-lab/silver-fiesta-last-in-4-lab .

RUN go test ./...
ENV CGO_ENABLED=0
RUN go install ./cmd/...

# ==== Final image ====
FROM alpine:latest
WORKDIR /opt/practice-4
COPY entry.sh /opt/practice-4/
COPY --from=build /go/bin/* /opt/practice-4
ENTRYPOINT ["/opt/practice-4/entry.sh"]
CMD ["server"]