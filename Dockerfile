FROM golang:1.22 as build

WORKDIR /go/src/practice-4
COPY . .

RUN go test ./...
ENV CGO_ENABLED=0
RUN go install ./cmd/...
RUN chmod +x entry.sh

# ==== Final image ====
FROM alpine:latest
WORKDIR /opt/practice-4
COPY --chmod=764 entry.sh /opt/practice-4/
COPY --from=build /go/bin/* /opt/practice-4
ENTRYPOINT ["/opt/practice-4/entry.sh"]
CMD ["server", "--db-addr-base=db:8070"]
