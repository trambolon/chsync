# --- build stage ---------------------------------------------------------
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/ddl-saver ./cmd/ddl-saver

# --- runtime stage -------------------------------------------------------
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata \
 && addgroup -S ddlsaver && adduser -S -G ddlsaver ddlsaver
USER ddlsaver
COPY --from=build /out/ddl-saver /usr/local/bin/ddl-saver
ENTRYPOINT ["ddl-saver"]
CMD ["run", "-config", "/etc/ddl-saver/config.toml"]
