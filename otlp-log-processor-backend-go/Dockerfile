FROM golang:1.23-alpine AS builder

ENV CGO_ENABLED=0 GOOS=linux

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build the application with reduced binary size by stripping debug information
RUN go build -o log-processor-backend -ldflags="-s -w" .

# Stage 2: Create a minimal production image
FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/log-processor-backend .

EXPOSE 4317

ENTRYPOINT ["./log-processor-backend"]

CMD ["-listenAddr", "0.0.0.0:4317", "-key", "service.name", "-window", "30", "-loglevel", "INFO"]