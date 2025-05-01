# Build Stage
FROM golang:1.24-alpine AS builder

# Install Git for go get if needed
RUN apk add --no-cache git

WORKDIR /app

# Copy and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the code
COPY . .

# Build the Go binary
RUN go build -o flux_balancer ./cmd/flux_balancer

# Runtime Stage
FROM alpine:3.20

# Install useful debugging tools (optional for prod)
RUN apk add --no-cache bash curl vim net-tools iproute2

WORKDIR /app

# Copy the built binary from builder
COPY --from=builder /app/flux_balancer .

# Expose the internal app port (matches your Go server's Listen port)
EXPOSE 8080

# Run the app
CMD ["./flux_balancer"]
