FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata
WORKDIR /app

# The binary is pre-compiled on the build machine (Ubuntu PC)
# to avoid heavy compilation CPU/RAM spikes on low-resource VPS
COPY tg_proxy /app/tg_proxy

EXPOSE 8080

VOLUME ["/tmp/tg_file_proxy"]

ENTRYPOINT ["/app/tg_proxy"]
