# syntax=docker/dockerfile:1.7
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN if [ -f go.sum ]; then go mod download; fi
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/qmax ./cmd/qmax && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/qmax-workbench ./cmd/qmax-workbench && \
    install -d -m 0700 /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/qmax /usr/local/bin/qmax
COPY --from=build /out/qmax-workbench /usr/local/bin/qmax-workbench
COPY --from=build --chown=65532:65532 /out/data /data
VOLUME ["/data"]
EXPOSE 8080 8081
ENTRYPOINT ["/usr/local/bin/qmax"]
CMD ["serve", "--listen", "0.0.0.0:8080", "--allow-non-loopback", "--data-dir", "/data"]
