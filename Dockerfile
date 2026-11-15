# One image, three binaries (D-004): compose runs it as migrate, api and worker.
# Plain RUN steps (no BuildKit cache mounts) so it builds with any Docker builder.

FROM golang:1.27.4-alpine AS build
WORKDIR /src

# Dependencies first, so this layer is reused until go.mod/go.sum change.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
USER nonroot:nonroot
EXPOSE 8080
CMD ["/usr/local/bin/api"]
