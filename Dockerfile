FROM --platform=$BUILDPLATFORM golang:1.27 AS build
ARG TARGETOS TARGETARCH VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w -X main.version=$VERSION" -o /correlated-telemetrygen .

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /correlated-telemetrygen /correlated-telemetrygen
ENTRYPOINT ["/correlated-telemetrygen"]
