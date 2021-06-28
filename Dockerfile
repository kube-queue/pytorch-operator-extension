FROM golang:1.15.3-alpine3.12 as build
RUN apk add make git
ADD . /go/src/github.com/kube-queue/pytorch-operator-extension

WORKDIR /go/src/github.com/kube-queue/pytorch-operator-extension
RUN make

FROM alpine:3.12
COPY --from=build /go/src/github.com/kube-queue/pytorch-operator-extension/bin/pytorch-operator-extension /usr/bin/pytorch-operator-extension
RUN chmod +x /usr/bin/pytorch-operator-extension
ENTRYPOINT ["/usr/bin/pytorch-operator-extension"]