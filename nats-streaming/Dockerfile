FROM golang:1.9.2

ARG command

ADD . /go/src/github.com/chadit/MessageQueueExamples/nats-streaming 
WORKDIR /go/src/github.com/chadit/MessageQueueExamples/nats-streaming/nats
RUN /bin/bash -c "CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build"
CMD ["${command}"]