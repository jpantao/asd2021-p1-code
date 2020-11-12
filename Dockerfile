FROM openjdk:14-alpine

RUN apk add --no-cache \
                bind-tools \
                iproute2 \
                nload

WORKDIR code
ADD docker/* ./
ADD configs/* ./
ADD log4j2.xml .
ADD target/asdProj.jar .


ENTRYPOINT ["./setupTc.sh"]
