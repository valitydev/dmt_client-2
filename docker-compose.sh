#!/bin/bash
cat <<EOF
version: '2.1'
services:
  ${SERVICE_NAME}:
    image: ${BUILD_IMAGE}
    volumes:
      - .:$PWD
      - $HOME/.cache:/home/$UNAME/.cache
    working_dir: $PWD
    command: /sbin/init
    depends_on:
      dominant:
        condition: service_healthy
  dominant:
    image: dr.rbkmoney.com/rbkmoney/dominant:7dddf0d39cb2c58e0fb50276326901fcdd6672d7
    command: /opt/dominant/bin/dominant foreground
    depends_on:
      machinegun:
        condition: service_healthy
    healthcheck:
      test: "curl http://localhost:8022/"
      interval: 5s
      timeout: 1s
      retries: 12
  machinegun:
    image: dr.rbkmoney.com/rbkmoney/machinegun:c3980e3de6fb85f20a6d53d26e3866866d00c5d7
    command: /opt/machinegun/bin/machinegun foreground
    volumes:
      - ./test/machinegun/config.yaml:/opt/machinegun/etc/config.yaml
    healthcheck:
      test: "curl http://localhost:8022/"
      interval: 5s
      timeout: 1s
      retries: 12
EOF
