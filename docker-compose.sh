#!/bin/bash
cat <<EOF
version: '2'
services:
  ${SERVICE_NAME}:
    image: ${BUILD_IMAGE}
    volumes:
      - .:$PWD
      - $HOME/.cache:/home/$UNAME/.cache
    working_dir: $PWD
    command: /sbin/init
    links:
      - dominant
  dominant:
    image: dr.rbkmoney.com/rbkmoney/dominant:e6af73a005779d5714a1d3b9e310a12f69f6fb0c
    command: /opt/dominant/bin/dominant foreground
    links:
      - machinegun
  machinegun:
    image: dr.rbkmoney.com/rbkmoney/machinegun:e04e529f4c5682b527d12d73a13a3cf9eb296d4d
    command: /opt/machinegun/bin/machinegun  foreground
    volumes:
      - ./test/machinegun/sys.config:/opt/machinegun/releases/0.1.0/sys.config
networks:
  default:
    driver: bridge
    driver_opts:
      com.docker.network.enable_ipv6: "true"
      com.docker.network.bridge.enable_ip_masquerade: "false"
EOF
