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
    image: dr.rbkmoney.com/rbkmoney/dominant:b79f1e6acf5fd07ac60a51f9551faca48115770f
    command: /opt/dominant/bin/dominant foreground
    links:
      - machinegun
  machinegun:
    image: dr.rbkmoney.com/rbkmoney/machinegun:2c956c1172cf8f7b4a09512cd1571bdd4c57f1c1
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
