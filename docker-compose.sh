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
    image: dr.rbkmoney.com/rbkmoney/dominant:f3c72168d9dfeb4da241d4eb5d6a29787c81faef
    command: /opt/dominant/bin/dominant foreground
    links:
      - machinegun
  machinegun:
    image: dr.rbkmoney.com/rbkmoney/machinegun:a48f9e93dd5a709d5f14db0c9785d43039282e86
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
