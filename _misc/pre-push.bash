#!/bin/bash
set -e

echo "linting... "
make lint
echo "lint OK"
echo "testing... "
make test-race
echo "tests OK"
echo "building... "
make build
echo "build OK"
