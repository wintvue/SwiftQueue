#!/bin/sh
#
# Script to run the SwiftQueue program.

set -e # Exit on error

# Copied from .codecrafters/compile.sh
#
(
  cd "$(dirname "$0")" # Ensure compile steps are run within the repository directory
  go build -o /tmp/swift-queue app/*.go
)

# Run program 
exec /tmp/swift-queue "$@"
