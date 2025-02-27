#!/bin/bash
set -o errexit -o nounset -o pipefail
cd "$(dirname "$0")/.."

yarn shadow-cljs release test
node target/test.js