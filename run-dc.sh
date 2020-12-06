#!/bin/bash

set -Eeuxoa pipefail

BYTECODE=$(solc --bin-runtime deposit.sol | sed -n 4p)

make hack && ./build/bin/hack --action cfg --mode worker --bytecode $BYTECODE