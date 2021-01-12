#!/bin/bash

set -Eeuxoa pipefail

BYTECODE=$(solc --bin-runtime erc20_b.sol | sed -n 4p)

make hack && ./build/bin/hack --action cfg --mode worker --bytecode $BYTECODE