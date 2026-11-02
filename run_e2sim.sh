#!/bin/bash
# Call as ./run_e2sim.sh e2term_ip e2term_port
set -euo pipefail
cd -- "$(dirname -- "$0")"
if [[ $# != 2 ]]; then
    echo "Usage: $0 e2term_ip e2term_port" >&2
    exit 2
fi
binary=${E2SIM_BINARY:-./e2sm_examples/kpm_e2sm/build_seqwrap/src/kpm/kpm_sim}
source_dir=./e2sm_examples/kpm_e2sm/src
if [[ ! -x "$binary" ]] || [[ -n "$(find "$source_dir" -type f \( -name '*.cpp' -o -name '*.hpp' -o -name '*.c' -o -name '*.h' \) -newer "$binary" -print -quit)" ]]; then
    echo "Build the current sequence-wrap implementation before starting: $binary" >&2
    echo "cmake --build e2sm_examples/kpm_e2sm/build_seqwrap -j 4" >&2
    exit 1
fi
echo "Starting E2Sim: $binary"
exec "$binary" "$1" "$2"

