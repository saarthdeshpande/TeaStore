#!/bin/bash
# run_all_experiments.sh — TeaStore
#
# Runs experiments across 16 configurations:
#   2 CPU resource configs (res_500m, res_1000m)
#   x 2 manifests (unfixed_config, fixed_config)
#   x 4 scaling policies (cpu_50, cpu_90, memory_50, memory_90)
#
# Each experiment runs for 2 hours.
#
# unfixed_config = no JAVA_TOOL_OPTIONS for any service (current manifest.yaml)
# fixed_config   = JAVA_TOOL_OPTIONS uncommented for teastore-webui
#
# After each experiment, copies results from cpu_memory_/cpu_memory_train.csv
# into the appropriate results directory, then cleans cpu_memory_/.
#
# Usage: ./run_all_experiments.sh </dev/null 2>baselines.log &

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

DURATION="2h"

RESULTS_DIR="$SCRIPT_DIR/experiment_results"
DATA_COLLECTOR="$SCRIPT_DIR/data_collector.py"
DATA_COLLECTOR_BACKUP="$SCRIPT_DIR/data_collector.py.bak"
MANIFEST="$SCRIPT_DIR/manifest.yaml"
MANIFEST_BACKUP="$SCRIPT_DIR/manifest.yaml.bak"

# --- Helper Functions ---

backup_files() {
    echo ">>> Backing up original files..."
    
    if [ -f "$DATA_COLLECTOR_BACKUP" ]; then
        echo ">>> Found existing data_collector.py.bak, restoring it first to prevent contamination..."
        cp "$DATA_COLLECTOR_BACKUP" "$DATA_COLLECTOR"
    else
        cp "$DATA_COLLECTOR" "$DATA_COLLECTOR_BACKUP"
    fi

    if [ -f "$MANIFEST_BACKUP" ]; then
        echo ">>> Found existing manifest.yaml.bak, restoring it first to prevent contamination..."
        cp "$MANIFEST_BACKUP" "$MANIFEST"
    else
        cp "$MANIFEST" "$MANIFEST_BACKUP"
    fi

    if [ -f "$SCRIPT_DIR/run_experiments.sh.bak" ]; then
        echo ">>> Found existing run_experiments.sh.bak, restoring it first to prevent contamination..."
        cp "$SCRIPT_DIR/run_experiments.sh.bak" "$SCRIPT_DIR/run_experiments.sh"
    else
        cp "$SCRIPT_DIR/run_experiments.sh" "$SCRIPT_DIR/run_experiments.sh.bak"
    fi
}

restore_files() {
    echo ">>> Restoring original files..."
    cp "$DATA_COLLECTOR_BACKUP" "$DATA_COLLECTOR"
    cp "$MANIFEST_BACKUP" "$MANIFEST"
    cp "$SCRIPT_DIR/run_experiments.sh.bak" "$SCRIPT_DIR/run_experiments.sh"
    rm -f "$DATA_COLLECTOR_BACKUP"
    rm -f "$MANIFEST_BACKUP"
    rm -f "$SCRIPT_DIR/run_experiments.sh.bak"
}

# set_manifest <config_name>
#   config_name: "unfixed_config" or "fixed_config"
#   unfixed = JAVA_TOOL_OPTIONS commented out for all services (current state)
#   fixed   = JAVA_TOOL_OPTIONS uncommented for teastore-webui
set_manifest() {
    local config_name="$1"

    # Always start from the original manifest
    cp "$MANIFEST_BACKUP" "$MANIFEST"

    if [ "$config_name" = "unfixed_config" ]; then
        echo ">>> Using UNFIXED manifest (no JAVA_TOOL_OPTIONS)"
        # Already the default state — JAVA_TOOL_OPTIONS is commented out
    else
        echo ">>> Using FIXED manifest (JAVA_TOOL_OPTIONS for teastore-webui)"
        # Uncomment the JAVA_TOOL_OPTIONS lines for teastore-webui only.
        # The webui block uses 1500m heap (unique), while others use 128m/256m.
        # We match on "1500m" to target only the webui lines.
        sed -i '/# - name: JAVA_TOOL_OPTIONS/{
            N
            /1500m/{
                s/# //
                s/# //
            }
        }' "$MANIFEST"
    fi
}

# set_cpu_threshold <threshold>
#   Sets all CPU averageUtilization values in data_collector.py
set_cpu_threshold() {
    local threshold="$1"
    echo ">>> Setting CPU averageUtilization to ${threshold}%"
    # In TeaStore's data_collector.py, the CPU metric is built inline with
    # "averageUtilization": 90 — replace it
    sed -i "s/\"averageUtilization\": 90/\"averageUtilization\": ${threshold}/g" "$DATA_COLLECTOR"
}

# modify_for_memory_only <threshold>
#   Modifies data_collector.py so the HPA uses memory instead of CPU
modify_for_memory_only() {
    local threshold="$1"
    echo ">>> Modifying data_collector.py for memory-only HPA at ${threshold}%"

    # In the HPA metrics block, swap "cpu" -> "memory" so the generated HPA
    # targets memory utilization instead of CPU.
    sed -i 's/"name": "cpu"/"name": "memory"/' "$DATA_COLLECTOR"
    # Hardcode the threshold (replace the inline 90)
    sed -i "s/\"averageUtilization\": 90/\"averageUtilization\": ${threshold}/g" "$DATA_COLLECTOR"
}

# set_cpu_resources <req> <lim>
#   Sets CPU request and limit in DEF_all in data_collector.py
set_cpu_resources() {
    local req="$1"
    local lim="$2"
    echo ">>> Setting CPU resources: request=${req}, limit=${lim}"
    # Order matters: replace request first (1000m→req), then limit (2000m→lim)
    sed -i "s/\"cpu\": \"1000m\"/\"cpu\": \"${req}\"/" "$DATA_COLLECTOR"
    sed -i "s/\"cpu\": \"2000m\"/\"cpu\": \"${lim}\"/" "$DATA_COLLECTOR"
}

# run_single_experiment <res_label> <cpu_req> <cpu_lim> <config_name> <metric_type> <threshold>
run_single_experiment() {
    local res_label="$1"     # res_500m or res_1000m
    local cpu_req="$2"       # 500m or 1000m
    local cpu_lim="$3"       # 1000m or 2000m
    local config_name="$4"   # unfixed_config or fixed_config
    local metric_type="$5"   # cpu or memory
    local threshold="$6"     # 50 or 90

    local experiment_label="${res_label}/${config_name}/${metric_type}_${threshold}"
    local output_dir="$RESULTS_DIR/$experiment_label"

    echo ""
    echo "============================================================"
    echo "  EXPERIMENT: $experiment_label"
    echo "  Duration:   $DURATION"
    echo "============================================================"
    echo ""

    # Restore data_collector.py to original before each experiment
    cp "$DATA_COLLECTOR_BACKUP" "$DATA_COLLECTOR"

    # Apply CPU resource config (must be after restore)
    set_cpu_resources "$cpu_req" "$cpu_lim"

    # Set the right manifest
    set_manifest "$config_name"

    # Apply threshold modifications
    if [ "$metric_type" = "cpu" ]; then
        set_cpu_threshold "$threshold"
    else
        modify_for_memory_only "$threshold"
    fi

    # Run the experiment via run_experiments.sh
    echo ">>> Running run_experiments.sh..."
    bash run_experiments.sh "$DURATION"

    # Copy results
    if [ -f "cpu_memory_/cpu_memory_train.csv" ]; then
        mkdir -p "$output_dir"
        cp "cpu_memory_/cpu_memory_train.csv" "$output_dir/"
        echo ">>> Copied results to $output_dir/cpu_memory_train.csv"
    else
        echo ">>> WARNING: cpu_memory_/cpu_memory_train.csv not found!"
    fi

    # Clean up cpu_memory_/ directory
    echo ">>> Cleaning cpu_memory_/ directory..."
    rm -rf cpu_memory_/*

    echo ">>> Experiment $experiment_label complete."
}

# --- Main ---

trap restore_files EXIT

backup_files

# Create results directory structure
mkdir -p "$RESULTS_DIR"

# Ensure run_experiments.sh uses the right flags
# For CPU experiments: -c; for memory experiments: -m
# We handle this by always using -cm (data_collector.py's create_hpa_yaml
# builds metrics based on args). We disable the unwanted metric via sed
# in modify_for_memory_only. For CPU-only, the original code already only
# adds memory metric if args.memory is set, so passing just -c works.
# But run_experiments.sh hardcodes flags=("-cm"), so we change it to -c
# and handle memory via our sed modifications.
sed -i 's/^flags=.*/flags=("-c")/' run_experiments.sh

# Run all 16 experiments (2 resource configs x 2 manifests x 4 scaling policies)
for res_config in "res_500m 500m 1000m" "res_1000m 1000m 2000m"; do
    read -r res_label cpu_req cpu_lim <<< "$res_config"
    for config in unfixed_config fixed_config; do
        for metric in cpu memory; do
            for threshold in 50 90; do
                run_single_experiment "$res_label" "$cpu_req" "$cpu_lim" "$config" "$metric" "$threshold"
            done
        done
    done
done

echo ""
echo "============================================================"
echo "  ALL EXPERIMENTS COMPLETE"
echo "  Results saved in: $RESULTS_DIR/"
echo "============================================================"
echo ""
echo "Directory structure:"
find "$RESULTS_DIR" -type f | sort
