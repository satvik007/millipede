#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
OUT="$REPO_ROOT/target/template-smoke"

rm -rf -- "$OUT"
mkdir -p -- "$OUT"

for t in basic-http basic-html basic-browser; do
    generated="$OUT/smoke-$t"

    if command -v cargo-generate >/dev/null 2>&1; then
        cargo generate \
            --path "$REPO_ROOT/templates/$t" \
            --name "smoke-$t" \
            --no-workspace \
            --destination "$OUT"
    else
        cp -R -- "$REPO_ROOT/templates/$t" "$generated"
        rm -- "$generated/cargo-generate.toml"
        temp_manifest=$(mktemp "$generated/Cargo.toml.XXXXXX")
        sed "s/{{project-name}}/smoke-$t/g" "$generated/Cargo.toml" >"$temp_manifest"
        mv -- "$temp_manifest" "$generated/Cargo.toml"
    fi

    printf '\n[patch.crates-io]\nmillipede = { path = "%s/millipede" }\n' \
        "$REPO_ROOT" >>"$generated/Cargo.toml"

    # --offline works after prior workspace builds populate Cargo's local registry cache.
    if ! CARGO_TARGET_DIR="$OUT/target" cargo check \
        --manifest-path "$generated/Cargo.toml" --offline; then
        if [[ "${TEMPLATE_CHECK_ONLINE:-0}" == "1" ]]; then
            CARGO_TARGET_DIR="$OUT/target" cargo check \
                --manifest-path "$generated/Cargo.toml"
        else
            exit 1
        fi
    fi

    echo "PASS: $t"
done
