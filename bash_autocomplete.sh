#!/usr/bin/env bash

# Path to your YAML specification file.
SPEC_FILE="./sdflow.yaml"

# Function to generate auto-completion suggestions.
_sdflow_auto_complete() {
    local cur_word="${COMP_WORDS[COMP_CWORD]}"
    local suggestions=$(yq eval 'keys | .[]' "$SPEC_FILE" | grep "^$cur_word")

    COMPREPLY=($(compgen -W "${suggestions}" -- "$cur_word"))
}

# Register the auto-completion function for the 'sdflow' command.
complete -F _sdflow_auto_complete sdflow
