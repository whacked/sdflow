#!/usr/env/bin zsh

# Path to your YAML specification file.
SPEC_FILE="./sdflow.yaml"

_sdflow_auto_complete() {
    local -a suggestions
    local cur_word="$1"

    # Using yq to parse the YAML file and grep to filter entries starting with the current word
    suggestions=($(yq eval 'keys | .[]' "$SPEC_FILE" | grep "^$cur_word"))

    _describe 'command' suggestions
}

# Register the auto-completion function for the 'sdflow' command using compdef (Zsh).
compdef _sdflow_auto_complete sdflow
