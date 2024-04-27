SPEC_FILE="./Sdflow.yaml"

_sdflow_auto_complete() {
    local -a suggestions
    local cur_word="$1"

    suggestions=($(sdflow --targets))

    _describe 'command' suggestions
}

# Register the auto-completion function for the 'sdflow' command using compdef (Zsh).
compdef _sdflow_auto_complete sdflow
