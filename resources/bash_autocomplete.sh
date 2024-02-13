SPEC_FILE="./sdflow.yaml"

_sdflow_auto_complete() {
    local cur_word="${COMP_WORDS[COMP_CWORD]}"
    local suggestions=$(sdflow --targets)

    COMPREPLY=($(compgen -W "${suggestions}" -- "$cur_word"))
}

# Register the auto-completion function for the 'sdflow' command.
complete -F _sdflow_auto_complete sdflow
