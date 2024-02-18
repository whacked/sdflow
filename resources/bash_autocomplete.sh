SPEC_FILE="./Schmakefile"

_schmake_auto_complete() {
    local cur_word="${COMP_WORDS[COMP_CWORD]}"
    local suggestions=$(schmake --targets)

    COMPREPLY=($(compgen -W "${suggestions}" -- "$cur_word"))
}

# Register the auto-completion function for the 'schmake' command.
complete -F _schmake_auto_complete schmake
