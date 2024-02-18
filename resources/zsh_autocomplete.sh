SPEC_FILE="./Schmakefile"

_schmake_auto_complete() {
    local -a suggestions
    local cur_word="$1"

    suggestions=($(schmake --targets))

    _describe 'command' suggestions
}

# Register the auto-completion function for the 'schmake' command using compdef (Zsh).
compdef _schmake_auto_complete schmake
