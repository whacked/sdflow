SPEC_FILE="./Sdflow.yaml"

_sdflow_auto_complete() {
    local cur_word="${COMP_WORDS[COMP_CWORD]}"
    local prev_word="${COMP_WORDS[COMP_CWORD-1]}"
    
    # If previous word is -f or --file, complete with file paths
    if [[ "$prev_word" == "-f" || "$prev_word" == "--file" ]]; then
        # Get all files and directories
        local all_completions=($(compgen -f -- "$cur_word"))
        local filtered_completions=()
        
        for item in "${all_completions[@]}"; do
            # Include directories (so user can navigate into them)
            if [[ -d "$item" ]]; then
                filtered_completions+=("$item")
            # Include files with recognized extensions
            elif [[ "$item" =~ \.(yaml|yml|jsonnet|json)$ ]]; then
                filtered_completions+=("$item")
            fi
        done
        
        COMPREPLY=("${filtered_completions[@]}")
    else
        # Check if -f or --file flag was used earlier in the command
        local file_arg=""
        local i=1
        while [[ $i -lt $COMP_CWORD ]]; do
            if [[ "${COMP_WORDS[$i]}" == "-f" || "${COMP_WORDS[$i]}" == "--file" ]]; then
                if [[ $((i + 1)) -lt ${#COMP_WORDS[@]} ]]; then
                    file_arg="${COMP_WORDS[$((i + 1))]}"
                    break
                fi
            fi
            ((i++))
        done
        
        # Get targets using the specified file or default
        if [[ -n "$file_arg" ]]; then
            local suggestions=$(sdflow -f "$file_arg" --targets 2>/dev/null)
        else
            local suggestions=$(sdflow --targets 2>/dev/null)
        fi
        
        COMPREPLY=($(compgen -W "${suggestions}" -- "$cur_word"))
    fi
}

# Register the auto-completion function for the 'sdflow' command.
complete -F _sdflow_auto_complete sdflow
