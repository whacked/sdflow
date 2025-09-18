SPEC_FILE="./Sdflow.yaml"

_sdflow_files() {
    local -a files
    files=(${(f)"$(find . -maxdepth 1 -type d -o -name '*.yaml' -o -name '*.yml' -o -name '*.jsonnet' -o -name '*.json' 2>/dev/null | sed 's|^\./||')"})
    _describe 'flow definition files' files
}

_sdflow_auto_complete() {
    local context curcontext="$curcontext" state line
    local -a arguments
    typeset -A opt_args
    
    arguments=(
        '(-f --file)'{-f,--file}'[specify flow definition file]:file:_sdflow_files'
        '*:targets:->targets'
    )
    
    _arguments -C $arguments && return 0
    
    case $state in
        targets)
            local -a suggestions
            # Use the file specified with -f if available, otherwise use default
            if [[ -n "${opt_args[-f]}" ]]; then
                suggestions=($(sdflow -f "${opt_args[-f]}" --targets 2>/dev/null))
            elif [[ -n "${opt_args[--file]}" ]]; then
                suggestions=($(sdflow -f "${opt_args[--file]}" --targets 2>/dev/null))
            else
                suggestions=($(sdflow --targets 2>/dev/null))
            fi
            _describe 'targets' suggestions
            ;;
    esac
}

# Register the auto-completion function for the 'sdflow' command using compdef (Zsh).
compdef _sdflow_auto_complete sdflow
