# drun completion: source it, e.g. source <(drun completion bash)
if [ -n "$ZSH_VERSION" ]; then autoload -U +X bashcompinit && bashcompinit; fi
_drun() {
	local args=("${COMP_WORDS[@]:1:$COMP_CWORD}")
	local IFS=$'\n'
	COMPREPLY=($(GO_FLAGS_COMPLETION=1 "${COMP_WORDS[0]}" "${args[@]}"))
	return 0
}
complete -F _drun drun
