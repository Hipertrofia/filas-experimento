# Funções comuns dos scripts de infra/aws. Uso: source "$(dirname "$0")/comum.sh"
# Roda no Git Bash (Windows), Linux ou macOS, com o AWS CLI v2 configurado.

set -euo pipefail

# Raiz do repositório: os caminhos (experiments/, registros/) são relativos a ela.
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

STACK="${FILAS_STACK:-filas-experimento}"
export AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"
# No Git Bash, impede que argumentos como /opt/filas virem caminhos do Windows.
export MSYS_NO_PATHCONV=1
# O AWS CLI no Windows imprime com a página de código do console; a saída remota tem acentos.
export PYTHONUTF8=1 PYTHONIOENCODING=utf-8

# O aws.exe do Windows termina as linhas com \r, o que estraga variáveis no bash.
aws() {
    command aws "$@" | tr -d '\r'
}

log() {
    echo "[$(basename "$0" .sh)] $*" >&2
}

die() {
    log "ERRO: $*"
    exit 1
}

saida_stack() {
    aws cloudformation describe-stacks --stack-name "$STACK" \
        --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text
}

# Instâncias em execução de um papel (worker ou gerador), separadas por espaço.
instancias() {
    aws ec2 describe-instances \
        --filters "Name=tag:filas-stack,Values=$STACK" "Name=tag:filas-papel,Values=$1" \
                  "Name=instance-state-name,Values=running" \
        --query 'Reservations[].Instances[].InstanceId' --output text | tr '\t' ' ' | xargs
}

# ssm_enviar "<ids>" <timeout_s> <comando>...  → imprime o CommandId.
# Os comandos não podem ter aspas duplas nem barras invertidas (vão dentro de JSON).
ssm_enviar() {
    local ids=$1 timeout=$2 cmds="" c
    shift 2
    for c in "$@"; do
        cmds+="${cmds:+,}\"$c\""
    done
    # shellcheck disable=SC2086
    aws ssm send-command --document-name AWS-RunShellScript --instance-ids $ids \
        --parameters "{\"commands\":[$cmds],\"executionTimeout\":[\"$timeout\"]}" \
        --query Command.CommandId --output text
}

ssm_estado() {
    aws ssm get-command-invocation --command-id "$1" --instance-id "$2" \
        --query Status --output text 2>/dev/null || echo Pending
}

# ssm_esperar <command_id> "<ids>"  → falha se alguma instância não terminou com sucesso.
ssm_esperar() {
    local cmd=$1 ids=$2 id st falhou=0
    for id in $ids; do
        while :; do
            st=$(ssm_estado "$cmd" "$id")
            case "$st" in
                Success) break ;;
                Pending | InProgress | Delayed) sleep 3 ;;
                *) log "$id terminou com $st"; falhou=1; break ;;
            esac
        done
    done
    return $falhou
}

ssm_saida() {
    aws ssm get-command-invocation --command-id "$1" --instance-id "$2" \
        --query '[StandardOutputContent,StandardErrorContent]' --output text
}

# Valor de uma chave num .properties (sem expansão de variáveis).
propriedade() {
    sed -n "s/^[[:space:]]*$2[[:space:]]*=[[:space:]]*//p" "$1" | tail -1 | tr -d '\r'
}
