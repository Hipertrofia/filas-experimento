#!/usr/bin/env bash
# Confere o relógio do gerador e dos pods antes de uma rodada.
# Uso: infra/aws/relogio.sh [arquivo de saída]
# Falha se o desvio máximo possível entre duas instâncias (soma dos dois maiores limites de erro
# do chrony) passar de FILAS_RELOGIO_LIMITE_US (padrão 1000 µs = 5% de um job de 20 ms).
source "$(dirname "$0")/comum.sh"

limite_us="${FILAS_RELOGIO_LIMITE_US:-1000}"
ids=$(echo $(instancias gerador) $(instancias worker))
[[ -n "$ids" ]] || die "nenhuma instância ligada"

cmd=$(ssm_enviar "$ids" 60 "/opt/filas/app/remoto/relogio.sh")
ssm_esperar "$cmd" "$ids" || die "falha ao consultar o chrony (comando $cmd)"

linhas=""
for id in $ids; do
    linhas+="$(aws ssm get-command-invocation --command-id "$cmd" --instance-id "$id" \
        --query StandardOutputContent --output text)"$'\n'
done

relatorio=$(printf '%s' "$linhas" | awk -v limite="$limite_us" '
    NF == 4 {
        printf "  %-20s ref %-16s offset %9.1f µs  limite de erro %8.1f µs\n", $1, $2, $3, $4
        if ($4 > a) { b = a; a = $4 } else if ($4 > b) { b = $4 }
        if ($2 != "169.254.169.123" && $2 != "fd00:ec2::123" && $2 !~ /^PHC/) ruim = ruim " " $1
    }
    END {
        printf "  desvio máximo entre instâncias: %.1f µs (limite %d µs)\n", a + b, limite
        if (ruim != "") printf "  ATENÇÃO: fora do Amazon Time Sync:%s\n", ruim
        exit (a + b > limite || ruim != "") ? 1 : 0
    }') && ok=1 || ok=0

echo "$relatorio"
if [[ $# -ge 1 ]]; then
    mkdir -p "$(dirname "$1")"
    { date -u +%FT%TZ; echo "$relatorio"; } >> "$1"
fi
[[ $ok -eq 1 ]] || die "relógios fora do limite; espere o chrony convergir (minutos após o boot) e repita"
log "relógios dentro do limite"
