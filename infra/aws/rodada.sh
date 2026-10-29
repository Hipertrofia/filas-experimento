#!/usr/bin/env bash
# Executa uma rodada completa na AWS e traz os registros para registros/aws/<rótulo>/.
#
# Uso: infra/aws/rodada.sh <worker.properties> <gerador.properties> [--rotulo NOME] [--purge]
#   ex.: infra/aws/rodada.sh experiments/aws-worker-prioridade.properties experiments/aws-fumaca.properties
#
# Passos: gera o trace localmente se faltar → confere relógios → confere filas vazias → sobe os
# workers → envia o trace → espera os workers ficarem ociosos e saírem → baixa os registros.
source "$(dirname "$0")/comum.sh"

[[ $# -ge 2 ]] || die "uso: $0 <worker.properties> <gerador.properties> [--rotulo NOME] [--purge]"
wcfg=$1
gcfg=$2
shift 2
rotulo=""
purge=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --rotulo) rotulo=$2; shift 2 ;;
        --purge) purge=1; shift ;;
        *) die "opção desconhecida: $1" ;;
    esac
done
[[ -f "$wcfg" && -f "$gcfg" ]] || die "configuração não encontrada"
# As instâncias recebem uma cópia de experiments/ e usam os mesmos caminhos relativos.
[[ "$wcfg" == experiments/* && "$gcfg" == experiments/* ]] || die "as configs precisam estar em experiments/"
grep -q '^[[:space:]]*sqs.endpoint' "$wcfg" "$gcfg" && die "remova sqs.endpoint das configs da AWS"

run_id=$(propriedade "$gcfg" run.id)
politica=$(propriedade "$wcfg" worker.policy)
rotulo=${rotulo:-$run_id-$(echo "$politica" | tr 'A-Z_' 'a-z-')-$(date -u +%Y%m%dT%H%M%SZ)}
destino="registros/aws/$rotulo"
[[ ! -e "$destino" ]] || die "$destino já existe"
# Ociosidade antes de o worker sair: maior que qualquer intervalo entre chegadas plausível.
ocioso_ms="${FILAS_OCIOSO_MS:-15000}"

bucket=$(saida_stack Bucket)
workers=$(instancias worker)
gerador=$(instancias gerador)
[[ -n "$workers" ]] || die "nenhum worker ligado (infra/aws/capacidade.sh)"
[[ -n "$gerador" ]] || die "gerador desligado (infra/aws/capacidade.sh)"
log "rodada $rotulo: $(echo $workers | wc -w) workers, política $politica"

trace=$(propriedade "$gcfg" trace.file)
trace=${trace:-experiments/traces/$run_id.csv}
if [[ ! -f "$trace" ]]; then
    log "gerando $trace"
    ./gradlew -q :generator-app:run --args="generate $gcfg"
fi

mkdir -p "$destino"
infra/aws/relogio.sh "$destino/relogio.txt"

filas=$(echo $(propriedade "$wcfg" worker.queues | tr ',' ' '))
for f in $filas; do
    url=$(aws sqs get-queue-url --queue-name "$f" --query QueueUrl --output text)
    if [[ $purge -eq 1 ]]; then
        aws sqs purge-queue --queue-url "$url"
    fi
    n=$(aws sqs get-queue-attributes --queue-url "$url" \
        --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
        --query 'sum(map(&to_number(@), values(Attributes)))' --output text)
    [[ $purge -eq 1 || "$n" == 0 ]] || die "fila $f com ~$n mensagens; use --purge"
done
if [[ $purge -eq 1 ]]; then
    log "purge pedido; o SQS leva até 60 s para concluir"
    sleep 60
fi

aws s3 sync experiments "s3://$bucket/experiments" --delete --quiet
cp "$wcfg" "$gcfg" "$destino/"

log "subindo os workers"
cmd_w=$(ssm_enviar "$workers" 14400 "/opt/filas/app/remoto/worker.sh $wcfg $rotulo $ocioso_ms")
for id in $workers; do
    while :; do
        st=$(ssm_estado "$cmd_w" "$id")
        case "$st" in
            InProgress) break ;;
            Pending | Delayed) sleep 2 ;;
            *) ssm_saida "$cmd_w" "$id" | tail -20; die "worker $id terminou com $st antes da rodada" ;;
        esac
    done
done
# Tempo para a JVM subir e o worker começar o polling.
sleep 20

log "enviando o trace"
cmd_g=$(ssm_enviar "$gerador" 14400 "/opt/filas/app/remoto/gerador.sh $gcfg $rotulo")
gerador_ok=1
ssm_esperar "$cmd_g" "$gerador" || gerador_ok=0
ssm_saida "$cmd_g" "$gerador" | tail -8
[[ $gerador_ok -eq 1 ]] || log "ATENÇÃO: o gerador falhou; os workers vão parar por ociosidade"

log "esperando os workers esvaziarem as filas e ficarem ociosos ($ocioso_ms ms)"
prazo=$(( $(date +%s) + ocioso_ms / 1000 + 120 ))
for id in $workers; do
    while [[ "$(ssm_estado "$cmd_w" "$id")" =~ ^(Pending|InProgress|Delayed)$ ]]; do
        if [[ $(date +%s) -gt $prazo ]]; then
            # Fila vazia há tempo suficiente e o worker não saiu (ex.: nunca recebeu um job).
            log "parando $id com SIGTERM"
            ssm_enviar "$id" 60 "pkill -TERM -f experimento.worker.WorkerMain || true" >/dev/null
            prazo=$(( $(date +%s) + 60 ))
        fi
        sleep 5
    done
    ssm_saida "$cmd_w" "$id" | tail -2
done

aws s3 sync "s3://$bucket/registros/$rotulo" "$destino" --quiet
log "registros em $destino:"
ls -l "$destino"
log "análise: analysis/build/install/analysis/bin/analysis --warmup-s <s> $destino"
