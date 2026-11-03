#!/usr/bin/env bash
# Executa uma rodada completa na AWS e traz os registros para registros/aws/<rótulo>/.
#
# Uso: infra/aws/rodada.sh <workers> <gerador.properties> [--rotulo NOME] [--purge]
#   <workers>: uma config de worker para todos os pods, ou grupos <config>:<n>,<config> em que o
#   grupo sem :n fica com os pods restantes (pods em ordem de instance-id). Ex.:
#     infra/aws/rodada.sh experiments/aws-worker-prioridade.properties experiments/aws-fumaca.properties
#     infra/aws/rodada.sh experiments/aws-worker-reservado-prio.properties:1,experiments/aws-worker-reservado-normal.properties \
#         experiments/serie/c4-s1-sqs.properties
#
# Com queue.backend=RABBITMQ nas configs, a rodada usa o broker da stack (capacidade.sh … 1).
# Passos: gera o trace localmente se faltar → confere relógios → confere filas vazias → sobe os
# workers → envia o trace → espera os workers ficarem ociosos e saírem → baixa os registros.
source "$(dirname "$0")/comum.sh"

[[ $# -ge 2 ]] || die "uso: $0 <workers> <gerador.properties> [--rotulo NOME] [--purge]"
especificacao=$1
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

cfgs=()
qtds=()
IFS=',' read -ra itens <<< "$especificacao"
for it in "${itens[@]}"; do
    if [[ "$it" == *:* ]]; then
        cfgs+=("${it%%:*}")
        qtds+=("${it##*:}")
    else
        cfgs+=("$it")
        qtds+=("")
    fi
done
[[ -f "$gcfg" ]] || die "configuração não encontrada: $gcfg"
# As instâncias recebem uma cópia de experiments/ e usam os mesmos caminhos relativos.
[[ "$gcfg" == experiments/* ]] || die "as configs precisam estar em experiments/"
for c in "${cfgs[@]}"; do
    [[ -f "$c" ]] || die "configuração não encontrada: $c"
    [[ "$c" == experiments/* ]] || die "as configs precisam estar em experiments/"
done
grep -q '^[[:space:]]*sqs.endpoint' "${cfgs[@]}" "$gcfg" && die "remova sqs.endpoint das configs da AWS"

run_id=$(propriedade "$gcfg" run.id)
politica=$(propriedade "${cfgs[0]}" worker.policy)
backend=$(propriedade "$gcfg" queue.backend | tr 'a-z' 'A-Z')
filas=""
for c in "${cfgs[@]}"; do
    [[ "$(propriedade "$c" worker.policy)" == "$politica" ]] || die "configs de worker com worker.policy diferentes"
    [[ "$(propriedade "$c" queue.backend | tr 'a-z' 'A-Z')" == "$backend" ]] \
        || die "worker e gerador com queue.backend diferentes ($c)"
    for f in $(propriedade "$c" worker.queues | tr ',' ' '); do
        [[ " $filas " == *" $f "* ]] || filas="$filas $f"
    done
done
sufixo=""
[[ "$backend" == RABBITMQ ]] && sufixo="-rabbitmq"
rotulo=${rotulo:-$run_id-$(echo "$politica" | tr 'A-Z_' 'a-z-')$sufixo-$(date -u +%Y%m%dT%H%M%SZ)}
destino="registros/aws/$rotulo"
[[ ! -e "$destino" ]] || die "$destino já existe"
# Ociosidade antes de o worker sair: maior que qualquer intervalo entre chegadas plausível.
ocioso_ms="${FILAS_OCIOSO_MS:-15000}"

bucket=$(saida_stack Bucket)
workers=$(instancias worker | tr ' ' '\n' | sort | xargs)
gerador=$(instancias gerador)
[[ -n "$workers" ]] || die "nenhum worker ligado (infra/aws/capacidade.sh)"
[[ -n "$gerador" ]] || die "gerador desligado (infra/aws/capacidade.sh)"
if [[ "$backend" == RABBITMQ ]]; then
    broker=$(instancias broker)
    [[ -n "$broker" ]] || die "broker desligado (infra/aws/capacidade.sh <workers> 1 1)"
    broker_esperar
fi

# Distribui os pods entre os grupos: os com :n primeiro, na ordem dada; o grupo sem :n fica com o resto.
read -ra pods <<< "$workers"
total=${#pods[@]}
fixos=0
sem_n=0
for q in "${qtds[@]}"; do
    if [[ -n "$q" ]]; then fixos=$((fixos + q)); else sem_n=$((sem_n + 1)); fi
done
[[ $sem_n -le 1 ]] || die "só um grupo pode ficar sem :n"
[[ $fixos -le $total ]] || die "os grupos pedem $fixos pods, há $total ligados"
[[ $sem_n -eq 1 || $fixos -eq $total ]] || die "os grupos pedem $fixos pods, há $total ligados"
declare -A cfg_de
pos=0
for i in "${!cfgs[@]}"; do
    n=${qtds[$i]:-$((total - fixos))}
    for ((k = 0; k < n; k++)); do
        cfg_de[${pods[$pos]}]=${cfgs[$i]}
        pos=$((pos + 1))
    done
    log "grupo ${cfgs[$i]}: $n pods"
done
log "rodada $rotulo: $total workers, política $politica, $backend"

trace=$(propriedade "$gcfg" trace.file)
trace=${trace:-experiments/traces/$run_id.csv}
if [[ ! -f "$trace" ]]; then
    log "gerando $trace"
    ./gradlew -q :generator-app:run --args="generate $gcfg"
fi

mkdir -p "$destino"
infra/aws/relogio.sh "$destino/relogio.txt"

if [[ "$backend" == RABBITMQ ]]; then
    # O purge do RabbitMQ é imediato. Fila ainda não declarada (primeira rodada) conta como vazia.
    for f in $filas; do
        if [[ $purge -eq 1 ]]; then
            cmd=$(ssm_enviar "$broker" 60 "docker exec rabbitmq rabbitmqctl -q purge_queue $f || true")
            ssm_esperar "$cmd" "$broker" || die "purge de $f falhou"
        fi
        cmd=$(ssm_enviar "$broker" 60 "docker exec rabbitmq rabbitmqctl -q list_queues name messages")
        ssm_esperar "$cmd" "$broker" || die "não consegui listar as filas do broker"
        n=$(ssm_saida "$cmd" "$broker" | awk -v f="$f" '$1 == f { print $2 }')
        [[ -z "$n" || "$n" == 0 ]] || die "fila $f com $n mensagens; use --purge"
    done
else
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
fi

aws s3 sync experiments "s3://$bucket/experiments" --delete --quiet
cp "${cfgs[@]}" "$gcfg" "$destino/"
for id in "${pods[@]}"; do
    echo "$id ${cfg_de[$id]}"
done > "$destino/pods.txt"

log "subindo os workers"
declare -A cmd_de
for i in "${!cfgs[@]}"; do
    ids=""
    for id in "${pods[@]}"; do
        [[ "${cfg_de[$id]}" == "${cfgs[$i]}" ]] && ids="$ids $id"
    done
    [[ -n "$ids" ]] || continue
    cmd=$(ssm_enviar "$ids" 14400 "/opt/filas/app/remoto/worker.sh ${cfgs[$i]} $rotulo $ocioso_ms")
    for id in $ids; do
        cmd_de[$id]=$cmd
    done
done
for id in "${pods[@]}"; do
    while :; do
        st=$(ssm_estado "${cmd_de[$id]}" "$id")
        case "$st" in
            InProgress) break ;;
            Pending | Delayed) sleep 2 ;;
            *) ssm_saida "${cmd_de[$id]}" "$id" | tail -20; die "worker $id terminou com $st antes da rodada" ;;
        esac
    done
done
# Tempo para a JVM subir e o worker começar a consumir.
sleep 20

log "enviando o trace"
cmd_g=$(ssm_enviar "$gerador" 14400 "/opt/filas/app/remoto/gerador.sh $gcfg $rotulo")
gerador_ok=1
ssm_esperar "$cmd_g" "$gerador" || gerador_ok=0
ssm_saida "$cmd_g" "$gerador" | tail -8
[[ $gerador_ok -eq 1 ]] || log "ATENÇÃO: o gerador falhou; os workers vão parar por ociosidade"

log "esperando os workers esvaziarem as filas e ficarem ociosos ($ocioso_ms ms)"
prazo=$(( $(date +%s) + ocioso_ms / 1000 + 120 ))
for id in "${pods[@]}"; do
    while [[ "$(ssm_estado "${cmd_de[$id]}" "$id")" =~ ^(Pending|InProgress|Delayed)$ ]]; do
        if [[ $(date +%s) -gt $prazo ]]; then
            # Fila vazia há tempo suficiente e o worker não saiu (ex.: nunca recebeu um job).
            log "parando $id com SIGTERM"
            ssm_enviar "$id" 60 "pkill -TERM -f experimento.worker.WorkerMain || true" >/dev/null
            prazo=$(( $(date +%s) + 60 ))
        fi
        sleep 5
    done
    ssm_saida "${cmd_de[$id]}" "$id" | tail -2
done

aws s3 sync "s3://$bucket/registros/$rotulo" "$destino" --quiet
log "registros em $destino:"
ls -l "$destino"
log "análise: analysis/build/install/analysis/bin/analysis --warmup-s <s> $destino"
