#!/usr/bin/env bash
# Liga ou desliga as instâncias. Uso: infra/aws/capacidade.sh <workers> <gerador 0|1> [broker 0|1]
#   infra/aws/capacidade.sh 2 1     dois pods e o gerador (sem broker: só SQS)
#   infra/aws/capacidade.sh 1 1 1   um pod, o gerador e o broker RabbitMQ
#   infra/aws/capacidade.sh 0 0     desliga tudo (filas e bucket continuam, sem custo de EC2)
# O broker omitido vale 0: ele é desligado se estava ligado.
# Desligar termina as instâncias: os registros já coletados ficam no S3 e em registros/aws/.
source "$(dirname "$0")/comum.sh"

[[ $# -eq 2 || $# -eq 3 ]] || die "uso: $0 <workers> <gerador 0|1> [broker 0|1]"
workers=$1
gerador=$2
broker=${3:-0}

aws cloudformation deploy \
    --stack-name "$STACK" \
    --template-file infra/aws/filas.yaml \
    --capabilities CAPABILITY_IAM \
    --no-fail-on-empty-changeset \
    --parameter-overrides "WorkerCount=$workers" "GeneratorCount=$gerador" "BrokerCount=$broker"

if [[ $broker -eq 1 ]]; then
    broker_esperar
fi

esperado=$((workers + gerador))
[[ $esperado -gt 0 ]] || { log "tudo desligado"; exit 0; }

log "esperando $esperado instâncias aparecerem no SSM"
for _ in $(seq 1 60); do
    ids="$(instancias worker) $(instancias gerador)"
    n=0
    for id in $ids; do
        st=$(aws ssm describe-instance-information --filters "Key=InstanceIds,Values=$id" \
            --query 'InstanceInformationList[0].PingStatus' --output text)
        [[ "$st" == Online ]] && n=$((n + 1))
    done
    if [[ $n -eq $esperado ]]; then
        log "prontas: $ids"
        log "se o pacote mudou desde a última implantação, rode infra/aws/implantar.sh"
        exit 0
    fi
    sleep 10
done
die "as instâncias não ficaram online no SSM em 10 min"
