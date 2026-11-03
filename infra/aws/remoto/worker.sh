#!/bin/bash
# Roda um pod worker até ele ficar ocioso, depois envia o registro ao S3.
# Uso: worker.sh <worker.properties> <rótulo da rodada> <ociosidade_ms>
set -euo pipefail
source /etc/filas.env

cfg=$1
rotulo=$2
ocioso_ms=$3

cd /opt/filas
aws s3 sync "s3://$BUCKET/experiments" experiments --delete --quiet

token=$(curl -s -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')
id=$(curl -s -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)

saida="registros/$rotulo"
mkdir -p "$saida"
# Sobrescrevem o arquivo de configuração (ver WorkerConfig).
export WORKER_ID="$id"
export SINK_FILE="$saida/$id.jsonl"
export WORKER_EXIT_AFTER_IDLE_MS="$ocioso_ms"
export SQS_REGION="$REGION"
# Heap fixo e pré-alocado: nada de crescimento de heap no meio da rodada.
export JAVA_OPTS="-Xms512m -Xmx512m -XX:+AlwaysPreTouch"
# Com RabbitMQ, URI e senha vêm do objeto que o broker grava no bucket ao subir.
if grep -qi '^[[:space:]]*queue.backend[[:space:]]*=[[:space:]]*rabbitmq' "$cfg"; then
    RABBITMQ_URI=$(aws s3 cp "s3://$BUCKET/broker/rabbitmq.env" - | sed -n 's/^RABBITMQ_URI=//p')
    [[ -n "$RABBITMQ_URI" ]] || { echo "sem broker/rabbitmq.env no bucket" >&2; exit 1; }
    export RABBITMQ_URI
fi

echo "worker $id: $cfg → $SINK_FILE"
rc=0
app/worker-app/bin/worker-app "$cfg" 2> "$saida/$id.log" || rc=$?
tail -5 "$saida/$id.log" || true
aws s3 cp "$saida/$id.log" "s3://$BUCKET/registros/$rotulo/$id.log" --quiet
if [[ -f "$saida/$id.jsonl" ]]; then
    aws s3 cp "$saida/$id.jsonl" "s3://$BUCKET/registros/$rotulo/$id.jsonl" --quiet
    echo "worker $id: $(wc -l < "$saida/$id.jsonl") registros, saída $rc"
else
    echo "worker $id: sem registro, saída $rc"
fi
exit $rc
