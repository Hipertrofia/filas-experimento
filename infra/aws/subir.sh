#!/usr/bin/env bash
# Cria ou atualiza a stack (filas, bucket, papéis, grupos de instâncias).
# Uso: infra/aws/subir.sh [Parametro=valor ...]   ex.: InstanceType=c7i.large
# Na criação, WorkerCount e GeneratorCount ficam em 0: nada de EC2 é cobrado até capacidade.sh.
source "$(dirname "$0")/comum.sh"

log "stack $STACK em $AWS_REGION"
aws cloudformation deploy \
    --stack-name "$STACK" \
    --template-file infra/aws/filas.yaml \
    --capabilities CAPABILITY_IAM \
    --no-fail-on-empty-changeset \
    ${1:+--parameter-overrides} "$@"
log "bucket: $(saida_stack Bucket)"
