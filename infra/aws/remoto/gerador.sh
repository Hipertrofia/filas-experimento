#!/bin/bash
# Envia o trace da rodada em tempo real e guarda a saída (atraso de envio) no S3.
# Uso: gerador.sh <gerador.properties> <rótulo da rodada>
set -euo pipefail
source /etc/filas.env

cfg=$1
rotulo=$2

cd /opt/filas
aws s3 sync "s3://$BUCKET/experiments" experiments --delete --quiet
mkdir -p "registros/$rotulo"

export SQS_REGION="$REGION"
export JAVA_OPTS="-Xms512m -Xmx512m -XX:+AlwaysPreTouch"

rc=0
app/generator-app/bin/generator-app send "$cfg" 2>&1 | tee "registros/$rotulo/gerador.log" || rc=$?
aws s3 cp "registros/$rotulo/gerador.log" "s3://$BUCKET/registros/$rotulo/gerador.log" --quiet
exit $rc
