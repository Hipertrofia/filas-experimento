#!/usr/bin/env bash
# Apaga tudo: instâncias, filas, papéis e o bucket (com os registros guardados nele).
# Uso: infra/aws/destruir.sh
source "$(dirname "$0")/comum.sh"

bucket=$(saida_stack Bucket || true)
echo "Isto apaga a stack $STACK em $AWS_REGION e o bucket ${bucket:-?} com tudo dentro."
echo "Registros já baixados em registros/aws/ não são afetados."
read -r -p "Digite o nome da stack para confirmar: " resposta
[[ "$resposta" == "$STACK" ]] || die "cancelado"

if [[ -n "$bucket" ]]; then
    aws s3 rm "s3://$bucket" --recursive --quiet
fi
aws cloudformation delete-stack --stack-name "$STACK"
log "apagando (alguns minutos)"
aws cloudformation wait stack-delete-complete --stack-name "$STACK"
log "stack apagada"
