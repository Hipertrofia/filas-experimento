#!/usr/bin/env bash
# Compila worker-app e generator-app, envia o pacote ao S3 e instala nas instâncias ligadas.
# Uso: infra/aws/implantar.sh
source "$(dirname "$0")/comum.sh"

bucket=$(saida_stack Bucket)
[[ -n "$bucket" ]] || die "stack $STACK sem bucket: rode infra/aws/subir.sh"

log "compilando"
./gradlew -q :worker-app:installDist :generator-app:installDist

pacote=build/pacote
rm -rf "$pacote" build/filas.tar.gz
mkdir -p "$pacote"
cp -r worker-app/build/install/worker-app generator-app/build/install/generator-app "$pacote/"
cp -r infra/aws/remoto "$pacote/"
tar czf build/filas.tar.gz -C "$pacote" .
aws s3 cp build/filas.tar.gz "s3://$bucket/app/filas.tar.gz" >/dev/null
log "pacote em s3://$bucket/app/filas.tar.gz"

ids="$(instancias worker) $(instancias gerador)"
ids=$(echo $ids)
if [[ -z "$ids" ]]; then
    log "nenhuma instância ligada; as próximas baixam o pacote ao iniciar"
    exit 0
fi

log "instalando em $ids"
cmd=$(ssm_enviar "$ids" 900 \
    "cloud-init status --wait >/dev/null || true" \
    "aws s3 cp s3://$bucket/app/filas.tar.gz /tmp/filas.tar.gz --quiet" \
    "rm -rf /opt/filas/app && mkdir -p /opt/filas/app /opt/filas/registros" \
    "tar xzf /tmp/filas.tar.gz -C /opt/filas/app" \
    "find /opt/filas/app -name '*.sh' -exec sed -i 's/[[:cntrl:]]$//' {} +" \
    "chmod +x /opt/filas/app/*/bin/* /opt/filas/app/remoto/*.sh" \
    "java -version 2>&1 | head -1")
ssm_esperar "$cmd" "$ids" || die "falha na instalação (veja o console do SSM, comando $cmd)"
log "instalado"
