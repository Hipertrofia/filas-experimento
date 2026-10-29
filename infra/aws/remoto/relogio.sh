#!/bin/bash
# Estado do relógio desta instância, numa linha:
#   <instance-id> <referência> <offset_us> <limite_erro_us>
# O limite de erro segue o do chrony: |offset| + root_delay/2 + root_dispersion. O desvio entre
# duas instâncias que usam a mesma referência fica abaixo da soma dos limites delas.
set -euo pipefail

token=$(curl -s -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')
id=$(curl -s -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id)

# chronyc -n -c tracking: 2 = referência, 5 = offset do sistema (s), 11 = root delay, 12 = root dispersion
chronyc -n -c tracking | awk -F, -v id="$id" '{
    off = $5 < 0 ? -$5 : $5
    printf "%s %s %.1f %.1f\n", id, $2, $5 * 1e6, (off + $11 / 2 + $12) * 1e6
}'
