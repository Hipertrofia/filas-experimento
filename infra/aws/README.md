# Infra na AWS

Uma stack do CloudFormation (`filas.yaml`) com:

- **Filas SQS** `prioritaria`, `normal`, `unica` e `dlq`, com os mesmos nomes e redrive do ElasticMQ.
- **Pods worker**: um grupo de Auto Scaling de tamanho fixo (`WorkerCount`). Cada instância roda
  um worker com uma thread, ou seja, um servidor.
- **Gerador**: outro grupo, com 0 ou 1 instância.
- **Broker RabbitMQ**: outro grupo, com 0 ou 1 instância (`BrokerCount`). RabbitMQ em Docker
  (imagem oficial pelo ECR Public) com a rede do host. A porta AMQP (5672) aceita só as instâncias
  da própria stack. A senha é sorteada no boot; quando o broker termina de subir, grava URI e
  senha em `s3://<bucket>/broker/rabbitmq.env`, de onde worker e gerador leem.
- **Bucket S3** para o pacote da aplicação, a cópia de `experiments/` e os registros.
- **Papel IAM** com acesso só a essas filas e a esse bucket, mais o SSM.

As instâncias usam Amazon Linux 2023, ficam todas na mesma zona e não têm chave SSH; a única porta
de entrada é a AMQP do broker, fechada para fora da stack.
Os scripts as comandam pelo **SSM Run Command**. O relógio vem do Amazon Time Sync Service
(chrony, já configurado na AMI).

## Antes da primeira vez

1. **Conta AWS** em <https://aws.amazon.com/>. Ela pede cartão de crédito. Não use o usuário raiz
   no dia a dia: crie um usuário com permissão de administrador no IAM Identity Center (ou no IAM).
   Crie também um **alerta de orçamento** em *Billing → Budgets*, por exemplo de US$ 20/mês, para
   receber um e-mail se algo ficar ligado por engano.
   - Contas do *AWS Academy Learner Lab* não permitem criar papéis IAM. Nesse caso, o template
     precisa usar o `LabInstanceProfile` existente no lugar de `Role`/`InstanceProfile`.
2. **AWS CLI v2**: `winget install Amazon.AWSCLI`, depois `aws configure sso` (Identity Center) ou
   `aws configure` (chave de acesso). Confira com `aws sts get-caller-identity`.
3. Os scripts rodam no **Git Bash**, a partir de qualquer pasta. A região padrão é `us-east-1`
   (`AWS_REGION` muda); o nome da stack é `filas-experimento` (`FILAS_STACK` muda).

## Fluxo

```
infra/aws/subir.sh                       # cria filas, bucket e papéis; nenhuma instância ainda
infra/aws/capacidade.sh 2 1              # liga 2 pods worker e o gerador (passa a cobrar EC2)
infra/aws/implantar.sh                   # compila e instala worker-app e generator-app
infra/aws/relogio.sh                     # confere os relógios (a rodada também confere)

infra/aws/rodada.sh experiments/aws-worker-prioridade.properties experiments/aws-fumaca.properties
analysis/build/install/analysis/bin/analysis --warmup-s 10 registros/aws/<rótulo>

# RabbitMQ: o terceiro argumento liga o broker; mesmo trace da fumaça SQS c = 1
infra/aws/capacidade.sh 1 1 1
infra/aws/rodada.sh experiments/aws-worker-rabbitmq-prioridade.properties experiments/aws-fumaca-c1-rabbitmq.properties

infra/aws/capacidade.sh 0 0              # desliga as instâncias ao terminar o dia (broker incluído)
infra/aws/destruir.sh                    # apaga tudo, inclusive o bucket
```

`load.servers` no gerador tem que ser igual ao número de pods ligados, senão o ρ do trace
não é o ρ da rodada. A análise usa como c o número de workers que aparecem nos registros.

### O que a rodada faz (`rodada.sh`)

1. Gera o trace localmente se `trace.file` ainda não existir. Como é determinístico pela seed, o
   mesmo trace serve para comparar disciplinas.
2. **Confere os relógios** de todas as instâncias (`relogio.sh`) e aborta se o desvio possível
   passar de 1 ms (`FILAS_RELOGIO_LIMITE_US`). O resultado fica em `relogio.txt`, junto dos registros.
3. **Confere se as filas estão vazias.** `--purge` esvazia as filas e espera os 60 s que o SQS
   exige.
4. Copia `experiments/` para o S3 e sobe os workers. Cada um grava
   `registros/<rótulo>/<instance-id>.jsonl` e sai depois de 15 s sem atender nada
   (`FILAS_OCIOSO_MS`).
5. O gerador envia o trace em tempo real. A saída dele (atraso médio e máximo de envio) aparece no
   terminal e fica em `gerador.log`.
6. Espera os workers terminarem. Se um deles não sair (por exemplo, porque nunca recebeu um job),
   recebe SIGTERM.
7. Baixa tudo para **`registros/aws/<rótulo>/`**: um `.jsonl` e um `.log` por pod, `gerador.log`,
   `relogio.txt` e cópias das duas configs. A análise aceita o diretório como uma rodada só.

O rótulo padrão é `<run.id>-<política>-<instante UTC>`; `--rotulo` escolhe outro.

### Relógio

O `remoto/relogio.sh` calcula o limite de erro do chrony em cada instância:
|offset| + root delay/2 + root dispersion. O desvio entre duas instâncias fica abaixo da soma dos
limites delas. Com o Amazon Time Sync, os limites costumam ser de dezenas a centenas de µs. Logo depois do
boot, o chrony ainda está convergindo; se a checagem falhar, espere alguns minutos.

## Custos (aproximados; confira na página de preços da região)

| Item | Ordem de grandeza |
|---|---|
| EC2 `c7i.large` | ~US$ 0,09/h por instância. Com 2 pods + gerador, ~US$ 0,27/h, só enquanto ligadas |
| SQS | US$ 0,40 por milhão de requisições, com o primeiro milhão do mês grátis. **O polling curto domina**: cada pod ocioso faz dezenas de receives por segundo, então uma hora de rodada pode passar de 1 milhão de requisições |
| S3, EBS | centavos |

As filas e o bucket parados não custam nada relevante. O que custa é deixar instâncias ligadas:
use `capacidade.sh 0 0` ao terminar. O tipo `t3`/`t4g` é mais barato, mas o crédito de CPU
pode acabar no meio da rodada e mudar o tempo de serviço; por isso o padrão é `c7i`.
