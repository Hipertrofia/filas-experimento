# filas-experimento

Experimento de filas com workers em Java (SQS ou RabbitMQ + EC2). Contexto e decisões em `CLAUDE.md`;
especificação do worker em `docs/especificacao-worker.md`.

- Java 17, Gradle (Kotlin DSL) com wrapper, pacote base `br.com.filas.experimento`.
- `core`: domínio, portas e `WorkerLoop`, só JDK (garantido por teste ArchUnit).
- `adapter-local`: fila em memória, executores `SLEEP`/`CPU_BURN`, relógio do sistema, sink JSONL.
- `adapter-sqs`: fila SQS (AWS SDK v2); testado contra ElasticMQ embutido.
- `adapter-rabbitmq`: fila RabbitMQ por push (`basic.consume`, prefetch = 1, `x-max-priority`);
  testes de integração contra o broker de `infra/rabbitmq/` (pulados se ele não estiver no ar).
- `worker-app`: configuração, montagem e ciclo de vida (SIGTERM).
- `generator-app`: trace de Poisson por seed (`generate`) e envio em tempo real (`send`, SQS ou RabbitMQ).
- `analysis`: leitura do JSONL, métricas por classe e comparação com a teoria,
  com a réplica ideal da própria rodada (mesmas chegadas e serviços, sem overhead) e o overhead de
  entrega por worker (fila ocupada: fim → início do próximo; servidor ocioso: chegada → início).
- `experiments/`: configs de rodada e traces gerados.
- `infra/elasticmq/`: SQS local para desenvolvimento (filas `prioritaria`, `normal`, `unica` e `dlq`).
- `infra/rabbitmq/`: RabbitMQ local para desenvolvimento (as filas são declaradas pelos apps).
- `infra/aws/`: filas SQS, pods EC2, checagem de relógio e coleta dos registros (ver `infra/aws/README.md`).

## Comandos

Os apps rodam a partir da raiz do repositório (caminhos das configs são relativos a ela).

```
./gradlew build
./gradlew :generator-app:run --args="generate experiments/fumaca.properties"
./gradlew :worker-app:run --args="experiments/local-prioridade.properties"
```

O primeiro comando sorteia `experiments/traces/fumaca-s1.csv` e imprime λ, E[S], E[S²] e ρ
medidos no trace. O segundo faz o replay desse trace em tempo real na fila em memória (no mesmo
processo dos workers), atende e grava `registros/local-prioridade.jsonl`.

## Com SQS (ElasticMQ local)

Gerador e worker em processos separados, como na AWS:

```
docker compose -f infra/elasticmq/docker-compose.yml up -d
./gradlew :worker-app:installDist :generator-app:installDist
worker-app/build/install/worker-app/bin/worker-app experiments/sqs-dev-worker.properties
generator-app/build/install/generator-app/bin/generator-app send experiments/sqs-dev-fumaca.properties
```

Sem Docker, o jar standalone do ElasticMQ usa a mesma configuração:
`java -Dconfig.file=infra/elasticmq/elasticmq.conf -jar elasticmq-server-1.7.1.jar`.

Na AWS real, omita `sqs.endpoint`: as credenciais vêm da cadeia padrão (perfil, variáveis ou
papel da instância). No SQS, a espera do receive (`queue.wait-ms*`) é em segundos inteiros, de 0 a
20, e o `send` do gerador avisa quando o atraso de envio passa de 5 ms.

## Com RabbitMQ (Docker local)

```
docker compose -f infra/rabbitmq/docker-compose.yml up -d
./gradlew :worker-app:installDist :generator-app:installDist
generator-app/build/install/generator-app/bin/generator-app generate experiments/rabbitmq-dev-fumaca.properties
worker-app/build/install/worker-app/bin/worker-app experiments/rabbitmq-dev-worker.properties
generator-app/build/install/generator-app/bin/generator-app send experiments/rabbitmq-dev-fumaca.properties
```

O broker empurra a próxima mensagem assim que o worker confirma a anterior (prefetch = 1), então
não há polling. A disciplina fica na declaração da fila: as listadas em `rabbitmq.priority-queues`
recebem `x-max-priority = 1` e entregam os jobs `PRIORITY` primeiro (prioridade não-preemptiva);
as demais são FIFO. Para `PRIORIDADE`, as duas classes vão para a mesma fila (`queue.priority` =
`queue.normal`) e o worker consome só ela. Cada worker consome exatamente uma fila; `RESERVADO` e
`FAST_LANE_FISICA` usam filas diferentes em pods diferentes. Gerador e workers declaram as filas
com os mesmos argumentos; se uma fila já existir com outros, a partida falha.

`rabbitmq.uri` sem barra final usa o vhost `/` (`amqp://usuario:senha@host:5672`); a senha pode vir
de `RABBITMQ_URI`. O `receive_count` vale 1 ou 2 (o RabbitMQ só informa se é reentrega).

## Trace

CSV com cabeçalho `# chave=valor` (`run_id`, `seed` e os parâmetros) e as colunas
`offset_us,job_id,class,size_us,slice_count`. `size_us` é o serviço total; cada fatia recebe
`size_us / slice_count`. Cada classe usa fluxos aleatórios próprios para chegadas e tamanhos,
então, com a mesma seed, mudar ρ só reescala os intervalos e mantém os tamanhos.

## Configuração do worker

Precedência: padrões < arquivo `.properties` (1º argumento) < variável de ambiente
(`worker.policy` → `WORKER_POLICY`) < `-D`. Chaves em `WorkerConfig.Keys`; exemplo comentado em
`worker-app/config/local-prioridade.properties`.

## Formato do registro (JSONL)

Um job atendido por linha. Instantes em epoch e durações em microssegundos inteiros (`_us`):
`job_id`, `run_id`, `class`, `size_us`, `enqueue_ts_us`, `receive_ts_us`, `start_ts_us`,
`end_ts_us`, `service_actual_us`, `worker_id`, `policy`, `queue`, `receive_count`,
`parent_job_id`, `slice_index`, `slice_count`.

## Análise

```
./gradlew :analysis:installDist
analysis/build/install/analysis/bin/analysis --warmup-s 5 registros/polling/L-memoria.jsonl
analysis/build/install/analysis/bin/analysis --warmup-s 30 --replicas --csv registros/resumo.csv r-s1.jsonl r-s2.jsonl r-s3.jsonl
```

Por arquivo: deduplica por `job_id` (fica a conclusão que terminou primeiro), reúne as fatias no
job pai, descarta os jobs que chegaram durante o aquecimento (`--warmup-s`) e imprime, por classe,
n, λ, E[S], E[S²], E[Wq], E[Wq²], p99 de Wq, E[W], E[W²] e p99 de W, em ms. Wq soma as esperas
(`start − enqueue`) de todas as fatias; W vai da chegada do pai ao fim da última fatia.

A teoria usa os λ e os momentos de serviço medidos na própria rodada (`--service nominal` usa o
`size_us` do trace) e c = número de workers no registro (`--servers` para mudar):

| Política | c = 1 (exato) | c > 1 (aproximação) |
|---|---|---|
| `FIFO_COMPARTILHADO` | Pollaczek–Khinchine; E[Wq²] de Takács | Allen–Cunneen |
| `PRIORIDADE` | Cobham; E[Wq²] por classe | Bondi–Buzen (exata se as classes têm a mesma média) |

`--model fifo|priority|none` força o modelo, por exemplo para ver quanto a prioridade ganha sobre
o FIFO na mesma rodada. As outras políticas e os jobs fatiados saem sem teoria. `--replicas`
agrega os arquivos como réplicas (média e IC de 95% pela t de Student entre rodadas), e `--csv`
grava uma linha por arquivo e classe. Os testes validam as fórmulas contra um simulador de eventos
discretos (`QueueSimulator`).
