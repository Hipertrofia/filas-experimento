# filas-experimento

Experimento de filas com workers em Java (SQS + EC2). Contexto e decisões em `CLAUDE.md`;
especificação do worker em `docs/especificacao-worker.md`.

- Java 17, Gradle (Kotlin DSL) com wrapper, pacote base `br.com.filas.experimento`.
- `core`: domínio, portas e `WorkerLoop`, só JDK (garantido por teste ArchUnit).
- `adapter-local`: fila em memória, executores `SLEEP`/`CPU_BURN`, relógio do sistema, sink JSONL.
- `adapter-sqs`: fila SQS (AWS SDK v2); testado contra ElasticMQ embutido.
- `worker-app`: configuração, montagem e ciclo de vida (SIGTERM).
- `generator-app`: trace de Poisson por seed (`generate`) e envio em tempo real (`send`, fase SQS).
- `experiments/`: configs de rodada e traces gerados.
- `infra/elasticmq/`: SQS local para desenvolvimento (filas `prioritaria`, `normal`, `unica` e `dlq`).

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
