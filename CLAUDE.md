# Experimento de filas com workers em Java (SQS + EC2)

## Objetivo

Validar empiricamente, num sistema de filas real, o comportamento de jobs normais e prioritários com tempos de serviço exponenciais e chegadas de Poisson. Métricas por classe: tempo médio e segundo momento de Wq (espera na fila) e W (tempo no sistema), além do p99 quando possível.

O trabalho se apoia no documento "Filas, distribuições phase-type e orquestração de containers" (teoria de filas aplicada a dimensionamento de containers). Pontos que importam aqui:

- Jobs da classe prioritária com média de 20 ms e da classe normal com média de 200 ms (parametrizáveis).
- Sistema alvo: fila compartilhada com c servidores (competing consumers), um job por vez em cada pod.
- Disciplinas a comparar: FIFO compartilhado, prioridade, slots reservados, fast lane física e, depois, fatiamento da tarefa cara.
- Fórmulas de validação: Pollaczek–Khinchine (FIFO) e Cobham (prioridade não-preemptiva).

## Decisões tomadas

- Linguagem: Java 17, arquitetura hexagonal, projeto multi-módulo.
- Build: Gradle (Kotlin DSL) com wrapper; versões em `gradle/libs.versions.toml`.
- Pacote base: `br.com.filas.experimento`.
- O worker (atendimento dos jobs) é código próprio. O SQS entra só como fila que distribui jobs entre pods.
- A ordem de trabalho: primeiro a especificação do worker (`docs/especificacao-worker.md`), depois a configuração de SQS e EC2.
- O sorteio do tamanho do job fica no gerador, nunca no worker, para o experimento ser reprodutível por trace com seed.
- Preempção real não é possível com SQS. Só FIFO e prioridade não-preemptiva são reproduzíveis, e o fatiamento cobre o restante.

## Estrutura de módulos

```
core/            domínio + aplicação, Java puro, sem dependências
adapter-local/   fila em memória, executores (sleep, cpu burn), relógio, sink JSONL
adapter-sqs/     único módulo que conhece o AWS SDK
worker-app/      ponto de entrada, configuração, montagem dos adaptadores
generator-app/   fase seguinte: trace Poisson e envio
experiments/     configs de rodada (política, ρ, seed) e traces
analysis/        leitura do JSONL e cálculo das métricas
infra/           reservado: SQS e EC2
```

Regras de dependência: `core` não depende de nada fora do JDK; `adapter-*` dependem só do `core`; só o `worker-app` conhece os adaptadores. Vale ter um teste ArchUnit no `core` que falhe se alguma classe importar algo fora do JDK.

## Regras do worker

- Um job por vez, uma mensagem por receive, sem prefetch.
- Durações com relógio monotônico; instantes com relógio de parede sincronizado por NTP.
- Registro em buffer em memória, descarregado por outra thread, para não ocupar o servidor entre jobs.
- Confirmar (delete) só depois de atender; a análise deduplica por `job_id`.
- Visibility timeout da fila maior que o maior tempo de serviço possível.

Detalhes completos em `docs/especificacao-worker.md`.

## Pendências

- Estratégia de polling para a fila prioritária sem vazamento de prioridade. No ElasticMQ (2026-10-20), polling curto nas duas filas com pausa de 5 ms ficou perto da referência em memória; long polling na normal vaza prioridade e long polling na prioritária deixa a normal instável. Falta confirmar na AWS, onde o polling curto pode responder vazio com mensagens na fila, e medir o custo em requisições.
- Modo de atendimento: `SLEEP` primeiro, `CPU_BURN` depois.
- Onde guardar os registros de cada rodada.

## Cuidados com a medição

- Medir o overhead do SQS rodando com serviço zero e tratá-lo como parte do serviço efetivo.
- Conferir o desvio de relógio entre gerador e pods antes de cada rodada (com 20 ms de serviço, poucos ms já distorcem).
- Desenvolver com LocalStack ou ElasticMQ; os números finais devem vir da AWS real.
- Rodar várias réplicas com seeds diferentes e descartar o aquecimento.
