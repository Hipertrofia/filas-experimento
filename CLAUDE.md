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
- O worker (atendimento dos jobs) é código próprio. A fila (SQS e, a seguir, RabbitMQ) só distribui jobs entre pods.
- A ordem de trabalho: primeiro a especificação do worker (`docs/especificacao-worker.md`), depois a configuração de SQS e EC2.
- Infra na AWS: CloudFormation (`infra/aws/filas.yaml`) e scripts bash com AWS CLI, sem SSH (SSM Run Command). Um pod = uma instância EC2 com um worker de uma thread. Docker local foi considerado e descartado (2026-10-29): os números finais vêm da AWS.
- Registros: cada pod grava um JSONL, enviado ao S3 e baixado para `registros/aws/<rótulo>/`; a análise lê o diretório como uma rodada.
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
analysis/        leitura do JSONL, métricas por classe e teoria (P-K, Cobham)
infra/           elasticmq/ (desenvolvimento) e aws/ (CloudFormation + scripts SSM)
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

- Estratégia de polling para a fila prioritária sem vazamento de prioridade. No ElasticMQ (2026-10-20), polling curto nas duas filas com pausa de 5 ms ficou perto da referência em memória; long polling na normal vaza prioridade e long polling na prioritária deixa a normal instável.
- Na AWS (2026-10-29, fumaça c = 1, ρ = 0,54, prioridade, `registros/aws/aws-fumaca-c1-s1-prioridade-20261029T204216Z`) o polling curto não serve: E[Wq] ficou 6–7× acima de Cobham. Fim de job → receive do próximo: ~11 ms se o próximo é prioritário, ≥ 60 ms se é normal (um receive vazio na prioritária custa ~50 ms); com o servidor ocioso, ~79 ms de mediana até o receive. O serviço efetivo sobe de ~105 para ~155 ms e o ρ real fica perto de 0,8. Relógio, coleta e serviço (`SLEEP`, +0,6 ms) funcionaram.
- Próximo passo (decidido em 2026-10-29): `adapter-rabbitmq` com `basic.consume`, prefetch = 1 e fila com `x-max-priority` (push, sem polling, prioridade nativa), testado contra RabbitMQ no Docker; depois o broker na stack da AWS e rodada c = 1 lado a lado com o SQS. Manter os dois: RabbitMQ como caso próximo do ideal, SQS como caso de nuvem em que o overhead de polling é resultado. `RESERVADO` vai precisar de duas filas.
- Cota de EC2 da conta: 5 vCPUs sob demanda (cabem 2 `c7i.large`); pedido de 32 vCPUs aberto como chamado de suporte em 2026-10-29.
- Modo de atendimento: `SLEEP` primeiro, `CPU_BURN` depois.

## Cuidados com a medição

- Medir o overhead do SQS rodando com serviço zero e tratá-lo como parte do serviço efetivo.
- Conferir o desvio de relógio entre gerador e pods antes de cada rodada (com 20 ms de serviço, poucos ms já distorcem).
- Desenvolver com LocalStack ou ElasticMQ; os números finais devem vir da AWS real.
- Rodar várias réplicas com seeds diferentes e descartar o aquecimento.
