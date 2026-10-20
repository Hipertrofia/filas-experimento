# Especificação do atendimento de jobs (worker)

Rascunho v1. Cobre apenas o worker. Configuração de SQS e EC2 fica para a fase seguinte, e o acesso à fila fica isolado atrás de uma interface para essa troca não afetar a lógica.

## 1. Escopo

O worker pega um job de uma fila, "atende" por um tempo definido pelo próprio job, registra os instantes e confirma a conclusão. Cada pod roda **um worker, atendendo um job por vez**, o que preserva o modelo de servidor único por pod (o sistema completo é uma fila compartilhada com c servidores, padrão *competing consumers*).

Fora do escopo por enquanto: gerador de carga, criação das filas, instâncias, autoscaling.

## 2. Modelo do job (mensagem)

| Campo | Descrição |
|---|---|
| `job_id` | UUID do job |
| `run_id` | Identifica o experimento (configuração + seed) |
| `class` | `PRIORITY` ou `NORMAL` |
| `size_ms` | Tempo de serviço nominal, sorteado pelo gerador de Exp(média da classe) |
| `enqueue_ts` | Instante de envio, em epoch (ms ou µs) |
| `parent_job_id`, `slice_index`, `slice_count` | Fatiamento. No começo, `slice_count = 1` e `parent_job_id = job_id` |

Tamanhos de partida, conforme o documento de referência: classe prioritária com média de **20 ms** e classe normal com média de **200 ms** (parametrizáveis). O sorteio fica no gerador, não no worker, para o worker não ter aleatoriedade própria e o experimento ser reprodutível a partir do trace.

## 3. Ciclo do worker

```
loop:
  msg = fila.receber()                # 1 mensagem, sem prefetch
  t_receive = agora()
  t_start   = agora()                 # logo após o receive retornar
  atender(msg.size_ms)                # sleep ou queima de CPU
  t_end     = agora()
  fila.confirmar(msg)                 # delete
  registrar(msg, t_receive, t_start, t_end)
```

Regras:

- Medir durações com **relógio monotônico** e usar relógio de parede sincronizado (NTP) para os instantes comparáveis entre pods.
- O registro vai para um **buffer em memória**, descarregado por outra thread. Escrita síncrona entre dois jobs aumentaria o tempo de ocupação do servidor e contaminaria a medição.
- **Confirmar só depois de atender.** Se o pod cair no meio, o job volta para a fila e é atendido de novo. A análise deduplica por `job_id`, guardando a primeira conclusão.
- Em `SIGTERM`, terminar o job em curso e então parar.
- Sem prefetch: com mais de uma mensagem por receive, as extras esperam numa fila local do pod, criando bloqueio de cabeça de fila escondido.

## 4. Política de consumo

Configuração do worker: lista ordenada de filas que ele consulta.

| Política | Filas consultadas pelo worker |
|---|---|
| `FIFO_COMPARTILHADO` | uma fila única |
| `PRIORIDADE` | prioritária primeiro, depois normal |
| `RESERVADO(k)` | k pods só consomem a prioritária (ou prioritária e depois normal); os demais só a normal |
| `FAST_LANE_FISICA` | pods fixos por fila, sem sobreposição |

**Ponto de risco (em aberto):** se o worker faz long polling na fila normal e chega um job prioritário nesse intervalo, ele só será visto quando o polling terminar, e a prioridade vaza. Alternativas: polling curto alternado com intervalo mínimo (mais requisições e mais custo) ou polling longo na prioritária com tempo curto. Medir o impacto já na fase de overhead.

## 5. Modo de atendimento

- `SLEEP`: o pod dorme pelo tempo nominal. Isola o modelo da contenção de CPU, bom para a primeira fase.
- `CPU_BURN`: laço ocupando CPU pelo tempo nominal. Aproxima da realidade e permite estudar limites de CPU depois.

Registrar sempre o `service_actual = t_end − t_start` além do nominal. A diferença mostra a imprecisão do sleep e o overhead do ambiente.

## 6. Registro por job

| Campo | Observação |
|---|---|
| `job_id`, `run_id`, `class`, `size_ms` | Copiados da mensagem |
| `enqueue_ts`, `receive_ts`, `start_ts`, `end_ts` | Instantes de parede |
| `worker_id` | Identifica pod e thread |
| `policy`, `queue` | Disciplina e fila de origem |
| `receive_count` | Número de entregas (detecta duplicatas) |

Derivados na análise:

- **Wq** = `start_ts − enqueue_ts`
- **W** = `end_ts − enqueue_ts`
- **serviço** = `end_ts − start_ts`

Daí saem, por classe, E[Wq], E[Wq²], E[W], E[W²] e, se as amostras forem guardadas, o p99. Formato de saída: JSON Lines, um registro por linha. Onde armazenar fica para a fase de infraestrutura.

## 7. Fatiamento (fase 2, já previsto no esquema)

O job caro com `slice_count = m` vira m mensagens de tamanho `size_ms/m`. Ao terminar a fatia i, o worker reenfileira a fatia i+1 no **fim** da fila da classe. Cada fatia gera seu registro, e o tempo do job pai é calculado na análise: `end_ts` da última fatia menos o `enqueue_ts` original do pai.

## 8. Interface com a fila

O worker só conhece uma interface mínima:

- `receber()` → mensagem ou nada
- `confirmar(msg)`
- `enviar(msg, fila)` (usado no fatiamento)

Isso permite uma implementação em memória para testar a lógica localmente e uma implementação SQS depois.

## 9. Testes da especificação

1. Precisão do atendimento: `service_actual` próximo do nominal (medir erro médio e máximo para 20 ms e 200 ms).
2. Política: com a fila fake, conferir que `PRIORIDADE` atende o prioritário antes do normal quando ambos esperam.
3. Registro: nenhum instante faltando, e `start ≥ enqueue`.
4. Duplicata: reentrega do mesmo job registra uma só conclusão válida na análise.

## 10. Decisões em aberto

| Decisão | Sugestão inicial |
|---|---|
| Ferramenta de build | **Decidido:** Gradle (Kotlin DSL) |
| Pacote base | **Decidido:** `br.com.filas.experimento` |
| Estratégia de polling na prioridade | Polling curto alternado, medindo o impacto |
| Modo de atendimento | `SLEEP` primeiro, `CPU_BURN` depois |
| Onde guardar os registros | Arquivo local nas instâncias, coletado ao fim da rodada |
| Resolução do tempo | Milissegundos |
