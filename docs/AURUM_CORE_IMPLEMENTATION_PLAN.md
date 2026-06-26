# AurumMQ — Plan de implementación para `crates/hot-path/aurum-core`

Este documento define cómo implementar `aurum-core`, el corazón del data plane de AurumMQ. El objetivo de este crate no es conocer protocolos, storage físico, clustering ni AMQP. Su responsabilidad es ejecutar la semántica interna de una cola Rabbit-like usando un diseño **range/mask-first**, cache-friendly y preparado para thread-per-core.

## 0. Decisión arquitectónica

`aurum-core` debe ser el crate que responda esta pregunta:

> Dado un flujo de `PublishBatch`, crédito de consumidores, `AckBatch`, `NackBatch`, retry y redelivery, ¿cuál es el siguiente trabajo de entrega y cómo queda el estado de la cola?

Debe operar con:

- `DeliveryRange`: entrega compacta de secuencias contiguas.
- `DeliveryMask`: entrega compacta de bits dentro de un bloque.
- `AckRange`: confirmación compacta de secuencias contiguas.
- `AckMask`: confirmación compacta de bits dentro de un bloque.
- `NackMask` / `RetryMask`: transición compacta a retry/redelivery.

No debe operar primariamente con `Message` ni con nodos por mensaje. El mensaje individual existe como compatibilidad de borde, no como unidad central del engine.

## 1. Fronteras del crate

### 1.1 Lo que `aurum-core` sí debe hacer

- Mantener estado por cola/shard:
  - ready secuencial.
  - sparse ready.
  - inflight.
  - acked.
  - retry.
  - dead/deleted en fases posteriores.
- Publicar mensajes como rangos internos.
- Entregar trabajo usando rangos y máscaras.
- Aplicar ack/nack/retry usando rangos y máscaras.
- Mantener estructuras activas por bloque, no por mensaje.
- Proveer snapshots lógicos del estado para tests/model checking.
- Exponer eventos mínimos para storage posterior:
  - `CoreEvent::PublishedRange`.
  - `CoreEvent::DeliveredRange`.
  - `CoreEvent::AckedRange`.
  - `CoreEvent::AckedMask`.
  - `CoreEvent::NackedMask`.
  - `CoreEvent::RetryScheduled`.
- Mantener invariantes fuertes y verificables.

### 1.2 Lo que `aurum-core` NO debe hacer

- No parsea AMQP.
- No conoce `AMQPFrame`, `basic.ack`, `basic.nack`, etc.
- No parsea el protocolo nativo.
- No escribe a disco.
- No replica.
- No hace I/O de red.
- No sabe de Kubernetes ni operator.
- No tiene timers reales del SO.
- No usa runtime async.
- No usa `dyn Trait` en hot paths.
- No depende de `aurum-protocol-amqp`, `aurum-protocol-native`, `aurum-storage`, `aurum-runtime` ni `aurum-cluster`.

La dependencia permitida es hacia abajo:

```text
aurum-core
  -> aurum-types
  -> aurum-kernels
  -> aurum-intrusive
```

Opcionalmente, más adelante:

```text
aurum-core
  -> smallvec / arrayvec
```

si mejora `DeliveryWork` sin meter heap allocations.

## 2. Estado actual

Actualmente existe una primera versión de `HybridRangeBlockQueue` en:

```text
crates/hot-path/aurum-core/src/queue.rs
```

Tiene:

- `MsgBlock` con `inflight`, `acked`, `retry`, `sparse_ready`.
- `sequential_head` / `sequential_tail`.
- `BlockList` interno para retry/sparse.
- `deliver` que produce `DeliveryWork`.
- `ack_work` y `nack_work_to_retry`.
- `retry_all_now`.
- tests mínimos.

El problema del estado actual es que todo está concentrado en `queue.rs`. La próxima fase debe convertirlo en una implementación mantenible, testeable y preparada para optimización.

## 3. Estructura de módulos objetivo

Reorganizar `aurum-core` así:

```text
crates/hot-path/aurum-core/src/
  lib.rs
  queue/
    mod.rs
    constants.rs
    ids.rs
    block.rs
    lists.rs
    work.rs
    error.rs
    state.rs
    hybrid.rs
    publish.rs
    delivery.rs
    ack.rs
    nack.rs
    retry.rs
    model.rs
    invariants.rs
    stats.rs
    tests.rs
```

### 3.1 `constants.rs`

Define parámetros básicos:

```rust
pub const MSGS_PER_BLOCK: usize = 256;
pub const WORDS_PER_BLOCK: usize = MSGS_PER_BLOCK / 64;
pub const DEFAULT_DELIVERY_BATCH_RANGES: usize = 8;
pub const DEFAULT_DELIVERY_BATCH_MASKS: usize = 16;
```

Más adelante se puede evaluar `MSGS_PER_BLOCK = 128/256/512` con const generics, pero para la primera versión se fija en 256.

### 3.2 `ids.rs`

Tipos canónicos del core:

```rust
pub struct QueueSeq(pub u64);
pub struct BlockId(pub u32);
pub struct BlockGeneration(pub u16);
pub struct WordOffset(pub u8);
pub struct BitOffset(pub u8);
```

Inicialmente se puede reutilizar `aurum_types::Seq`, `BlockIndex`, `WordIndex`, pero conviene encapsular la semántica en `aurum-core` si aparecen invariantes específicas.

### 3.3 `block.rs`

Define `MsgBlock`:

```rust
#[repr(C)]
pub struct MsgBlock {
    pub base_seq: Seq,

    pub inflight: [u64; WORDS_PER_BLOCK],
    pub acked: [u64; WORDS_PER_BLOCK],
    pub retry: [u64; WORDS_PER_BLOCK],
    pub sparse_ready: [u64; WORDS_PER_BLOCK],

    pub retry_word_mask: u8,
    pub sparse_word_mask: u8,
    pub dirty_word_mask: u8,

    pub retry_link: Link,
    pub sparse_link: Link,
    pub dirty_link: Link,
}
```

Métodos mínimos:

```rust
impl MsgBlock {
    pub const fn new(base_seq: Seq) -> Self;
    pub fn is_retry_empty(&self) -> bool;
    pub fn is_sparse_ready_empty(&self) -> bool;
    pub fn mark_sparse_word(&mut self, word: usize);
    pub fn unmark_sparse_word_if_empty(&mut self, word: usize);
    pub fn mark_retry_word(&mut self, word: usize);
    pub fn unmark_retry_word_if_empty(&mut self, word: usize);
}
```

### 3.4 `lists.rs`

Primera fase: mover el `BlockList` actual ahí.

Segunda fase: reemplazarlo por `aurum_intrusive::IndexList` si conseguimos modelar múltiples links por bloque de forma limpia.

Necesidad específica: un mismo `MsgBlock` puede estar simultáneamente en varias listas:

- sparse ready list.
- retry list.
- dirty list.
- timer list futura.

Por eso `aurum_intrusive::Linked` actual no basta directamente, porque asume un solo link por nodo. Opciones:

1. Mantener `BlockList` interno especializado por `ListKind`.
2. Extender `aurum-intrusive` para soportar multi-link por kind.
3. Implementar `IntrusiveBlockList<K>` con accessor estático por tipo.

Para la primera implementación, usar opción 1. Para consolidación, opción 2.

### 3.5 `work.rs`

Tipos de trabajo interno:

```rust
pub struct DeliveryBatch {
    pub ranges: Vec<DeliveryRange>,
    pub masks: Vec<DeliveryMask>,
}

pub struct AckBatch {
    pub ranges: Vec<AckRange>,
    pub masks: Vec<AckMask>,
}

pub struct NackBatch {
    pub ranges: Vec<NackRange>,
    pub masks: Vec<NackMask>,
    pub reason: NackReason,
}
```

A corto plazo se puede seguir usando `aurum_types::DeliveryWork`. La meta es separar:

- `DeliveryWork`: output del scheduler.
- `AckWork`: input del ack engine.
- `NackWork`: input del nack engine.

Para evitar heap allocations por batch en hot path, después cambiar:

```rust
Vec<T>
```

por:

```rust
SmallVec<[T; N]>
```

o un batch con capacidad fija:

```rust
ArrayVec<T, N>
```

### 3.6 `error.rs`

Errores del core:

```rust
pub enum QueueError {
    SeqOutOfRange,
    AckNotInflight,
    NackNotInflight,
    EmptyDelivery,
    InvalidMask,
    CapacityExceeded,
}
```

La primera versión puede tener métodos infalibles para benchmarks, pero para producción necesitamos una API checked y otra unchecked/internal si el caller ya validó.

### 3.7 `state.rs`

Estado observable por mensaje para tests/model:

```rust
pub enum MessageState {
    Ready,
    Inflight,
    Acked,
    Retry,
    SparseReady,
    Dead,
}
```

No se usa en hot path. Es para tests, snapshots y debugging.

### 3.8 `hybrid.rs`

Define la estructura principal:

```rust
pub struct HybridRangeBlockQueue {
    total_messages: u64,
    next_seq: u64,

    sequential_head: u64,
    sequential_tail: u64,

    blocks: Vec<MsgBlock>,

    sparse_blocks: BlockList,
    retry_blocks: BlockList,
    dirty_blocks: BlockList,

    stats: QueueStats,
}
```

En fases posteriores separar:

```rust
Published but not yet visible
Visible ready sequential
Sparse ready
Inflight
Acked frontier
Retry scheduled
Deadlettered
```

## 4. API pública inicial de `aurum-core::queue`

La API de la primera iteración debe quedar así:

```rust
impl HybridRangeBlockQueue {
    pub fn empty() -> Self;
    pub fn with_messages(total_messages: u64) -> Self;

    pub fn publish_contiguous(&mut self, count: u32) -> DeliveryRange;

    pub fn deliver(&mut self, max_messages: u32, out: &mut DeliveryWork) -> u32;

    pub fn ack_work(&mut self, work: &DeliveryWork);
    pub fn nack_work_to_retry(&mut self, work: &DeliveryWork);

    pub fn ack_range(&mut self, start: Seq, len: u32);
    pub fn ack_mask(&mut self, mask: DeliveryMask);
    pub fn ack_id(&mut self, seq: Seq);

    pub fn nack_range_to_retry(&mut self, start: Seq, len: u32);
    pub fn nack_mask_to_retry(&mut self, mask: DeliveryMask);

    pub fn retry_all_now(&mut self) -> u32;

    pub fn len(&self) -> u64;
    pub fn sequential_ready_len(&self) -> u64;
    pub fn stats(&self) -> QueueStats;

    pub fn debug_state_of(&self, seq: Seq) -> Option<MessageState>;
    pub fn validate_invariants(&self) -> Result<(), InvariantViolation>;
}
```

### 4.1 Notas importantes

- `with_messages(total)` existe solo para benchmarks; el API real será `empty() + publish_contiguous()`.
- `ack_id` existe como fallback de compatibilidad, no como fast path.
- `deliver` debe llenar un batch reutilizable, no asignar memoria cada vez.
- `ack_work` debe aplicar primero ranges y después masks.
- `nack_work_to_retry` debe mover ranges/masks a retry sin convertir a IDs individuales.

## 5. Invariantes del core

Estas invariantes son obligatorias:

### 5.1 Invariantes de estado

Para cada bit de mensaje:

```text
acked && inflight == false
acked && retry == false
acked && sparse_ready == false
inflight && retry == false
inflight && sparse_ready == false
retry && sparse_ready == false
```

Puede existir mensaje no marcado en ningún bit si está en el rango secuencial ready.

### 5.2 Invariantes de rangos

```text
0 <= sequential_head <= sequential_tail <= next_seq <= total/capacity
```

En `with_messages(total)`:

```text
sequential_head = 0
sequential_tail = total
next_seq = total
```

En `empty()`:

```text
sequential_head = 0
sequential_tail = 0
next_seq = 0
```

### 5.3 Invariantes de listas activas

Un bloque está en `sparse_blocks` si y solo si:

```text
block.sparse_word_mask != 0
```

Un bloque está en `retry_blocks` si y solo si:

```text
block.retry_word_mask != 0
```

Durante operaciones internas puede haber momentos transitorios, pero al retornar de una API pública deben cumplirse.

### 5.4 Invariantes de masks

Para cada `DeliveryMask`:

```text
mask.mask != 0
mask.word < WORDS_PER_BLOCK
mask.block < blocks.len()
```

Una máscara entregada debe haber movido bits desde `sparse_ready` hacia `inflight`.

### 5.5 Invariantes de acks

Un `ack_range` válido debe limpiar `inflight` y setear `acked`.

Si se recibe ack de un mensaje no inflight, hay dos modos futuros:

- modo strict: error.
- modo idempotent: ignorar si ya acked.

Para H1, modo idempotent puede ser aceptable para benchmarks. Para H2 Rabbit-like semantics, hay que decidir con precisión.

## 6. Tests obligatorios

### 6.1 Tests unitarios básicos

Archivo sugerido:

```text
queue/tests.rs
```

Casos:

```text
publish_empty_queue
publish_contiguous_increases_ready_tail
deliver_single_range
deliver_cross_block_range
ack_single_range
ack_cross_block_range
nack_range_to_retry
retry_all_now_moves_to_sparse_ready
deliver_sparse_mask
ack_sparse_mask
ack_id_fallback
zero_length_publish_is_noop
zero_length_ack_is_noop
```

### 6.2 Tests de invariantes después de cada operación

Cada operación pública debe tener tests que llamen:

```rust
q.validate_invariants().unwrap();
```

### 6.3 Differential tests contra modelo simple

Crear `queue/model.rs`:

```rust
pub struct ModelQueue {
    states: Vec<MessageState>,
    ready: VecDeque<Seq>,
}
```

Operaciones:

```rust
publish(count)
deliver(max)
ack_range(start, len)
ack_mask(mask)
nack_range_to_retry(start, len)
nack_mask_to_retry(mask)
retry_all_now()
```

Test diferencial:

```text
1. Generar una secuencia determinista de operaciones.
2. Aplicar a ModelQueue.
3. Aplicar a HybridRangeBlockQueue.
4. Comparar conteos y estados observables.
5. Validar invariantes.
```

Primero con generador manual xorshift. Luego con `proptest`.

### 6.4 Tests de edge cases

```text
seq 0
seq 63/64/65
seq 255/256/257
range exactly one word
range crossing word
range crossing block
ack repeated
nack repeated
retry empty
retry block partially empty
sparse block partially delivered
max_messages = 0
max_messages = 1
max_messages > available
```

## 7. Benchmarks H1 integrados

El experimento `experiments/h1-queue-engine` debe dejar de duplicar lógica y usar `aurum-core`.

### 7.1 Workloads mínimos

```text
deliver_ack
random_ack
nack_retry_ack
ack_multiple
windowed_random_ack
sparse_ready_1_percent
slow_consumer
mixed_interleaved
```

### 7.2 Métricas mínimas

```text
ns/message
messages/sec
range_count
mask_count
ack_range_count
ack_mask_count
allocations/op si se instrumenta
```

Con perf externo:

```text
cycles
instructions
branches
branch-misses
cache-references
cache-misses
LLC-loads
LLC-load-misses
```

### 7.3 Baselines

Mantener al menos:

```text
per_message_vecdeque
hybrid_range_block
```

Agregar después:

```text
per_message_intrusive
bitset_only
range_only
word_mask_queue
```

No mezclar todos en producción; son solo investigación.

## 8. Implementación por fases

## Fase 1 — Refactor estructural sin cambiar comportamiento

Objetivo: mover `queue.rs` a módulos sin cambiar resultados.

Tareas:

1. Crear `src/queue/mod.rs`.
2. Mover constantes a `constants.rs`.
3. Mover `MsgBlock` a `block.rs`.
4. Mover `BlockList`/`ListKind` a `lists.rs`.
5. Mover `HybridRangeBlockQueue` a `hybrid.rs`.
6. Mover helpers `locate`, `take_lowest_bits_local` o reemplazarlos por `aurum-kernels`.
7. Mantener re-exports en `lib.rs`.
8. Correr:

```bash
cargo test -p aurum-core
cargo run --release -p h1-queue-engine -- --messages=1048576 --batch=128 --workload=deliver_ack --variant=both
```

DoD:

- Compila.
- Tests actuales pasan.
- Bench H1 no se rompe.
- API pública no cambia salvo imports internos.

## Fase 2 — API real de publish

Objetivo: dejar de usar `with_messages` como forma principal.

Tareas:

1. Implementar `HybridRangeBlockQueue::empty()`.
2. Implementar `publish_contiguous(count)`.
3. Hacer que `with_messages(total)` use `empty() + publish_contiguous(total)` o inicialice estado equivalente.
4. Agregar capacidad dinámica de bloques:
   - si `next_seq + count` requiere nuevos bloques, extender `blocks`.
5. Tests:
   - publish en queue vacía.
   - publish que cruza bloque.
   - publish múltiple.

DoD:

- `with_messages` sigue funcionando.
- `publish_contiguous` no marca inflight/acked/retry.
- `deliver` consume desde `sequential_head..sequential_tail`.

## Fase 3 — Invariants + debug state

Objetivo: tener seguridad semántica antes de optimizar.

Tareas:

1. Implementar `MessageState` observable.
2. Implementar `debug_state_of(seq)`.
3. Implementar `validate_invariants()`.
4. Agregar `InvariantViolation` con información útil:
   - block.
   - word.
   - mask conflict.
   - list mismatch.
5. Llamar invariants en tests principales.

DoD:

- Se detecta conflicto si manualmente se setean bits incompatibles.
- Se detecta bloque en lista equivocada.
- Se detecta `sequential_head > sequential_tail`.

## Fase 4 — Differential model

Objetivo: validar correctness con operaciones pseudoaleatorias.

Tareas:

1. Implementar `ModelQueue` simple.
2. Implementar generador determinista de operaciones:
   - publish.
   - deliver.
   - ack delivered work.
   - nack delivered work.
   - retry now.
   - ack_id.
3. Comparar conteos:
   - total published.
   - ready.
   - inflight.
   - acked.
   - retry.
4. Comparar estados por sampling de secuencias.
5. Agregar test con 1000, 10_000, 100_000 operaciones.

DoD:

- Differential test pasa en debug.
- No hay duplicados evidentes.
- No se pierden mensajes.

## Fase 5 — Work batches sin heap allocation frecuente

Objetivo: que `DeliveryWork` no haga allocations recurrentes.

Opciones:

1. Reutilizar `Vec` con `clear()` y `reserve()`.
2. Cambiar a `SmallVec`.
3. Cambiar a `ArrayVec` de capacidad fija.

Plan:

- Primero mantener `Vec` y garantizar que experiment reuse un solo `DeliveryWork`.
- Medir allocations.
- Si aparecen allocations, migrar a `SmallVec` o `ArrayVec`.

DoD:

- En H1 no debe haber allocation por mensaje.
- Idealmente no debe haber allocation por batch después del warmup.

## Fase 6 — Ack/Nack types separados

Objetivo: preparar H2 Rabbit-like acks.

Tareas:

1. Agregar `AckRange`, `AckMask`, `AckBatch` en `aurum-types` o `aurum-core::queue::work`.
2. Agregar `NackRange`, `NackMask`, `NackBatch`, `NackReason`.
3. Convertir `ack_work(&DeliveryWork)` en wrapper que genera `AckBatch`.
4. Convertir `nack_work_to_retry(&DeliveryWork)` en wrapper que genera `NackBatch`.
5. Agregar API explícita:

```rust
pub fn apply_ack_batch(&mut self, batch: &AckBatch) -> AckOutcome;
pub fn apply_nack_batch(&mut self, batch: &NackBatch) -> NackOutcome;
```

DoD:

- El core ya no depende semánticamente de que el ack venga del delivery work original.
- Es posible construir ack batches desde adapters.

## Fase 7 — Rabbit-like ack semantics groundwork

Objetivo: preparar soporte para `basic.ack`, `multiple=true`, `basic.nack`, redelivery.

Tareas:

1. Definir `DeliveryTag` futuro:

```text
shard_id | queue_local | block | generation | offset
```

2. Para `aurum-core`, definir mapping local:

```rust
pub fn seq_to_local_delivery_ref(seq) -> DeliveryRef;
pub fn delivery_ref_to_seq(ref) -> Seq;
```

3. Implementar `ack_multiple_until(seq)`.
4. Implementar `ack_range` como fast path.
5. Implementar `ack_id` como fallback.
6. Definir comportamiento de double ack:
   - para ahora idempotent.
   - después configurable strict/protocol.

DoD:

- `ack_multiple_until` evita iterar mensaje por mensaje.
- `ack_id` sigue correcto pero no domina benchmarks.

## Fase 8 — Retry scheduling abstraction

Objetivo: separar “retry now” de “retry after deadline”.

Tareas:

1. Añadir `retry_after` metadata mínima en `MsgBlock` o estructura lateral.
2. Para H1 mantener `retry_all_now`.
3. Preparar trait interno:

```rust
pub trait RetryScheduler {
    fn schedule_mask(block, word, mask, deadline_tick);
    fn pop_due(deadline_tick, out);
}
```

4. No implementar timing wheel completo aún, solo interfaz.

DoD:

- `nack_to_retry` no obliga a retry inmediato.
- `retry_all_now` sigue como helper de benchmark.

## Fase 9 — Dirty tracking para storage

Objetivo: preparar storage sin acoplar `aurum-storage`.

Tareas:

1. Agregar `dirty_word_mask` a `MsgBlock` si no existe.
2. Cuando se cambie `inflight`, `acked`, `retry`, `sparse_ready`, marcar dirty.
3. Agregar `dirty_blocks`.
4. API:

```rust
pub fn drain_dirty_blocks(&mut self, out: &mut DirtyBatch) -> usize;
```

5. `DirtyBatch` solo contiene block ids/masks, no hace I/O.

DoD:

- Storage futuro puede preguntar qué bloques cambiaron.
- No se escanea toda la queue para snapshots incrementales.

## Fase 10 — Performance review

Objetivo: optimizar con datos.

Tareas:

1. Correr H1 con mensajes:
   - 1M.
   - 4M.
   - 16M.
   - 64M.
2. Correr batch:
   - 1.
   - 8.
   - 32.
   - 128.
   - 512.
3. Correr perf:

```bash
scripts/perf_h1.sh
```

4. Revisar assembly de:
   - `deliver`.
   - `mark_inflight_range`.
   - `ack_range`.
   - `nack_mask_to_retry`.
   - `retry_all_now`.
5. Optimizar solo si hay evidencia.

DoD:

- H1 report document con tabla comparativa.
- No hacer intrinsics antes de saber dónde está el cuello.

## 9. Criterios de aceptación de `aurum-core` v0.1

`aurum-core` v0.1 está listo cuando:

```text
1. Tiene HybridRangeBlockQueue modular.
2. Tiene publish_contiguous real.
3. Tiene deliver range/mask.
4. Tiene ack range/mask/id.
5. Tiene nack range/mask to retry.
6. Tiene retry_all_now.
7. Tiene invariants.
8. Tiene differential model tests.
9. H1 benchmark usa aurum-core real.
10. No depende de protocolos, runtime ni storage.
```

## 10. Criterios de aceptación de performance H1

Con `messages=4_194_304`, `batch=128`, en máquina local del desarrollador:

```text
deliver_ack:
  hybrid_range_block debe ganar claramente contra per_message_vecdeque.

nack_retry_ack:
  hybrid_range_block debe ganar de forma fuerte.

random_ack:
  hybrid_range_block debe ganar, empatar o perder poco; si pierde, revisar si el workload es random global irreal.
```

Más importante que el tiempo bruto:

```text
cycles/message
instructions/message
LLC-load-misses/message
branch-misses/message
allocations/message
```

## 11. Riesgos

### 11.1 Riesgo: diseño demasiado complejo para el caso simple

Mitigación:

- Mantener `sequential_ready` como rango, no como bitset.
- No meter sparse blocks para publish normal.

### 11.2 Riesgo: API interna todavía demasiado acoplada a `DeliveryWork`

Mitigación:

- Separar `DeliveryBatch`, `AckBatch`, `NackBatch`.

### 11.3 Riesgo: active lists con bugs sutiles

Mitigación:

- Invariants.
- Differential tests.
- Tests específicos de remove/push/list membership.

### 11.4 Riesgo: benchmarks irreales

Mitigación:

- Agregar workloads de prefetch window.
- Agregar mixed interleaved.
- No basarse solo en random global.

### 11.5 Riesgo: optimizar antes de estabilizar semántica

Mitigación:

- Primero correctness.
- Luego perf.
- Intrinsics solo después de perf/asm.

## 12. Primer PR recomendado

Título:

```text
core: split queue engine into modules and add invariant checks
```

Contenido:

```text
- Crear src/queue/mod.rs.
- Mover MsgBlock a block.rs.
- Mover BlockList a lists.rs.
- Mover HybridRangeBlockQueue a hybrid.rs.
- Agregar state.rs con MessageState.
- Agregar invariants.rs con validate_invariants.
- Mantener comportamiento existente.
- Agregar tests de invariants básicos.
```

No incluir todavía:

```text
- publish_contiguous.
- AckBatch/NackBatch nuevos.
- timing wheel.
- storage dirty drain.
```

## 13. Segundo PR recomendado

Título:

```text
core: add publish_contiguous and differential model tests
```

Contenido:

```text
- empty().
- publish_contiguous(count).
- with_messages basado en publish.
- ModelQueue.
- differential deterministic tests.
```

## 14. Tercer PR recomendado

Título:

```text
core: introduce explicit ack/nack batches
```

Contenido:

```text
- AckRange/AckMask/AckBatch.
- NackRange/NackMask/NackBatch.
- apply_ack_batch.
- apply_nack_batch.
- wrappers desde DeliveryWork.
```

## 15. Cuarto PR recomendado

Título:

```text
experiments: benchmark aurum-core queue engine directly
```

Contenido:

```text
- H1 usa HybridRangeBlockQueue de aurum-core.
- Workloads nuevos.
- scripts actualizados.
- README del experimento.
```

## 16. Quinta iteración: perf-specific

Título:

```text
core: optimize queue hot paths based on H1 perf results
```

Posibles cambios, solo si datos lo justifican:

```text
- SmallVec/ArrayVec para work batches.
- Especialización fast path len <= 64.
- Eliminar closures de apply_range si aparecen en asm.
- Reemplazar BlockList return-by-value por mutación directa.
- Inlining explícito en locate/set_range.
- Mask LUTs para rangos dentro de word.
```

## 17. Notas de implementación importantes

### 17.1 Evitar closures en hot path si aparecen en asm

Actualmente `apply_range` usa closures. Puede estar bien si LLVM inlinea. Si no, reescribir manualmente:

```rust
fn mark_inflight_range(...)
fn ack_range(...)
fn nack_range_to_retry(...)
```

con loops propios.

### 17.2 `BlockList` debería mutar en sitio

Actualmente algunos métodos retornan `Self`. Eso es elegante, pero puede generar copias. Cambiar a:

```rust
fn push_back(&mut self, ...)
fn pop_front(&mut self, ...) -> Option<u32>
fn remove(&mut self, ...)
```

si perf/asm lo justifica. Para claridad, puede hacerse ya.

### 17.3 `DeliveryWork` debería reutilizar buffers

En benchmarks y runtime:

```rust
let mut work = DeliveryWork::with_capacity(...);
loop {
    q.deliver(batch, &mut work);
    ...
}
```

No crear `DeliveryWork` por iteración.

### 17.4 `ack_id` no debe ser benchmark principal

`ack_id` existe, pero los adapters deben coalescer. El diseño final depende de que el core vea ranges/masks.

### 17.5 Strict vs idempotent semantics se decide en H2

H1 puede ser idempotent. H2 debe decidir Rabbit-like exacto.

## 18. Comandos de trabajo

Desde el workspace:

```bash
PATH=/mnt/data/rust/bin:$PATH cargo test -p aurum-core
PATH=/mnt/data/rust/bin:$PATH cargo test --workspace
PATH=/mnt/data/rust/bin:$PATH cargo run --release -p h1-queue-engine -- --messages=4194304 --batch=128 --workload=deliver_ack --variant=both
PATH=/mnt/data/rust/bin:$PATH cargo run --release -p h1-queue-engine -- --messages=4194304 --batch=128 --workload=nack_retry_ack --variant=both
```

En máquina local con perf:

```bash
taskset -c 2 perf stat \
  -e cycles,instructions,branches,branch-misses,cache-references,cache-misses,LLC-loads,LLC-load-misses \
  target/release/h1-queue-engine \
  --messages=4194304 --batch=128 --workload=nack_retry_ack --variant=hybrid
```

## 19. Resultado esperado

Al terminar este plan, `aurum-core` debe ser una pieza autónoma y confiable:

```text
protocol adapters -> CommandBatch -> aurum-core queue engine -> DeliveryWork/Ack outcomes
```

No será todavía un broker. Pero sí será el núcleo que justifica el resto del proyecto.

La decisión central que debe quedar codificada es:

> El core no procesa mensajes como objetos individuales. Procesa ranges, masks, blocks y listas activas de bloques.
