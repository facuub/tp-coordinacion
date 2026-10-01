# Informe

Se eligió la implementación en Python.

## Middleware

Se implementaron `MessageMiddlewareQueueRabbitMQ` y `MessageMiddlewareExchangeRabbitMQ` sobre `pika`.

- **Queue**: declara la cola y publica a través del exchange por defecto. Varios consumidores sobre la misma cola se reparten los mensajes (_work queue_), con ack manual.
- **Exchange**: declara un exchange `direct` y, por cada routing key, una cola `<exchange>.<routing_key>` ligada a él. Tanto productores como consumidores declaran estas colas, por lo que no se pierden mensajes sin importar el orden de arranque. `send` publica en todas las routing keys con las que se construyó el objeto, lo que permite tanto envíos dirigidos como _broadcast_.
- Si falla algún paso posterior a la conexión (declaración de colas, exchange o bindings), el constructor cierra la conexión antes de propagar el error.
- Colas, exchanges y mensajes son no durables de forma consistente, ya que el sistema no contempla reinicios del broker.
- `stop_consuming` usa `add_callback_threadsafe`, de modo que puede invocarse desde un handler de SIGTERM o desde otro hilo.

## Separación de flujos por cliente

El `MessageHandler` del gateway genera un `client_id` (UUID) por conexión y lo antepone a todos los mensajes internos:

- Dato: `[client_id, fruta, cantidad]`
- EOF: `[client_id, total_de_registros]`, donde el total es la cantidad de registros enviados por ese cliente.
- Resultado: `[client_id, top]`. Cada handler devuelve el top sólo si el `client_id` coincide con el suyo, de modo que el gateway entrega cada resultado a su cliente.

Todos los controles mantienen su estado indexado por `client_id`, por lo que se atienden varios clientes en simultáneo y el estado de cada cliente se libera al finalizar.

## Coordinación de Sum

Las instancias de Sum consumen de la misma cola de entrada y se reparten los registros. Cada una acumula, por cliente, la suma parcial por fruta (usando `FruitItem`) y la cantidad de registros procesados.

El EOF de un cliente llega a una sola instancia. Esa instancia lo reenvía por broadcast en el exchange de control `SUM_CONTROL_EXCHANGE`, en el que cada Sum tiene su propia routing key (`<SUM_PREFIX>_<ID>`). Cada Sum consume ese exchange en un hilo aparte, con su propia conexión, y comparte el estado con el hilo de datos mediante un lock.

Al recibir el control, cada Sum hace un _flush_ del cliente: particiona las frutas acumuladas por `crc32(fruta) % AGGREGATION_AMOUNT` y envía un único mensaje a cada Aggregator:

`[client_id, total, registros_procesados, es_control, frutas_de_esa_particion]`

Se usa `zlib.crc32` porque es un hash simple, no criptográfico, y da el mismo resultado en todos los procesos. El `hash()` de Python se aleatoriza por proceso, y fijar `PYTHONHASHSEED` requeriría modificar archivos que se reemplazan en la evaluación.

Como el control viaja por otra conexión, un registro de ese cliente que ya estuviera en vuelo hacia un Sum puede procesarse después del flush. Por eso, si llega un dato de un cliente que ya recibió el control, se hace un flush inmediato de ese registro (con `es_control = false`). Así no se pierde ningún dato sin depender del orden de entrega entre colas distintas.

## Coordinación de Aggregation

Cada fruta se asigna siempre al mismo Aggregator, lo que evita el procesamiento redundante (ninguna fruta se suma en dos Aggregators y cada uno recibe sólo su partición).

Cada Aggregator acumula, por cliente, las frutas recibidas, la suma de `registros_procesados` y la cantidad de flushes de control. Un cliente está completo cuando:

- se recibieron `SUM_AMOUNT` flushes de control (todos los Sum vieron el EOF), y
- la suma de registros reportados es igual al `total` enviado por el gateway (todos los registros fueron contabilizados).

Como cada flush lleva en un mismo mensaje los datos y la cantidad de registros que representa, al cumplirse esa condición el Aggregator tiene todos sus datos. Entonces calcula su top parcial de tamaño `TOP_SIZE` y lo envía al Join. Como las frutas están particionadas, el top parcial de cada Aggregator es exacto para su partición.

## Join

Join recibe, por cliente, un top parcial de cada Aggregator. Al recibir `AGGREGATION_AMOUNT` tops, los ordena con `FruitItem`, toma los `TOP_SIZE` primeros y envía `[client_id, top]` al gateway.

## Escalabilidad

- **Clientes**: los mensajes llevan `client_id` y el estado está separado por cliente, por lo que las consultas se resuelven de forma concurrente sobre las mismas instancias.
- **Volumen de datos**: Sum reduce los registros a un par por fruta y por cliente antes de enviarlos. Entre Sum y Aggregation se envía un único mensaje por Aggregator en cada flush, y cada Aggregator envía sólo `TOP_SIZE` elementos al Join.
- **Cantidad de controles**: agregar instancias de Sum reparte la ingesta mediante la work queue. Agregar instancias de Aggregation reparte las frutas mediante el particionado por hash. La coordinación sólo depende de `SUM_AMOUNT` y `AGGREGATION_AMOUNT`, que se toman de la configuración.

## SIGTERM

Sum, Aggregation y Join manejan SIGTERM deteniendo el consumo de sus colas. Luego cierran sus conexiones y terminan con código 0. En Sum se detienen ambos hilos de consumo y se espera al hilo de control antes de cerrar.
