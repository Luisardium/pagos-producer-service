Nombre: Luis Alfonso Morán Juárez  
Carnet: 0905-23-15636  
Repositorio: https://github.com/Luisardium/pagos-producer-service  
Modulo asignado: Envío de pagos a sistemas externos por colas  

# Oficina del Agua – Envío de pagos a sistemas externos por colas

Microservicio que toma los pagos cobrados por la Oficina del Agua y los entrega, de forma
asíncrona y confiable, a dos sistemas externos: el **banco** y el **ERP**. Usa Kafka como cola,
reintenta los fallos temporales, aparta los pagos que no se pudieron entregar y deja un
historial de cada intento en DynamoDB.

## Tabla de contenido

1. [Arquitectura general](#1-arquitectura-general)
2. [Ejecución del sistema](#2-ejecución-del-sistema)
3. [Módulo: envío de pagos a sistemas externos por colas](#3-módulo-envío-de-pagos-a-sistemas-externos-por-colas)
   - [3.1 Funcionalidad y objetivo](#31-funcionalidad-y-objetivo)
   - [3.2 Integración y responsabilidades](#32-integración-y-responsabilidades)
   - [3.3 Decisiones técnicas](#33-decisiones-técnicas)
   - [3.4 Instrucciones para probar el módulo](#34-instrucciones-para-probar-el-módulo)
4. [Estructura del código](#4-estructura-del-código)
5. [Alcance y limitaciones](#5-alcance-y-limitaciones)

---

## 1. Arquitectura general

### 1.1 Vista de la solución

Diagrama en Lucid: https://drive.google.com/file/d/1s60IZCqv2KyIiH-TmtLDVI1u3NqI08GR/view?usp=sharing

### 1.2 Componentes de la solución

| Componente | Tecnología | Rol | Puerto |
|---|---|---|---|
| Monolito de la Oficina del Agua | PHP | Sistema existente. Registra cada pago cobrado en la tabla `pagos`. **No se modifica.** | – |
| Base de datos | MariaDB (contenedor, compatible con MySQL), base `oficina_agua` | Guarda los pagos y su estado de entrega (flags y comprobantes) | 3306 |
| Cola de mensajes | Apache Kafka (contenedor) | Desacopla el cobro de la entrega a los sistemas externos. Topic `bank-payments-topic` y su dead-letter `bank-payments-topic-dlt` | 9092 |
| Consola de Kafka | Kafka UI (contenedor) | Inspeccionar topics, mensajes y grupos de consumidores | 8080 |
| Microservicio de pagos | Java 21, Spring Boot 4.1, Spring Kafka, Spring Data JPA, OpenFeign | Contiene el producer y los consumers del banco y del ERP | 8081 |
| Bitácora de transacciones | DynamoDB Local (contenedor) | Historial de cada intento de entrega: exitosos y fallidos, con la razón | 8000 |
| Consola de DynamoDB | DynamoDB Admin (contenedor) | Consultar la tabla `payment_logs` desde el navegador | 8001 |
| API del banco | Simulador del docente (AWS API Gateway) | Sistema externo. Registra el pago y devuelve un comprobante | – |
| API del ERP | Simulador del docente (AWS API Gateway) | Sistema externo. Contabiliza el pago y devuelve un comprobante | – |

Los contratos de las APIs externas están en `src/main/resources/openapi/`
(`bank-payments-api.yaml` y `erp-payments-api.yaml`). Los clientes HTTP se generan desde esos
contratos al compilar el proyecto.

### 1.3 Integración entre componentes

- **Monolito → microservicio:** el monolito no llama al microservicio ni sabe que existe. Solo
  inserta el pago en la tabla `pagos`. El microservicio detecta los pagos nuevos consultando
  esa tabla por flags (`sent_to_bank = 0` y `enqueued = 0`).
- **Producer → Kafka:** cada 10 segundos el producer publica los pagos pendientes en
  `bank-payments-topic` y solo después de que Kafka confirma la recepción marca `enqueued = 1`.
- **Kafka → consumers:** los dos consumers leen el mismo topic con grupos de consumidores
  distintos, por lo que cada uno recibe todos los pagos y avanza a su propio ritmo.
- **Consumers → sistemas externos:** cada consumer traduce el pago al formato de su destino y lo
  envía por HTTP (cliente Feign) con una llave de idempotencia estable.
- **Consumers → MySQL/MariaDB:** al confirmarse la entrega actualizan sus flags
  (`sent_to_bank` / `sent_to_erp`) y guardan el comprobante.
- **Consumers → DynamoDB:** registran cada intento (éxito, reintento, fallo definitivo) con su
  destino y la razón.
- **Dead-letter → DynamoDB:** un listener registra los pagos que terminaron en la dead-letter.

---

## 2. Ejecución del sistema

### 2.1 Requisitos previos

- Docker Desktop **en ejecución**.
- JDK 21.
- La API key del bancoy ERP

### 2.2 Variables de entorno

Las credenciales no están en el repositorio. Se entregan por variables de entorno.

| Variable | Obligatoria | Descripción |
|---|---|---|
| `BANK_API_KEY` | Sí | API key del banco |
| `ERP_API_KEY` | No | API key del ERP. Si no se define se usa `BANK_API_KEY` |
| `DB_PASSWORD` | No | Contraseña de la BD. Por defecto `root` |
| `BANK_SIMULATIONSCENARIO` | No | Fuerza un escenario del simulador del banco (por defecto `AUTO`) |
| `ERP_SIMULATIONSCENARIO` | No | Fuerza un escenario del simulador del ERP (por defecto `AUTO`) |

Además, en `src/main/resources/application.yaml`:

- `bank.team-code` y `erp.team-code` deben corresponder al equipo de la API key.
- `erp.base-url` debe llevar el identificador del API Gateway del ERP.

### 2.3 Levantar la infraestructura

```powershell
docker compose up -d
```

Levanta MariaDB, Kafka, Kafka UI, DynamoDB Local y DynamoDB Admin. Para comprobar que están
arriba:

```powershell
docker compose ps
```

### 2.4 Preparar la tabla `pagos`

El módulo usa columnas de control en la tabla `pagos`. Con un cliente SQL
(host `localhost`, puerto `3306`, base `oficina_agua`, usuario `root`) ejecutar:

```sql
ALTER TABLE pagos
  ADD COLUMN IF NOT EXISTS bank_receipt_id VARCHAR(30) NULL,
  ADD COLUMN IF NOT EXISTS sent_to_erp TINYINT(1) NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS erp_receipt_id VARCHAR(30) NULL;
```

Las columnas nuevas tienen valor por defecto o aceptan nulos, por lo que los `INSERT` del
monolito siguen funcionando sin cambios.

### 2.5 Levantar el microservicio

```powershell
$env:BANK_API_KEY = "<api-key-entregada>"
.\mvnw spring-boot:run
```

En Linux o macOS: `export BANK_API_KEY=<api-key-entregada>` y `./mvnw spring-boot:run`.

La primera vez, Maven genera los clientes del banco y del ERP desde los contratos OpenAPI.
Al arrancar, la tabla `payment_logs` de DynamoDB se crea sola y aparece en los logs:
`Tabla DynamoDB creada: payment_logs`.

### 2.6 Detener

```powershell
docker compose down        # conserva los datos
docker compose down -v     # borra también los volúmenes
```

---

## 3. Módulo: envío de pagos a sistemas externos por colas

### 3.1 Funcionalidad y objetivo

**Problema de negocio.** Cuando un ciudadano paga en la Oficina del Agua, ese pago debe
notificarse al banco y contabilizarse en el ERP. Ambos sistemas son externos: pueden ser
lentos, fallar a ratos o responder tarde. Si el monolito los llamara directamente al cobrar,
una caída del banco o del ERP bloquearía o haría perder pagos, y un reintento mal hecho podría
registrar el mismo pago dos veces.

**Qué se construyó.** Un microservicio que desacopla el cobro de la entrega:

1. El monolito registra el pago en la tabla `pagos` (`sent_to_bank = 0`, `enqueued = 0`).
2. El **producer** consulta cada 10 segundos los pagos pendientes y publica un evento por pago en
   Kafka, usando como clave la llave de idempotencia del pago. Solo cuando Kafka confirma la
   recepción marca `enqueued = 1`; si Kafka falla, el pago se reintenta en el siguiente ciclo.
3. Dos **consumers** independientes leen el evento: uno entrega al **banco** y otro al **ERP**.
4. Cada consumer adapta el pago al contrato de su destino:

   | | Banco | ERP |
   |---|---|---|
   | Llave de idempotencia | Header `Idempotency-Key` | Campo `documento_origen.numero` |
   | Monto | Centavos enteros (`15075`) | Texto con 2 decimales (`"150.75"`) |
   | Datos propios | Canal de pago | Periodo fiscal y detalle por rubro |
   | Comprobante | `receiptId` | `numero_comprobante` |

5. Según la respuesta del destino:

   | Respuesta | Acción |
   |---|---|
   | Éxito (`201`) | Marca el flag, guarda el comprobante, registra `SUCCESS` en DynamoDB |
   | Duplicado (`409`: `DUPLICATE_PAYMENT` / `DOCUMENTO_DUPLICADO`) | El destino ya tenía el pago: se trata como éxito |
   | Timeout, `429` o `5xx` | Error temporal: reintenta con espera creciente y registra `RETRY` |
   | `400`, `403`, `422`, llave reutilizada o monto inválido | Error definitivo: va directo a la dead-letter y registra `FAILED` |
   | Reintentos agotados | Va a la dead-letter y se registra `DEAD_LETTER` |

6. Cada intento queda en DynamoDB (`payment_logs`) con el destino (`BANK` o `ERP`), el código HTTP,
   el código de error del destino y la razón del fallo.

### 3.2 Integración y responsabilidades

- **El monolito se mantiene sin modificaciones.** Su única relación con el módulo es la tabla
  `pagos`: él inserta, el microservicio lee los pendientes y actualiza las columnas de control.
  No hay llamadas directas entre ambos, así que una caída del microservicio no afecta el cobro.
- **Solo el microservicio habla con el banco y el ERP.** Las credenciales viven en variables de
  entorno del microservicio, no están en el repositorio y nunca llegan a un navegador.
- **MySQL/MariaDB guarda el estado del negocio.** Los flags y comprobantes de `pagos` responden
  a "¿este pago ya se entregó?".
- **Kafka desacopla y absorbe los fallos.** Si un destino está caído, los pagos esperan en la cola
  en lugar de perderse, y un destino lento no frena al otro.
- **DynamoDB es la bitácora de auditoría, no parte del flujo del pago.** Responde "¿qué le pasó a
  este pago?": cuántos intentos hubo, por qué falló y si terminó en dead-letter. Es de solo
  escritura: si DynamoDB no está disponible, el error se anota en el log de la aplicación y el
  pago se procesa igual. Se separa de MySQL para no llenar la base del monolito con tablas de
  auditoría.
- **Los sistemas externos se consumen mediante contratos OpenAPI.** Los clientes se generan desde
  `bank-payments-api.yaml` y `erp-payments-api.yaml`, así que el código siempre coincide con el
  contrato.

### 3.3 Decisiones técnicas

**1. Un grupo de consumidores por destino.** El banco y el ERP leen el mismo topic con grupos
distintos (`pagos-bank-consumer` y `pagos-erp-consumer`), de modo que cada grupo recibe todos los
pagos y lleva su propio avance.

- *Alternativa descartada:* un solo listener que llame al banco y luego al ERP. Si el ERP fallaba,
  el reintento volvía a llamar al banco, y en la dead-letter no se sabría cuál de los dos falló.
- *Alternativa descartada:* un topic por destino. Obligaría al producer a publicar dos veces cada
  pago, con el riesgo de que una publicación funcione y la otra no.
- *Resultado:* si el ERP está caído, el banco sigue entregando pagos con normalidad.

**2. Llave de idempotencia estable.** El identificador del pago (`idempotency_key`) se envía como
`Idempotency-Key` al banco y como `documento_origen.numero` al ERP, y es el mismo en cada reintento.
Un timeout es ambiguo: el destino pudo haber registrado el pago aunque la respuesta se perdiera. Con
una llave estable, reintentar es seguro: si ya lo tenía, responde `409` y el consumer lo trata como
éxito en lugar de duplicarlo.

- *Alternativa descartada:* no reintentar tras un timeout, que dejaría pagos sin entregar.
- *Alternativa descartada:* generar una llave nueva por intento, que registraría el mismo pago
  varias veces.

**3. Clasificar los errores antes de reintentar, con backoff exponencial y dead-letter.** Los
errores temporales (timeout, `429`, `5xx`) se reintentan hasta 5 veces con esperas de 1, 2, 4, 8 y
15 segundos. Los definitivos (`400`, `403`, `422`, llave reutilizada, monto inválido) se mandan
directo a la dead-letter, porque reintentarlos no los arregla.

- *Alternativa descartada:* reintentar todo, que repetiría peticiones inválidas y retrasaría el
  resto de los pagos de esa partición.
- *Alternativa descartada:* no reintentar nada, que convertiría cualquier falla momentánea del
  banco en un pago perdido.

**4. Actualizaciones dirigidas en MySQL.** Banco y ERP terminan casi al mismo tiempo y actualizan
la misma fila de `pagos`. Con un `save` de la entidad, Hibernate reescribe todas las columnas con
lo que leyó antes, y un consumer podía pisar el flag del otro con un valor viejo. Por eso cada uno
ejecuta un `UPDATE` que toca solo sus columnas (`markSentToBank` y `markSentToErp`).

### 3.4 Instrucciones para probar el módulo

**Requisitos previos:** haber seguido la sección [2. Ejecución del sistema](#2-ejecución-del-sistema)
(contenedores arriba, tabla preparada y microservicio corriendo).

**Páginas útiles:**

| Qué | URL |
|---|---|
| Kafka UI (topics, mensajes, grupos) | http://localhost:8080 |
| DynamoDB Admin (tabla `payment_logs`) | http://localhost:8001 |

#### Prueba 0: APIs externas disponibles

El endpoint de salud de ambas APIs no requiere API key. Usar la URL base de cada API Gateway
(termina en `/prod`):

```powershell
curl.exe -i <URL_BASE_BANCO>/bank/v1/health
curl.exe -i <URL_BASE_ERP>/erp/v1/salud
```

Resultado esperado: `200` en ambas.

#### Prueba 1: pago normal a banco y ERP

1. Insertar un pago pendiente:

   ```sql
   INSERT INTO pagos (idempotency_key, customer_code, customer_name, tax_id, amount, sent_to_bank, enqueued, created_at)
   VALUES ('EQB-DEMO-0001', 'CLI-0001', 'Ana Lopez', 'CF', 150.75, 0, 0, NOW());
   ```

   Si la tabla `pagos` tiene otras columnas obligatorias, completarlas.

2. Esperar unos 15 segundos y consultar:

   ```sql
   SELECT idempotency_key, enqueued, sent_to_bank, bank_receipt_id, sent_to_erp, erp_receipt_id
   FROM pagos WHERE idempotency_key = 'EQB-DEMO-0001';
   ```

**Resultado esperado:**
- `enqueued = 1`, `sent_to_bank = 1`, `sent_to_erp = 1`.
- `bank_receipt_id` con formato `BNK-...` y `erp_receipt_id` con formato `ERP-...`.
- En la consola de la aplicación: `Pago EQB-DEMO-0001 registrado en el banco...` y
  `Pago EQB-DEMO-0001 contabilizado en el ERP...`.
- En DynamoDB Admin, tabla `payment_logs`: dos filas `SUCCESS` para ese pago, una con
  `destination = BANK` y otra con `destination = ERP`.
- En Kafka UI, el mensaje en `bank-payments-topic` y los grupos `pagos-bank-consumer` y
  `pagos-erp-consumer` sin retraso.

#### Prueba 2: un destino caído no afecta al otro

1. Detener el microservicio y volver a iniciarlo forzando el fallo del ERP:

   ```powershell
   $env:ERP_SIMULATIONSCENARIO = "ERROR_503"
   .\mvnw spring-boot:run
   ```

2. Insertar un pago nuevo con otra llave (por ejemplo `EQB-DEMO-0002`) y esperar unos 40 segundos.

**Resultado esperado:**
- `sent_to_bank = 1` y `sent_to_erp = 0`: el banco se completó aunque el ERP falló.
- En la consola, reintentos del ERP con espera creciente y al final `PAGO EN DEAD-LETTER EQB-DEMO-0002 (ERP)`.
- En DynamoDB: varias filas `RETRY` del ERP (con `errorCode = SERVICE_UNAVAILABLE`) seguidas de una
  `DEAD_LETTER`, además de la fila `SUCCESS` del banco.
- En Kafka UI, un mensaje en el topic `bank-payments-topic-dlt`.

> Si el simulador tiene deshabilitada la selección de escenarios, ignorará esta variable y
> responderá en modo aleatorio (`AUTO`); en ese caso los resultados pueden variar.

#### Prueba 3: reencolar un pago sin duplicarlo

1. Quitar el escenario forzado (`Remove-Item Env:ERP_SIMULATIONSCENARIO`) y reiniciar el microservicio.
2. Reencolar el pago de la prueba 2:

   ```sql
   UPDATE pagos SET enqueued = 0 WHERE idempotency_key = 'EQB-DEMO-0002';
   ```

**Resultado esperado:**
- `sent_to_erp = 1` con su comprobante.
- El banco, que ya tenía el pago, responde duplicado y se registra como éxito con el detalle
  `El banco ya tenía el pago (DUPLICATE_PAYMENT)`. `sent_to_bank` sigue en `1` y no se crea un
  segundo pago en el banco.
  
---

## 4. Estructura del código

Paquete base `com.oficinaagua.pagos_producer`:

| Paquete | Contenido |
|---|---|
| `producer` | Scheduler que lee los pagos pendientes y los publica en Kafka |
| `consumer` | Consumers del banco y del ERP, clasificadores de errores y listener de dead-letter |
| `mapper` | Traducción del evento de pago al formato de cada contrato externo |
| `service` | Actualización del estado en MySQL y escritura de los logs en DynamoDB |
| `config` | Propiedades (`bank.*`, `erp.*`, `dynamodb.*`), errores y reintentos de Kafka, cliente DynamoDB |
| `domain` | Evento de mensajería, entidad JPA de `pagos` y entidad de `payment_logs` |
| `repository` | Acceso a la tabla `pagos` |
| `exception` | Excepciones que distinguen error temporal de error definitivo |
| `banco` / `erp` | Clientes HTTP generados desde los contratos OpenAPI (en `target/` al compilar) |

---

## 5. Alcance y limitaciones

- **Lambda no implementada.** El diagrama de arquitectura contempla una función Lambda que
  actualice el flag en MySQL a partir de los registros de DynamoDB. En esta implementación los
  consumers actualizan el flag directamente, y DynamoDB se usa solo como bitácora de auditoría.
- **DynamoDB local.** La bitácora usa DynamoDB Local en contenedor, sin cuenta de AWS. Para usar el
  servicio real bastaría quitar `endpoint`, `access-key` y `secret-key` de la configuración
  `dynamodb.*` y crear la tabla fuera de la aplicación.
- **Periodo fiscal y detalle del ERP.** El evento de pago no trae el mes que se está pagando ni
  conceptos como mora o reconexión. El módulo usa el año y mes de la fecha del pago como periodo
  fiscal y una sola línea `AGUA_POTABLE` por el total del pago.
- **Pagos en dead-letter.** No se reintentan solos: quedan registrados y requieren revisión manual.
  Reencolarlos es tan simple como `UPDATE pagos SET enqueued = 0 WHERE ...`, y es seguro gracias a la
  idempotencia.
- **Microservicio fuera de Docker.** `docker-compose.yml` levanta la infraestructura; el
  microservicio se ejecuta con Maven.
- **Verificación manual.** La comprobación del módulo se hace con el flujo de la sección 3.4; el
  proyecto no incluye pruebas automatizadas.
