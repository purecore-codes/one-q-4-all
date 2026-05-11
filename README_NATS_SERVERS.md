# NATS Server em Zig

Implementação de um servidor NATS utilizando inteiramente a linguagem Zig, com suporte tanto para o protocolo nativo do NATS (texto sobre TCP) quanto para gRPC.

## Estrutura do Projeto

```
nats-zig/
├── build.zig          # Configuração de build do Zig
└── src/
    └── main.zig       # Implementação principal
```

## Recursos Implementados

### Protocolo NATS (TCP)
- Parser do protocolo de texto NATS
- Suporte aos comandos:
  - `PING` / `PONG`
  - `CONNECT`
  - `PUB` / `SUB`
  - `MSG`
  - `INFO`
  - `+OK` / `-ERR`
- Servidor TCP multi-cliente
- Broadcast de mensagens para subscribers

### Protocolo gRPC (TCP)
- Parser de frames gRPC
- Suporte a mensagens comprimidas e não-comprimidas
- Registro de serviços
- Processamento de chamadas RPC

## Como Compilar

```bash
cd nats-zig
zig build
```

## Como Executar

```bash
zig build run
```

O servidor irá iniciar:
- Servidor NATS na porta 4222
- Servidor gRPC na porta 50051

## Arquitetura

### NatsParser
Responsável por parsear o protocolo de texto do NATS, mantendo um buffer interno para reconstruir mensagens fragmentadas pelo TCP.

### GrpcParser
Parseia frames gRPC no formato:
- 1 byte: flag de compressão
- 4 bytes: tamanho da mensagem (big-endian)
- N bytes: payload da mensagem

### NatsServer
Servidor TCP que gerencia múltiplas conexões de clientes, mantendo estado de subscrições e fazendo broadcast de mensagens.

### GrpcServer
Servidor TCP para comunicação gRPC, com registro dinâmico de handlers de serviço.

---

# NATS Server em Jai

Implementação de um servidor NATS utilizando inteiramente a linguagem Jai, com suporte tanto para o protocolo nativo do NATS (texto sobre TCP) quanto para gRPC.

## Estrutura do Projeto

```
nats-jai/
└── src/
    └── main.jai       # Implementação principal
```

## Recursos Implementados

### Protocolo NATS (TCP)
- Parser do protocolo de texto NATS
- Suporte aos comandos:
  - `PING` / `PONG`
  - `CONNECT`
  - `PUB` / `SUB`
  - `MSG`
  - `INFO`
  - `+OK` / `-ERR`
- Servidor TCP multi-cliente
- Broadcast de mensagens para subscribers

### Protocolo gRPC (TCP)
- Parser de frames gRPC
- Suporte a mensagens comprimidas e não-comprimidas
- Registro de serviços
- Processamento de chamadas RPC

## Como Compilar

```bash
cd nats-jai
jai src/main.jai
```

## Como Executar

```bash
./main
```

O servidor irá iniciar:
- Servidor NATS na porta 4222
- Servidor gRPC na porta 50051

## Arquitetura

### Nats_Parser
Responsável por parsear o protocolo de texto do NATS, mantendo um buffer interno para reconstruir mensagens fragmentadas pelo TCP.

### Grpc_Parser
Parseia frames gRPC no formato:
- 1 byte: flag de compressão
- 4 bytes: tamanho da mensagem (big-endian)
- N bytes: payload da mensagem

### Nats_Server
Servidor TCP que gerencia múltiplas conexões de clientes, mantendo estado de subscrições e fazendo broadcast de mensagens.

### Grpc_Server
Servidor TCP para comunicação gRPC, com registro dinâmico de handlers de serviço.

## Diferenças entre as Implementações

### Zig
- Gerenciamento de memória explícito com allocators
- Sistema de tipos com unions taggadas
- Error handling com error sets
- Compilação para binário nativo otimizado

### Jai
- Sintaxe mais concisa e expressiva
- Arrays estáticos com bounds checking
- Procedimentos como cidadãos de primeira classe
- Foco em performance e simplicidade

## Protocolo NATS

O protocolo NATS é um protocolo de texto simples sobre TCP:

```
CONNECT {"verbose":false,"pedantic":false}
SUB subject.sid
PUB subject size
payload
PING
PONG
```

## Protocolo gRPC

O gRPC utiliza HTTP/2 sobre TCP, mas nesta implementação simplificamos para usar TCP direto com framing:

```
[compressed: 1 byte][length: 4 bytes big-endian][message: N bytes]
```

## Notas

Ambas as implementações seguem a mesma especificação funcional, demonstrando como o mesmo protocolo pode ser implementado em diferentes linguagens de programação sistemas modernas.
