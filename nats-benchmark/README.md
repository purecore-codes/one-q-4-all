# 🚀 Benchmark de Stress Test - NATS Servers

Este pacote contém ferramentas de benchmark de alta performance para testar os servidores NATS implementados em Zig e Jai.

## 📋 Configurações do Stress Test

O benchmark foi configurado para um teste **extremamente pesado**:

| Parâmetro | Valor | Descrição |
|-----------|-------|-----------|
| **Duração** | 120s | 2 minutos de teste contínuo |
| **Clientes** | 500 | 250 NATS + 250 gRPC concorrentes |
| **Mensagens/Cliente** | 10,000 | Até 10k mensagens por cliente |
| **Payload** | 1,024 bytes | 1KB por mensagem |
| **Total Teórico** | 5,000,000 | 5 milhões de mensagens potenciais |

## 🔧 Compilação

### Zig Benchmark

```bash
cd /workspace/nats-benchmark

# Compilar em modo release (otimizado)
zig build -Doptimize=ReleaseFast

# Compilar em modo debug
zig build

# Executar diretamente
zig build run
```

### Jai Benchmark

```bash
cd /workspace/nats-benchmark

# Compilar
jai benchmark.jai -out:benchmark_jai

# Executar
./benchmark_jai
```

## 🎯 Como Executar o Teste

### Pré-requisitos

1. **Inicie o servidor Zig** (terminal 1):
```bash
cd /workspace/nats-zig
zig build run
```

2. **OU inicie o servidor Jai** (terminal 1):
```bash
cd /workspace/nats-jai
jai main.jai
```

3. **Execute o benchmark** (terminal 2):
```bash
cd /workspace/nats-benchmark
zig build run
# ou
./benchmark_jai
```

## 📊 Métricas Monitoradas

O benchmark monitora em tempo real:

- **Throughput**: Mensagens por segundo (NATS e gRPC separadamente)
- **Latência implícita**: Tempo de round-trip PING/PONG
- **Erros**: Conexões falhadas, timeouts, erros de protocolo
- **Volume total**: Dados transferidos em MB
- **Concorrência**: 500 threads simultâneas

## 📈 Saída Esperada

Durante a execução, você verá um painel em tempo real:

```
[045s] NATS:   125430 msgs (  2789/s) | gRPC:   118234 msgs (  2627/s) | Erros: 12 | Total: 243664
```

Ao final, um relatório detalhado:

```
╔══════════════════════════════════════════════════════════╗
║                  RESULTADOS FINAIS                     ║
╠══════════════════════════════════════════════════════════╣
║ Tempo Total:              120.05 segundos                ║
╟──────────────────────────────────────────────────────────╢
║ PROTOCOLO NATS (TCP)                                     ║
║   Enviadas:            352847 mensagens                   ║
║   Recebidas:           352835 mensagens                   ║
║   Erros:                   12                              ║
║   Throughput:           2939 msgs/s                    ║
╟──────────────────────────────────────────────────────────╢
║ PROTOCOLO gRPC                                           ║
║   Enviadas:            341256 mensagens                   ║
║   Recebidas:           341244 mensagens                   ║
║   Erros:                   15                              ║
║   Throughput:           2842 msgs/s                    ║
╟──────────────────────────────────────────────────────────╢
║ TOTAIS GERAIS                                            ║
║   Total Mensagens:       694103                              ║
║   Total Erros:             27                              ║
║   Throughput Total:     5781 msgs/s                    ║
║   Dados Transferidos:    678.03 MB                        ║
╚══════════════════════════════════════════════════════════╝
```

## ⚙️ Customização

Edite as constantes no arquivo `benchmark.zig` ou `benchmark.jai`:

```zig
const CONFIG = struct {
    pub const duration_seconds: u32 = 120;     // Duração do teste
    pub const num_clients: usize = 500;        // Número de clientes
    pub const messages_per_client: usize = 10000;
    pub const payload_size: usize = 1024;      // Tamanho da mensagem
    pub const natss_port: u16 = 4222;          // Porta NATS
    pub const grpc_port: u16 = 50051;          // Porta gRPC
};
```

## 🔥 Dicas para Stress Test Extremo

1. **Aumente clientes**: Para sistemas robustos, aumente para 1000+ clientes
2. **Payload maior**: Teste com 4KB, 16KB ou 64KB para stress de rede
3. **Sem limite de mensagens**: Remova o limite `messages_per_client` para teste infinito
4. **Monitore recursos**: Use `htop`, `iftop` para monitorar CPU e rede

## ⚠️ Avisos

- Este benchmark é **intencionalmente agressivo**
- Pode consumir toda a CPU disponível
- Pode saturar a rede localhost
- Execute em ambiente controlado
- Servidores podem travar sob carga extrema (isso é esperado!)

## 📝 Comparação Zig vs Jai

Execute o benchmark contra ambos os servidores para comparar:

| Métrica | Servidor Zig | Servidor Jai |
|---------|--------------|--------------|
| Throughput NATS | ? msgs/s | ? msgs/s |
| Throughput gRPC | ? msgs/s | ? msgs/s |
| Estabilidade | ? erros | ? erros |
| Uso de CPU | ? % | ? % |

## 🎯 Objetivos do Teste

1. ✅ Medir throughput máximo sustentável
2. ✅ Identificar gargalos de concorrência
3. ✅ Testar estabilidade sob carga prolongada
4. ✅ Comparar eficiência Zig vs Jai
5. ✅ Validar implementação dos protocolos

Boa sorte com o stress test! 🚀
