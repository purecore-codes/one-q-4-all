#!/bin/bash

# Script de automação para Stress Test dos Servidores NATS
# Suporte para Zig e Jai

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${BLUE}"
    echo "╔══════════════════════════════════════════════════════════╗"
    echo "║     STRESS TEST - NATS SERVERS (Zig & Jai)              ║"
    echo "╚══════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

print_section() {
    echo -e "\n${YELLOW}▶ $1${NC}\n"
}

check_zig() {
    if command -v zig &> /dev/null; then
        echo -e "${GREEN}✓ Zig instalado${NC}"
        return 0
    else
        echo -e "${RED}✗ Zig não encontrado${NC}"
        return 1
    fi
}

check_jai() {
    if command -v jai &> /dev/null; then
        echo -e "${GREEN}✓ Jai instalado${NC}"
        return 0
    else
        echo -e "${RED}✗ Jai não encontrado${NC}"
        return 1
    fi
}

build_zig_server() {
    print_section "Compilando servidor NATS em Zig..."
    cd /workspace/nats-zig
    zig build -Doptimize=ReleaseFast
    echo -e "${GREEN}✓ Servidor Zig compilado com sucesso${NC}"
}

build_jai_server() {
    print_section "Compilando servidor NATS em Jai..."
    cd /workspace/nats-jai/src
    jai main.jai -out:../main
    echo -e "${GREEN}✓ Servidor Jai compilado com sucesso${NC}"
}

build_zig_benchmark() {
    print_section "Compilando benchmark em Zig..."
    cd /workspace/nats-benchmark
    zig build -Doptimize=ReleaseFast
    echo -e "${GREEN}✓ Benchmark Zig compilado com sucesso${NC}"
}

build_jai_benchmark() {
    print_section "Compilando benchmark em Jai..."
    cd /workspace/nats-benchmark
    jai benchmark.jai -out:benchmark_jai
    echo -e "${GREEN}✓ Benchmark Jai compilado com sucesso${NC}"
}

run_zig_server() {
    print_section "Iniciando servidor NATS em Zig..."
    echo -e "${YELLOW}Portas: NATS (4222) + gRPC (50051)${NC}"
    cd /workspace/nats-zig
    ./zig-out/bin/nats-zig
}

run_jai_server() {
    print_section "Iniciando servidor NATS em Jai..."
    echo -e "${YELLOW}Portas: NATS (4222) + gRPC (50051)${NC}"
    cd /workspace/nats-jai
    ./main
}

run_zig_benchmark() {
    print_section "Executando benchmark em Zig..."
    echo -e "${YELLOW}Configuração: 500 clientes, 2 minutos, 1KB payload${NC}"
    cd /workspace/nats-benchmark
    ./zig-out/bin/nats-benchmark
}

run_jai_benchmark() {
    print_section "Executando benchmark em Jai..."
    echo -e "${YELLOW}Configuração: 500 clientes, 2 minutos, 1KB payload${NC}"
    cd /workspace/nats-benchmark
    ./benchmark_jai
}

show_status() {
    print_section "Status dos Servidores"
    
    echo "Verificando porta NATS (4222)..."
    if nc -z localhost 4222 2>/dev/null; then
        echo -e "${GREEN}✓ Porta 4222 (NATS) está aberta${NC}"
    else
        echo -e "${RED}✗ Porta 4222 (NATS) está fechada${NC}"
    fi
    
    echo "Verificando porta gRPC (50051)..."
    if nc -z localhost 50051 2>/dev/null; then
        echo -e "${GREEN}✓ Porta 50051 (gRPC) está aberta${NC}"
    else
        echo -e "${RED}✗ Porta 50051 (gRPC) está fechada${NC}"
    fi
}

cleanup() {
    print_section "Limpando processos..."
    pkill -f "nats-zig" 2>/dev/null || true
    pkill -f "nats-jai" 2>/dev/null || true
    pkill -f "nats-benchmark" 2>/dev/null || true
    pkill -f "benchmark_jai" 2>/dev/null || true
    echo -e "${GREEN}✓ Processos finalizados${NC}"
}

show_help() {
    echo "Uso: $0 [comando]"
    echo ""
    echo "Comandos disponíveis:"
    echo "  build-all          Compila todos os servidores e benchmarks"
    echo "  build-zig-server   Compila apenas o servidor Zig"
    echo "  build-jai-server   Compila apenas o servidor Jai"
    echo "  build-benchmarks   Compila ambos os benchmarks"
    echo "  run-zig-server     Executa o servidor Zig"
    echo "  run-jai-server     Executa o servidor Jai"
    echo "  run-zig-bench      Executa o benchmark Zig"
    echo "  run-jai-bench      Executa o benchmark Jai"
    echo "  status             Verifica status das portas"
    echo "  cleanup            Mata todos os processos"
    echo "  help               Mostra esta ajuda"
    echo ""
    echo "Exemplo de uso:"
    echo "  1. Terminal 1: $0 run-zig-server"
    echo "  2. Terminal 2: $0 run-zig-bench"
}

# Main
print_header

case "${1:-help}" in
    build-all)
        check_zig && build_zig_server
        check_jai && build_jai_server
        check_zig && build_zig_benchmark
        check_jai && build_jai_benchmark
        ;;
    build-zig-server)
        check_zig && build_zig_server
        ;;
    build-jai-server)
        check_jai && build_jai_server
        ;;
    build-benchmarks)
        check_zig && build_zig_benchmark
        check_jai && build_jai_benchmark
        ;;
    run-zig-server)
        run_zig_server
        ;;
    run-jai-server)
        run_jai_server
        ;;
    run-zig-bench)
        run_zig_benchmark
        ;;
    run-jai-bench)
        run_jai_benchmark
        ;;
    status)
        show_status
        ;;
    cleanup)
        cleanup
        ;;
    help|*)
        show_help
        ;;
esac
