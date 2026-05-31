// Jai Stress Test para NATS Servers
// Compile: jai benchmark.jai -out:benchmark_jai.exe
// Run: ./benchmark_jai.exe

import "core"

CONFIG :: struct {
    duration_seconds      : int = 120,        // 2 minutos
    num_clients           : int = 500,        // 500 clientes concorrentes
    messages_per_client   : int = 10000,      // 10k msgs por cliente
    payload_size          : int = 1024,       // 1KB por mensagem
    nats_port             : u16 = 4222,
    grpc_port             : u16 = 50051,
    host                  : string = "127.0.0.1",
}

// Estatísticas globais (thread-safe via atomics em produção)
nats_tcp_sent      : atomic_int;
nats_tcp_recv      : atomic_int;
nats_tcp_errors    : atomic_int;
grpc_sent          : atomic_int;
grpc_recv          : atomic_int;
grpc_errors        : atomic_int;
stop_flag          : bool;
start_time         : i64;

Client_Context :: struct {
    client_id : int,
    payload   : []u8,
}

// Cliente NATS TCP
nats_tcp_client :: proc(ctx : Client_Context) {
    addr := net.make_address_ipv4(CONFIG.host, CONFIG.nats_port);
    
    for !stop_flag {
        socket, ok := net.tcp_connect(addr);
        if !ok {
            atomic_add(&nats_tcp_errors, 1);
            continue;
        }
        
        reader := socket.get_reader();
        writer := socket.get_writer();
        
        buffer : [256]u8;
        
        // Loop de mensagens
        for i := 0; i < CONFIG.messages_per_client && !stop_flag; i += 1 {
            // SUBSCRIBE
            sub_cmd := fmt.sprintf(buffer[:], "SUB bench.test.%d bench\r\n", ctx.client_id);
            if !writer.write_all(sub_cmd) {
                atomic_add(&nats_tcp_errors, 1);
                break;
            }
            
            // PUBLISH
            pub_cmd := fmt.sprintf(buffer[:], "PUB bench.test.%d %d\r\n%s\r\n", 
                                   ctx.client_id, len(ctx.payload), ctx.payload);
            if !writer.write_all(pub_cmd) {
                atomic_add(&nats_tcp_errors, 1);
                break;
            }
            atomic_add(&nats_tcp_sent, 1);
            
            // PING/PONG
            if !writer.write_all("PING\r\n") {
                atomic_add(&nats_tcp_errors, 1);
                break;
            }
            
            pong, ok := reader.read_until_delimiter('\n');
            if ok && pong != nil {
                atomic_add(&nats_tcp_recv, 1);
            }
        }
        
        socket.close();
    }
}

// Cliente gRPC
grpc_client :: proc(ctx : Client_Context) {
    addr := net.make_address_ipv4(CONFIG.host, CONFIG.grpc_port);
    
    for !stop_flag {
        socket, ok := net.tcp_connect(addr);
        if !ok {
            atomic_add(&grpc_errors, 1);
            continue;
        }
        
        reader := socket.get_reader();
        writer := socket.get_writer();
        
        buffer : [4096]u8;
        
        // gRPC Frame: Flags (1) + Length (4) + Message
        frame_buffer : [5 + CONFIG.payload_size]u8;
        frame_buffer[0] = 0; // No compression
        mem.write_be_u32(frame_buffer[1:5], cast u32 len(ctx.payload));
        mem.copy(frame_buffer[5:], ctx.payload);
        
        for i := 0; i < CONFIG.messages_per_client && !stop_flag; i += 1 {
            // Send gRPC frame
            if !writer.write_all(frame_buffer[:5+len(ctx.payload)]) {
                atomic_add(&grpc_errors, 1);
                break;
            }
            atomic_add(&grpc_sent, 1);
            
            // Read response
            response, ok := reader.read(buffer[:]);
            if ok && response > 0 {
                atomic_add(&grpc_recv, 1);
            }
        }
        
        socket.close();
    }
}

// Thread de monitoramento
monitor_thread :: proc() {
    last_nats_sent : int = 0;
    last_grpc_sent : int = 0;
    last_time      : i64 = time.now_millis();
    
    stdout := io.stdout();
    
    for !stop_flag {
        time.sleep(1000 * time.MILLISECOND);
        
        now := time.now_millis();
        elapsed : f64 = cast f64 (now - last_time) / 1000.0;
        
        nats_sent := atomic_load(&nats_tcp_sent);
        grpc_sent := atomic_load(&grpc_sent);
        nats_err  := atomic_load(&nats_tcp_errors);
        grpc_err  := atomic_load(&grpc_errors);
        
        nats_rate : f64 = cast f64 (nats_sent - last_nats_sent) / elapsed;
        grpc_rate : f64 = cast f64 (grpc_sent - last_grpc_sent) / elapsed;
        
        total_sent := nats_sent + grpc_sent;
        total_err  := nats_err + grpc_err;
        runtime    : f64 = cast f64 (now - start_time) / 1000.0;
        
        stdout.printf("\r[%03.0fs] NATS: %8d msgs (%6.0f/s) | gRPC: %8d msgs (%6.0f/s) | Erros: %d | Total: %d",
                      runtime, nats_sent, nats_rate, grpc_sent, grpc_rate, total_err, total_sent);
        
        last_nats_sent = nats_sent;
        last_grpc_sent = grpc_sent;
        last_time = now;
    }
}

main :: proc() {
    stdout := io.stdout();
    
    stdout.println("\n🚀 INICIANDO STRESS TEST MASSIVO - NATS SERVERS (JAI)");
    stdout.println("================================================");
    stdout.printf("⏱️  Duração: %d segundos\n", CONFIG.duration_seconds);
    stdout.printf("👥 Clientes: %d (metade NATS, metade gRPC)\n", CONFIG.num_clients);
    stdout.printf("📦 Mensagens/cliente: %d\n", CONFIG.messages_per_client);
    stdout.printf("📏 Payload: %d bytes\n\n", CONFIG.payload_size);
    
    // Gerar payload aleatório
    payload := make([]u8, CONFIG.payload_size);
    defer delete(payload);
    
    rand.init(cast u32 time.now());
    for i := 0; i < len(payload); i += 1 {
        payload[i] = cast u8 (rand.int() % 256);
    }
    
    start_time = time.now_millis();
    stop_flag = false;
    
    stdout.printf("🔥 Iniciando %d threads...\n\n", CONFIG.num_clients);
    
    threads : [CONFIG.num_clients]Thread_Handle;
    
    // Launch clients
    for i := 0; i < CONFIG.num_clients; i += 1 {
        ctx := Client_Context{
            client_id = i,
            payload   = payload,
        };
        
        if i % 2 == 0 {
            threads[i] = thread_start(nats_tcp_client, ctx);
        } else {
            threads[i] = thread_start(grpc_client, ctx);
        }
    }
    
    // Start monitor
    monitor := thread_start(monitor_thread);
    
    // Aguardar duração
    time.sleep(CONFIG.duration_seconds * time.SECOND);
    
    // Parar todos os clientes
    stop_flag = true;
    
    stdout.println("\n\n⏳ Aguardando finalização das threads...");
    
    // Join threads
    for i := 0; i < len(threads); i += 1 {
        thread_join(threads[i]);
    }
    thread_join(monitor);
    
    end_time := time.now_millis();
    total_time : f64 = cast f64 (end_time - start_time) / 1000.0;
    
    // Resultados finais
    nats_total := atomic_load(&nats_tcp_sent);
    grpc_total := atomic_load(&grpc_sent);
    nats_recv  := atomic_load(&nats_tcp_recv);
    grpc_recv  := atomic_load(&grpc_recv);
    nats_err   := atomic_load(&nats_tcp_errors);
    grpc_err   := atomic_load(&grpc_errors);
    
    total_msgs := nats_total + grpc_total;
    total_err  := nats_err + grpc_err;
    
    stdout.println("\n\n╔══════════════════════════════════════════════════════════╗");
    stdout.println("║                  RESULTADOS FINAIS                     ║");
    stdout.println("╠══════════════════════════════════════════════════════════╣");
    stdout.printf("║ Tempo Total:           %10.2f segundos                ║\n", total_time);
    stdout.println("╟──────────────────────────────────────────────────────────╢");
    stdout.println("║ PROTOCOLO NATS (TCP)                                     ║");
    stdout.printf("║   Enviadas:          %12d mensagens                   ║\n", nats_total);
    stdout.printf("║   Recebidas:         %12d mensagens                   ║\n", nats_recv);
    stdout.printf("║   Erros:             %12d                              ║\n", nats_err);
    stdout.printf("║   Throughput:        %10.0f msgs/s                    ║\n", cast f64 nats_total / total_time);
    stdout.println("╟──────────────────────────────────────────────────────────╢");
    stdout.println("║ PROTOCOLO gRPC                                           ║");
    stdout.printf("║   Enviadas:          %12d mensagens                   ║\n", grpc_total);
    stdout.printf("║   Recebidas:         %12d mensagens                   ║\n", grpc_recv);
    stdout.printf("║   Erros:             %12d                              ║\n", grpc_err);
    stdout.printf("║   Throughput:        %10.0f msgs/s                    ║\n", cast f64 grpc_total / total_time);
    stdout.println("╟──────────────────────────────────────────────────────────╢");
    stdout.println("║ TOTAIS GERAIS                                            ║");
    stdout.printf("║   Total Mensagens:   %12d                              ║\n", total_msgs);
    stdout.printf("║   Total Erros:       %12d                              ║\n", total_err);
    stdout.printf("║   Throughput Total:  %10.0f msgs/s                    ║\n", cast f64 total_msgs / total_time);
    stdout.printf("║   Dados Transferidos:%10.2f MB                        ║\n", 
                  cast f64 (total_msgs * CONFIG.payload_size) / (1024.0 * 1024.0));
    stdout.println("╚══════════════════════════════════════════════════════════╝\n");
}
