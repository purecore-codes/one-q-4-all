const std = @import("std");
const net = std.net;
const time = std.time;
const thread = std.Thread;
const atomic = std.atomic;

// Configurações de Stress Test
const CONFIG = struct {
    pub const duration_seconds: u32 = 120; // 2 minutos
    pub const num_clients: usize = 500;    // 500 clientes concorrentes
    pub const messages_per_client: usize = 10000; // 10k msgs por cliente
    pub const payload_size: usize = 1024;  // 1KB por mensagem
    pub const natss_port: u16 = 4222;
    pub const grpc_port: u16 = 50051;
    pub const host: []const u8 = "127.0.0.1";
};

// Estatísticas atômicas
var stats = struct {
    nats_tcp_sent: atomic.Value(u64) = atomic.Value(u64).init(0),
    nats_tcp_recv: atomic.Value(u64) = atomic.Value(u64).init(0),
    nats_tcp_errors: atomic.Value(u64) = atomic.Value(u64).init(0),
    grpc_sent: atomic.Value(u64) = atomic.Value(u64).init(0),
    grpc_recv: atomic.Value(u64) = atomic.Value(u64).init(0),
    grpc_errors: atomic.Value(u64) = atomic.Value(u64).init(0),
    start_time: atomic.Value(i64) = atomic.Value(i64).init(0),
    stop: atomic.Value(bool) = atomic.Value(bool).init(false),
}{};

const ClientContext = struct {
    client_id: usize,
    payload: []const u8,
    allocator: std.mem.Allocator,
};

// Cliente NATS TCP
fn natsTcpClient(ctx: ClientContext) void {
    const addr = net.Address.parseIp4(CONFIG.host, CONFIG.natss_port) catch {
        _ = stats.nats_tcp_errors.fetchAdd(1, .seq_cst);
        return;
    };

    while (!stats.stop.load(.seq_cst)) {
        const socket = net.tcpConnectToAddress(addr) catch {
            _ = stats.nats_tcp_errors.fetchAdd(1, .seq_cst);
            continue;
        };
        defer socket.close();

        var reader = socket.reader();
        var writer = socket.writer();
        
        var buffer: [256]u8 = undefined;
        
        // Loop de mensagens
        var i: usize = 0;
        while (i < CONFIG.messages_per_client and !stats.stop.load(.seq_cst)) : (i += 1) {
            // SUBSCRIBE
            const sub_cmd = std.fmt.bufPrint(&buffer, "SUB bench.test.{d} bench\r\n", .{ctx.client_id}) catch continue;
            writer.writeAll(sub_cmd) catch {
                _ = stats.nats_tcp_errors.fetchAdd(1, .seq_cst);
                break;
            };

            // PUBLISH
            const pub_cmd = std.fmt.bufPrint(&buffer, "PUB bench.test.{d} {d}\r\n{s}\r\n", .{ ctx.client_id, ctx.payload.len, ctx.payload }) catch continue;
            writer.writeAll(pub_cmd) catch {
                _ = stats.nats_tcp_errors.fetchAdd(1, .seq_cst);
                break;
            };
            _ = stats.nats_tcp_sent.fetchAdd(1, .seq_cst);

            // PING/PONG
            writer.writeAll("PING\r\n") catch {
                _ = stats.nats_tcp_errors.fetchAdd(1, .seq_cst);
                break;
            };
            
            const pong = reader.readUntilDelimiterOrEof(&buffer, '\n') catch {
                _ = stats.nats_tcp_errors.fetchAdd(1, .seq_cst);
                break;
            };
            if (pong != null) {
                _ = stats.nats_tcp_recv.fetchAdd(1, .seq_cst);
            }
        }
    }
}

// Cliente gRPC
fn grpcClient(ctx: ClientContext) void {
    const addr = net.Address.parseIp4(CONFIG.host, CONFIG.grpc_port) catch {
        _ = stats.grpc_errors.fetchAdd(1, .seq_cst);
        return;
    };

    while (!stats.stop.load(.seq_cst)) {
        const socket = net.tcpConnectToAddress(addr) catch {
            _ = stats.grpc_errors.fetchAdd(1, .seq_cst);
            continue;
        };
        defer socket.close();

        var reader = socket.reader();
        var writer = socket.writer();
        
        var buffer: [4096]u8 = undefined;
        
        // gRPC Frame: Flags (1) + Length (4) + Message
        var frame_buffer: [5 + CONFIG.payload_size]u8 = undefined;
        frame_buffer[0] = 0; // No compression
        std.mem.writeInt(u32, frame_buffer[1..5], @intCast(ctx.payload.len), .big);
        @memcpy(frame_buffer[5..5 + ctx.payload.len], ctx.payload);

        var i: usize = 0;
        while (i < CONFIG.messages_per_client and !stats.stop.load(.seq_cst)) : (i += 1) {
            // Send gRPC frame
            writer.writeAll(frame_buffer[0..5 + ctx.payload.len]) catch {
                _ = stats.grpc_errors.fetchAdd(1, .seq_cst);
                break;
            };
            _ = stats.grpc_sent.fetchAdd(1, .seq_cst);

            // Read response (simulated)
            const response = reader.read(&buffer) catch {
                _ = stats.grpc_errors.fetchAdd(1, .seq_cst);
                break;
            };
            if (response > 0) {
                _ = stats.grpc_recv.fetchAdd(1, .seq_cst);
            }
        }
    }
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const stdout = std.io.getStdOut().writer();
    
    try stdout.print("\n🚀 INICIANDO STRESS TEST MASSIVO - NATS SERVERS\n", .{});
    try stdout.print("================================================\n", .{});
    try stdout.print("⏱️  Duração: {d} segundos\n", .{CONFIG.duration_seconds});
    try stdout.print("👥 Clientes: {d} (metade NATS, metade gRPC)\n", .{CONFIG.num_clients});
    try stdout.print("📦 Mensagens/cliente: {d}\n", .{CONFIG.messages_per_client});
    try stdout.print("📏 Payload: {d} bytes\n\n", .{CONFIG.payload_size});

    // Gerar payload aleatório
    var payload = try allocator.alloc(u8, CONFIG.payload_size);
    defer allocator.free(payload);
    var prng = std.rand.DefaultPrng.init(@intCast(time.timestamp()));
    prng.fill(payload);

    stats.start_time.store(time.timestamp(), .seq_cst);

    var threads: [CONFIG.num_clients]thread.Handle(void) = undefined;
    
    try stdout.print("🔥 Iniciando {d} threads...\n\n", .{CONFIG.num_clients});
    
    const start = time.milliTimestamp();
    
    // Launch clients
    for (0..CONFIG.num_clients) |i| {
        const ctx = ClientContext{
            .client_id = i,
            .payload = payload,
            .allocator = allocator,
        };
        
        if (i % 2 == 0) {
            threads[i] = try thread.spawn(.{}, natsTcpClient, .{ctx});
        } else {
            threads[i] = try thread.spawn(.{}, grpcClient, .{ctx});
        }
    }

    // Monitoramento em tempo real
    const monitor_thread = try thread.spawn(.{}, struct {
        fn run() void {
            const stdout_mon = std.io.getStdOut().writer();
            var last_nats_sent: u64 = 0;
            var last_grpc_sent: u64 = 0;
            var last_time = time.milliTimestamp();
            
            while (!stats.stop.load(.seq_cst)) {
                thread.sleep(1000 * time.ms_per_s); // 1 segundo
                
                const now = time.milliTimestamp();
                const elapsed = @as(f64, @floatFromInt(now - last_time)) / 1000.0;
                
                const nats_sent = stats.nats_tcp_sent.load(.seq_cst);
                const grpc_sent = stats.grpc_sent.load(.seq_cst);
                const nats_err = stats.nats_tcp_errors.load(.seq_cst);
                const grpc_err = stats.grpc_errors.load(.seq_cst);
                
                const nats_rate = @as(f64, @floatFromInt(nats_sent - last_nats_sent)) / elapsed;
                const grpc_rate = @as(f64, @floatFromInt(grpc_sent - last_grpc_sent)) / elapsed;
                
                const total_sent = nats_sent + grpc_sent;
                const total_err = nats_err + grpc_err;
                const runtime = @as(f64, @floatFromInt(now - start)) / 1000.0;
                
                stdout_mon.print("\r[{:0>3.0}s] NATS: {:>8} msgs ({:>6.0}/s) | gRPC: {:>8} msgs ({:>6.0}/s) | Erros: {} | Total: {}", .{
                    runtime,
                    nats_sent,
                    nats_rate,
                    grpc_sent,
                    grpc_rate,
                    total_err,
                    total_sent,
                }) catch {};
                
                last_nats_sent = nats_sent;
                last_grpc_sent = grpc_sent;
                last_time = now;
            }
        }
    }.run, .{});

    // Aguardar duração
    thread.sleep(CONFIG.duration_seconds * time.ms_per_s);
    
    // Parar todos os clientes
    stats.stop.store(true, .seq_cst);
    
    try stdout.print("\n\n⏳ Aguardando finalização das threads...\n", .{});
    
    // Join threads
    for (threads) |t| {
        t.join();
    }
    monitor_thread.join();
    
    const end = time.milliTimestamp();
    const total_time = @as(f64, @floatFromInt(end - start)) / 1000.0;
    
    // Resultados finais
    const nats_total = stats.nats_tcp_sent.load(.seq_cst);
    const grpc_total = stats.grpc_sent.load(.seq_cst);
    const nats_recv = stats.nats_tcp_recv.load(.seq_cst);
    const grpc_recv = stats.grpc_recv.load(.seq_cst);
    const nats_err = stats.nats_tcp_errors.load(.seq_cst);
    const grpc_err = stats.grpc_errors.load(.seq_cst);
    
    const total_msgs = nats_total + grpc_total;
    const total_err = nats_err + grpc_err;
    
    try stdout.print("\n\n╔══════════════════════════════════════════════════════════╗\n", .{});
    try stdout.print("║                  RESULTADOS FINAIS                     ║\n", .{});
    try stdout.print("╠══════════════════════════════════════════════════════════╣\n", .{});
    try stdout.print("║ Tempo Total:           {:>10.2f} segundos                ║\n", .{total_time});
    try stdout.print("╟──────────────────────────────────────────────────────────╢\n", .{});
    try stdout.print("║ PROTOCOLO NATS (TCP)                                     ║\n", .{});
    try stdout.print("║   Enviadas:          {:>12} mensagens                   ║\n", .{nats_total});
    try stdout.print("║   Recebidas:         {:>12} mensagens                   ║\n", .{nats_recv});
    try stdout.print("║   Erros:             {:>12}                              ║\n", .{nats_err});
    try stdout.print("║   Throughput:        {:>10.0f} msgs/s                    ║\n", .{@as(f64, @floatFromInt(nats_total)) / total_time});
    try stdout.print("╟──────────────────────────────────────────────────────────╢\n", .{});
    try stdout.print("║ PROTOCOLO gRPC                                           ║\n", .{});
    try stdout.print("║   Enviadas:          {:>12} mensagens                   ║\n", .{grpc_total});
    try stdout.print("║   Recebidas:         {:>12} mensagens                   ║\n", .{grpc_recv});
    try stdout.print("║   Erros:             {:>12}                              ║\n", .{grpc_err});
    try stdout.print("║   Throughput:        {:>10.0f} msgs/s                    ║\n", .{@as(f64, @floatFromInt(grpc_total)) / total_time});
    try stdout.print("╟──────────────────────────────────────────────────────────╢\n", .{});
    try stdout.print("║ TOTAIS GERAIS                                            ║\n", .{});
    try stdout.print("║   Total Mensagens:   {:>12}                              ║\n", .{total_msgs});
    try stdout.print("║   Total Erros:       {:>12}                              ║\n", .{total_err});
    try stdout.print("║   Throughput Total:  {:>10.0f} msgs/s                    ║\n", .{@as(f64, @floatFromInt(total_msgs)) / total_time});
    try stdout.print("║   Dados Transferidos:{:>10.2f} MB                        ║\n", .{@as(f64, @floatFromInt(total_msgs * CONFIG.payload_size)) / (1024.0 * 1024.0)});
    try stdout.print("╚══════════════════════════════════════════════════════════╝\n\n", .{});
}
