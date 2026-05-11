// NATS Server implementation in Jai
// Implements both NATS text protocol and gRPC over TCP

#load "package:core"
#load "package:net"

using core :: import("core");
using net :: import("net");

// ============================================================================
// NATS Protocol Types
// ============================================================================

Nats_Message :: struct {
    subject: string;
    sid: string;
    reply_to: ?string;
    size: int;
    payload: string;
}

Nats_Control :: union {
    ping: void;
    pong: void;
    ok: void;
    err: string;
    info: string;
}

// ============================================================================
// NATS Parser - Text Protocol Implementation
// ============================================================================

Nats_Parser :: struct {
    buffer: []u8;
    allocator: core.Allocator;
}

nats_parser_init :: (allocator: core.Allocator) -> Nats_Parser {
    return Nats_Parser {
        buffer = allocator.alloc(u8, 4096),
        allocator = allocator,
    };
}

nats_parser_deinit :: (self: *Nats_Parser) {
    self.allocator.free(self.buffer);
}

nats_parser_append :: (self: *Nats_Parser, data: []u8) -> bool {
    // Simple append - in production would need to handle buffer growth
    if len(data) > len(self.buffer) {
        return false;
    }
    copy(self.buffer[0:len(data)], data);
    return true;
}

nats_parser_process :: (self: *Nats_Parser) -> struct {
    messages: [][128]Nats_Message;
    controls: [][128]Nats_Control;
    message_count: int;
    control_count: int;
} {
    messages: [][128]Nats_Message;
    controls: [][128]Nats_Control;
    message_count: int = 0;
    control_count: int = 0;

    buffer_len := len(self.buffer);
    pos: int = 0;

    while pos < buffer_len {
        // Find line ending (\r\n)
        line_end: int = -1;
        for i: int = pos; i < buffer_len - 1; i += 1 {
            if self.buffer[i] == '\r' && self.buffer[i+1] == '\n' {
                line_end = i;
                break;
            }
        }

        if line_end == -1 {
            break; // No complete line found
        }

        line := string(self.buffer[pos:line_end]);
        line = trim(line);

        if len(line) == 0 {
            pos = line_end + 2;
            continue;
        }

        // Parse command
        space_idx := index_char(line, ' ');
        command := if space_idx != -1 {
            substring(line, 0, space_idx)
        } else {
            line
        };
        command = to_upper(command);

        if command == "MSG" {
            if message_count < 128 {
                msg := parse_message(line, pos, line_end + 2, self.buffer, buffer_len);
                if msg != null {
                    messages[message_count] = msg;
                    message_count += 1;
                    // Update pos based on message size
                    pos = line_end + 2 + msg.size + 2;
                    continue;
                }
            }
        } else if command == "PING" {
            if control_count < 128 {
                controls[control_count] = Nats_Control {.ping = _};
                control_count += 1;
            }
        } else if command == "PONG" {
            if control_count < 128 {
                controls[control_count] = Nats_Control {.pong = _};
                control_count += 1;
            }
        } else if command == "+OK" {
            if control_count < 128 {
                controls[control_count] = Nats_Control {.ok = _};
                control_count += 1;
            }
        } else if command == "-ERR" {
            if control_count < 128 && len(line) > 5 {
                err_msg := trim(substring(line, 5));
                controls[control_count] = Nats_Control {.err = err_msg};
                control_count += 1;
            }
        } else if command == "INFO" {
            if control_count < 128 && len(line) > 5 {
                info_payload := substring(line, 5);
                controls[control_count] = Nats_Control {.info = info_payload};
                control_count += 1;
            }
        }

        pos = line_end + 2;
    }

    return struct {
        messages = messages,
        controls = controls,
        message_count = message_count,
        control_count = control_count,
    };
}

parse_message :: (line: string, payload_start: int, buffer_offset: int, 
                  buffer: []u8, buffer_len: int) -> ?Nats_Message {
    // MSG <subject> <sid> [reply-to] <#bytes>
    parts := split(line, ' ');
    
    if len(parts) < 3 {
        return null;
    }

    subject := parts[1];
    sid := parts[2];
    reply_to: ?string = null;
    size_str: string = "";

    if len(parts) == 5 {
        // Has reply-to
        reply_to = parts[3];
        size_str = parts[4];
    } else if len(parts) == 4 {
        // No reply-to
        size_str = parts[3];
    } else {
        return null;
    }

    size := string_to_int(size_str);
    if size <= 0 {
        return null;
    }

    // Check if we have enough data
    total_needed := buffer_offset + size + 2;
    if buffer_len < total_needed {
        return null;
    }

    payload := string(buffer[buffer_offset : buffer_offset + size]);

    return Nats_Message {
        subject = subject,
        sid = sid,
        reply_to = reply_to,
        size = size,
        payload = payload,
    };
}

// ============================================================================
// gRPC Protocol Types and Parser
// ============================================================================

Grpc_Frame :: struct {
    compressed: bool;
    length: u32;
    message: []u8;
}

Grpc_Parser :: struct {
    buffer: []u8;
    allocator: core.Allocator;
    pos: int;
}

grpc_parser_init :: (allocator: core.Allocator) -> Grpc_Parser {
    return Grpc_Parser {
        buffer = allocator.alloc(u8, 4096),
        allocator = allocator,
        pos = 0,
    };
}

grpc_parser_deinit :: (self: *Grpc_Parser) {
    self.allocator.free(self.buffer);
}

grpc_parser_append :: (self: *Grpc_Parser, data: []u8) -> bool {
    if len(data) > len(self.buffer) - self.pos {
        return false;
    }
    copy(self.buffer[self.pos:], data);
    self.pos += len(data);
    return true;
}

grpc_parser_process :: (self: *Grpc_Parser) -> struct {
    frames: [][64]Grpc_Frame;
    count: int;
} {
    frames: [][64]Grpc_Frame;
    count: int = 0;

    while self.pos >= 5 {
        compressed := self.buffer[0] != 0;
        length := read_u32_big_endian(self.buffer[1:5]);

        if self.pos < 5 + length {
            break;
        }

        if count < 64 {
            message := self.buffer[5 : 5 + length];
            frames[count] = Grpc_Frame {
                compressed = compressed,
                length = length,
                message = message,
            };
            count += 1;

            // Remove processed frame from buffer
            remaining := self.pos - (5 + length);
            copy(self.buffer[0:], self.buffer[5 + length : self.pos]);
            self.pos = remaining;
        }
    }

    return struct {
        frames = frames,
        count = count,
    };
}

read_u32_big_endian :: (data: []u8) -> u32 {
    return cast<u32>(data[0]) << 24 |
           cast<u32>(data[1]) << 16 |
           cast<u32>(data[2]) << 8 |
           cast<u32>(data[3]);
}

// ============================================================================
// NATS Client for TCP Server
// ============================================================================

Nats_Client :: struct {
    stream: net.TCP_Stream;
    parser: Nats_Parser;
    subscriptions: [][256]string;
    subscription_count: int;
    id: u64;
    active: bool;
}

nats_client_init :: (stream: net.TCP_Stream, allocator: core.Allocator, id: u64) -> Nats_Client {
    return Nats_Client {
        stream = stream,
        parser = nats_parser_init(allocator),
        subscriptions = [][256]string {},
        subscription_count = 0,
        id = id,
        active = true,
    };
}

nats_client_deinit :: (self: *Nats_Client) {
    nats_parser_deinit(&self.parser);
}

// ============================================================================
// NATS Server - TCP Implementation
// ============================================================================

Nats_Server :: struct {
    port: u16;
    allocator: core.Allocator;
    server: net.TCP_Server;
    clients: [][1024]*Nats_Client;
    client_count: int;
}

nats_server_init :: (allocator: core.Allocator, port: u16) -> ?Nats_Server {
    address := net.IPv4_Address {
        host = {0, 0, 0, 0},
        port = port,
    };

    server := net.tcp_listen(address);
    if server == null {
        return null;
    }

    return Nats_Server {
        port = port,
        allocator = allocator,
        server = server,
        clients = [][1024]*Nats_Client {},
        client_count = 0,
    };
}

nats_server_run :: (self: *Nats_Server) {
    core.printf("NATS Server listening on port %d\n", self.port);
    
    client_id: u64 = 0;

    while true {
        client_stream := net.tcp_accept(self.server);
        if client_stream == null {
            continue;
        }

        client := self.allocator.alloc(Nats_Client, 1);
        client[0] = nats_client_init(client_stream, self.allocator, client_id);
        client_id += 1;

        if self.client_count < 1024 {
            self.clients[self.client_count] = client;
            self.client_count += 1;
            
            // Handle client in separate thread (pseudo-code)
            // go nats_server_handle_client(self, client);
            nats_server_handle_client(self, client);
        }
    }
}

nats_server_handle_client :: (self: *Nats_Server, client: *Nats_Client) {
    defer {
        // Remove client from list
        nats_client_deinit(client);
        self.allocator.free(client);
        net.tcp_close(client.stream);
    }

    // Send INFO message
    info_msg := "INFO {\"server_id\":\"jai-nats\",\"version\":\"1.0.0\",\"go\":\"false\"}\r\n";
    net.tcp_send(client.stream, string(info_msg));

    buffer: [4096]u8;

    while client.active {
        n := net.tcp_recv(client.stream, &buffer);
        if n <= 0 {
            break;
        }

        copy(client.parser.buffer[0:n], buffer[0:n]);
        result := nats_parser_process(&client.parser);

        for i: int = 0; i < result.control_count; i += 1 {
            control := result.controls[i];
            if control.ping != _ {
                net.tcp_send(client.stream, "PONG\r\n");
            }
        }

        for i: int = 0; i < result.message_count; i += 1 {
            msg := result.messages[i];
            core.printf("Received message on subject: %s\n", msg.subject);
            nats_server_broadcast(self, msg);
        }
    }
}

nats_server_broadcast :: (self: *Nats_Server, msg: Nats_Message) {
    for i: int = 0; i < self.client_count; i += 1 {
        client := self.clients[i];
        if !client.active {
            continue;
        }

        for j: int = 0; j < client.subscription_count; j += 1 {
            sub := client.subscriptions[j];
            if sub == msg.subject {
                response := core.sprintf("MSG %s %s %d\r\n%s\r\n", 
                    msg.subject, client.sid, len(msg.payload), msg.payload);
                net.tcp_send(client.stream, response);
            }
        }
    }
}

// ============================================================================
// gRPC Server over TCP
// ============================================================================

Grpc_Service_Handler :: proc (message: []u8) -> []u8;

Grpc_Server :: struct {
    port: u16;
    allocator: core.Allocator;
    server: net.TCP_Server;
    handlers: [][64]struct { name: string; handler: Grpc_Service_Handler };
    handler_count: int;
}

grpc_server_init :: (allocator: core.Allocator, port: u16) -> ?Grpc_Server {
    address := net.IPv4_Address {
        host = {0, 0, 0, 0},
        port = port,
    };

    server := net.tcp_listen(address);
    if server == null {
        return null;
    }

    return Grpc_Server {
        port = port,
        allocator = allocator,
        server = server,
        handlers = [][64]struct { name: string; handler: Grpc_Service_Handler } {},
        handler_count = 0,
    };
}

grpc_server_register_service :: (self: *Grpc_Server, name: string, handler: Grpc_Service_Handler) {
    if self.handler_count < 64 {
        self.handlers[self.handler_count] = struct {
            name = name,
            handler = handler,
        };
        self.handler_count += 1;
    }
}

grpc_server_run :: (self: *Grpc_Server) {
    core.printf("gRPC Server listening on port %d\n", self.port);

    while true {
        client_stream := net.tcp_accept(self.server);
        if client_stream == null {
            continue;
        }

        // Handle client in separate thread (pseudo-code)
        // go grpc_server_handle_client(self, client_stream);
        grpc_server_handle_client(self, client_stream);
    }
}

grpc_server_handle_client :: (self: *Grpc_Server, stream: net.TCP_Stream) {
    defer net.tcp_close(stream);

    parser := grpc_parser_init(self.allocator);
    defer grpc_parser_deinit(&parser);

    buffer: [4096]u8;

    while true {
        n := net.tcp_recv(stream, &buffer);
        if n <= 0 {
            break;
        }

        grpc_parser_append(&parser, buffer[0:n]);
        result := grpc_parser_process(&parser);

        for i: int = 0; i < result.count; i += 1 {
            frame := result.frames[i];
            // Process gRPC frame
            // In full implementation:
            // 1. Parse protobuf message
            // 2. Route to appropriate service handler
            // 3. Send response back
        }
    }
}

// ============================================================================
// Utility Functions
// ============================================================================

trim :: (s: string) -> string {
    start: int = 0;
    end: int = len(s);

    while start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\r' || s[start] == '\n') {
        start += 1;
    }

    while end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\r' || s[end-1] == '\n') {
        end -= 1;
    }

    return substring(s, start, end);
}

to_upper :: (s: string) -> string {
    result: [256]u8;
    for i: int = 0; i < len(s) && i < 256; i += 1 {
        c := s[i];
        if c >= 'a' && c <= 'z' {
            result[i] = c - 32;
        } else {
            result[i] = c;
        }
    }
    return string(result[0:len(s)]);
}

split :: (s: string, delimiter: u8) -> [][16]string {
    parts: [][16]string;
    count: int = 0;
    start: int = 0;

    for i: int = 0; i <= len(s); i += 1 {
        if i == len(s) || s[i] == delimiter {
            if count < 16 {
                parts[count] = substring(s, start, i);
                count += 1;
            }
            start = i + 1;
        }
    }

    return parts;
}

index_char :: (s: string, c: u8) -> int {
    for i: int = 0; i < len(s); i += 1 {
        if s[i] == c {
            return i;
        }
    }
    return -1;
}

substring :: (s: string, start: int, end: int) -> string {
    if start < 0 || end > len(s) || start >= end {
        return "";
    }
    return string(cast<[]u8>(s)[start:end]);
}

string_to_int :: (s: string) -> int {
    result: int = 0;
    for i: int = 0; i < len(s); i += 1 {
        c := s[i];
        if c >= '0' && c <= '9' {
            result = result * 10 + (c - '0');
        }
    }
    return result;
}

// ============================================================================
// Main Entry Point
// ============================================================================

main :: proc() {
    allocator := core.heap_allocator();

    // Start NATS server on port 4222
    nats_server := nats_server_init(allocator, 4222);
    if nats_server == null {
        core.printf("Failed to start NATS server\n");
        return;
    }

    // Start gRPC server on port 50051
    grpc_server := grpc_server_init(allocator, 50051);
    if grpc_server == null {
        core.printf("Failed to start gRPC server\n");
        return;
    }

    // Run servers (in production, these would run in separate threads)
    core.printf("Starting NATS and gRPC servers...\n");
    
    // For demonstration, run NATS server first
    nats_server_run(&nats_server);
    
    // Then gRPC server (unreachable in this demo)
    // grpc_server_run(&grpc_server);
}
