//! NATS Server implementation in Zig
//! Implements both NATS text protocol and gRPC over TCP

const std = @import("std");
const net = std.net;
const mem = std.mem;

// ============================================================================
// NATS Protocol Types
// ============================================================================

pub const NatsMessage = struct {
    subject: []const u8,
    sid: []const u8,
    reply_to: ?[]const u8,
    size: usize,
    payload: []const u8,
};

pub const NatsControl = union(enum) {
    ping,
    pong,
    ok,
    err: []const u8,
    info: []const u8,
};

// ============================================================================
// NATS Parser - Text Protocol Implementation
// ============================================================================

pub const NatsParser = struct {
    buffer: std.ArrayList(u8),
    allocator: mem.Allocator,

    pub fn init(allocator: mem.Allocator) NatsParser {
        return .{
            .buffer = std.ArrayList(u8).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *NatsParser) void {
        self.buffer.deinit();
    }

    pub fn append(self: *NatsParser, data: []const u8) !void {
        try self.buffer.appendSlice(data);
    }

    pub fn process(self: *NatsParser) !struct {
        messages: std.ArrayList(NatsMessage),
        controls: std.ArrayList(NatsControl),
    } {
        var messages = std.ArrayList(NatsMessage).init(self.allocator);
        var controls = std.ArrayList(NatsControl).init(self.allocator);
        errdefer messages.deinit();
        errdefer controls.deinit();

        while (self.buffer.items.len > 0) {
            const line_end = mem.indexOfScalar(u8, self.buffer.items, '\n');
            if (line_end == null) break;

            const line_end_idx = line_end.?;
            
            // Check for \r\n
            if (line_end_idx == 0 or self.buffer.items[line_end_idx - 1] != '\r') {
                // Invalid line ending, skip
                self.buffer.items = self.buffer.items[line_end_idx + 1 ..];
                continue;
            }

            const line = mem.trimRight(u8, self.buffer.items[0..line_end_idx - 1], " \t");
            
            if (line.len == 0) {
                self.buffer.items = self.buffer.items[line_end_idx + 1 ..];
                continue;
            }

            const space_idx = mem.indexOfScalar(u8, line, ' ');
            const command = if (space_idx) |idx| 
                mem.eql(u8, mem.trimRight(u8, line[0..idx], " \t"), "") 
                then line 
                else line[0..space_idx.?];
            
            const cmd_upper = toUpper(command);

            if (mem.eql(u8, &cmd_upper, "MSG")) {
                const msg_result = try self.parseMessage(line, line_end_idx + 1);
                if (msg_result) |msg| {
                    try messages.append(msg);
                }
            } else if (mem.eql(u8, &cmd_upper, "PING")) {
                try controls.append(.ping);
                self.buffer.items = self.buffer.items[line_end_idx + 1 ..];
            } else if (mem.eql(u8, &cmd_upper, "PONG")) {
                try controls.append(.pong);
                self.buffer.items = self.buffer.items[line_end_idx + 1 ..];
            } else if (mem.eql(u8, &cmd_upper, "+OK")) {
                try controls.append(.ok);
                self.buffer.items = self.buffer.items[line_end_idx + 1 ..];
            } else if (mem.eql(u8, &cmd_upper, "-ERR")) {
                const err_msg = if (line.len > 5) mem.trim(u8, line[5..], " \"") else "";
                try controls.append(.{ .err = err_msg });
                self.buffer.items = self.buffer.items[line_end_idx + 1 ..];
            } else if (mem.eql(u8, &cmd_upper, "INFO")) {
                const info_payload = if (line.len > 5) line[5..] else "";
                try controls.append(.{ .info = info_payload });
                self.buffer.items = self.buffer.items[line_end_idx + 1 ..];
            } else {
                // Unknown command, skip line
                self.buffer.items = self.buffer.items[line_end_idx + 1 ..];
            }
        }

        return .{ .messages = messages, .controls = controls };
    }

    fn parseMessage(self: *NatsParser, line: []const u8, payload_start: usize) !?NatsMessage {
        // MSG <subject> <sid> [reply-to] <#bytes>
        var iter = mem.tokenizeScalar(u8, line, ' ');
        
        _ = iter.next(); // Skip "MSG"
        const subject = iter.next() orelse return null;
        const sid = iter.next() orelse return null;
        
        var reply_to: ?[]const u8 = null;
        var size_str: ?[]const u8 = null;
        
        const third = iter.next();
        const fourth = iter.next();
        
        if (fourth) |f| {
            // Has reply-to: MSG subject sid reply-to size
            reply_to = third;
            size_str = f;
        } else if (third) |t| {
            // No reply-to: MSG subject sid size
            size_str = t;
        } else {
            return null;
        }

        const size = std.fmt.parseInt(usize, size_str.?, 10) catch return null;
        
        // Check if we have enough data for the payload
        const total_needed = payload_start + size + 2; // +2 for \r\n after payload
        if (self.buffer.items.len < total_needed) {
            return null;
        }

        const payload = self.buffer.items[payload_start .. payload_start + size];
        
        // Remove processed data from buffer
        self.buffer.items = self.buffer.items[total_needed..];

        return NatsMessage{
            .subject = subject,
            .sid = sid,
            .reply_to = reply_to,
            .size = size,
            .payload = payload,
        };
    }

    fn toUpper(str: []const u8) [10]u8 {
        var result: [10]u8 = undefined;
        const len = @min(str.len, 10);
        for (str[0..len], 0..) |c, i| {
            result[i] = if (c >= 'a' and c <= 'z') c - 32 else c;
        }
        return result;
    }
};

// ============================================================================
// gRPC Protocol Types and Parser
// ============================================================================

pub const GrpcFrame = struct {
    compressed: bool,
    length: u32,
    message: []const u8,
};

pub const GrpcParser = struct {
    buffer: std.ArrayList(u8),
    allocator: mem.Allocator,

    pub fn init(allocator: mem.Allocator) GrpcParser {
        return .{
            .buffer = std.ArrayList(u8).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *GrpcParser) void {
        self.buffer.deinit();
    }

    pub fn append(self: *GrpcParser, data: []const u8) !void {
        try self.buffer.appendSlice(data);
    }

    pub fn process(self: *GrpcParser) !std.ArrayList(GrpcFrame) {
        var frames = std.ArrayList(GrpcFrame).init(self.allocator);
        errdefer frames.deinit();

        while (self.buffer.items.len >= 5) {
            const compressed = self.buffer.items[0] != 0;
            const length = mem.readIntBig(u32, self.buffer.items[1..5]);

            if (self.buffer.items.len < 5 + length) {
                break;
            }

            const message = self.buffer.items[5 .. 5 + length];
            try frames.append(GrpcFrame{
                .compressed = compressed,
                .length = length,
                .message = message,
            });

            self.buffer.items = self.buffer.items[5 + length ..];
        }

        return frames;
    }
};

// ============================================================================
// NATS Server - TCP Implementation
// ============================================================================

pub const NatsServer = struct {
    port: u16,
    allocator: mem.Allocator,
    server_socket: net.Server,
    clients: std.ArrayList(*Client),
    mutex: std.Thread.Mutex,

    const Client = struct {
        stream: net.Stream,
        parser: NatsParser,
        subscriptions: std.StringHashMap(void),
        id: u64,
        active: bool,

        pub fn init(stream: net.Stream, allocator: mem.Allocator, id: u64) !*Client {
            const client = try allocator.create(Client);
            client.stream = stream;
            client.parser = NatsParser.init(allocator);
            client.subscriptions = std.StringHashMap(void).init(allocator);
            client.id = id;
            client.active = true;
            return client;
        }

        pub fn deinit(self: *Client) void {
            self.parser.deinit();
            var it = self.subscriptions.keyIterator();
            while (it.next()) |key| {
                self.subscriptions.allocator.free(key.*);
            }
            self.subscriptions.deinit();
        }
    };

    pub fn init(allocator: mem.Allocator, port: u16) !NatsServer {
        const address = try net.Address.parseIp4("0.0.0.0", port);
        const server_socket = try net.tcpListen(address);
        
        return NatsServer{
            .port = port,
            .allocator = allocator,
            .server_socket = server_socket,
            .clients = std.ArrayList(*Client).init(allocator),
            .mutex = std.Thread.Mutex{},
        };
    }

    pub fn run(self: *NatsServer) !void {
        var client_id: u64 = 0;
        
        std.debug.print("NATS Server listening on port {d}\n", .{self.port});
        
        while (true) {
            const client_stream = try self.server_socket.accept();
            
            const client = try Client.init(client_stream.stream, self.allocator, client_id);
            client_id += 1;
            
            self.mutex.lock();
            try self.clients.append(client);
            self.mutex.unlock();
            
            const thread = try std.Thread.spawn(.{}, handleClient, .{ self, client });
            thread.detach();
        }
    }

    fn handleClient(self: *NatsServer, client: *Client) void {
        defer {
            self.mutex.lock();
            const idx = self.clients.items.indexOf(client);
            if (idx) |i| {
                _ = self.clients.orderedRemove(i);
            }
            self.mutex.unlock();
            client.deinit();
            client.stream.close();
        }

        // Send INFO message
        const info_msg = "INFO {\"server_id\":\"zig-nats\",\"version\":\"1.0.0\",\"go\":\"false\"}\r\n";
        client.stream.write(info_msg) catch return;

        var buffer: [4096]u8 = undefined;
        
        while (client.active) {
            const n = client.stream.read(&buffer) catch break;
            if (n == 0) break;

            client.parser.append(buffer[0..n]) catch break;
            const result = client.parser.process() catch break;
            defer {
                result.messages.deinit();
                result.controls.deinit();
            }

            for (result.controls.items) |control| {
                switch (control) {
                    .ping => {
                        client.stream.write("PONG\r\n") catch break;
                    },
                    .pong, .ok => {},
                    .err => {},
                    .info => {},
                }
            }

            for (result.messages.items) |msg| {
                std.debug.print("Received message on subject: {s}\n", .{msg.subject});
                // Broadcast to subscribers
                self.broadcast(msg) catch break;
            }
        }
    }

    fn broadcast(self: *NatsServer, msg: NatsMessage) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.clients.items) |client| {
            if (!client.active) continue;
            
            var it = client.subscriptions.keyIterator();
            while (it.next()) |sub| {
                if (matchPattern(sub.*, msg.subject)) {
                    const reply = try std.fmt.allocPrint(
                        self.allocator,
                        "MSG {s} {s} {d}\r\n{s}\r\n",
                        .{ msg.subject, client.sid, msg.payload.len, msg.payload },
                    );
                    client.stream.write(reply) catch continue;
                }
            }
        }
    }

    fn matchPattern(pattern: []const u8, subject: []const u8) bool {
        // Simple exact match for now
        return mem.eql(u8, pattern, subject);
    }
};

// ============================================================================
// gRPC Server over TCP
// ============================================================================

pub const GrpcServer = struct {
    port: u16,
    allocator: mem.Allocator,
    server_socket: net.Server,
    services: std.StringHashMap(ServiceHandler),

    const ServiceHandler = *const fn ([]const u8) anyerror![]const u8;

    pub fn init(allocator: mem.Allocator, port: u16) !GrpcServer {
        const address = try net.Address.parseIp4("0.0.0.0", port);
        const server_socket = try net.tcpListen(address);
        
        return GrpcServer{
            .port = port,
            .allocator = allocator,
            .server_socket = server_socket,
            .services = std.StringHashMap(ServiceHandler).init(allocator),
        };
    }

    pub fn registerService(self: *GrpcServer, name: []const u8, handler: ServiceHandler) !void {
        const key = try self.allocator.dupe(u8, name);
        try self.services.put(key, handler);
    }

    pub fn run(self: *GrpcServer) !void {
        std.debug.print("gRPC Server listening on port {d}\n", .{self.port});
        
        while (true) {
            const client_stream = try self.server_socket.accept();
            
            const thread = try std.Thread.spawn(.{}, handleGrpcClient, .{ self, client_stream.stream });
            thread.detach();
        }
    }

    fn handleGrpcClient(self: *GrpcServer, stream: net.Stream) void {
        defer stream.close();

        var parser = GrpcParser.init(self.allocator);
        defer parser.deinit();

        var buffer: [4096]u8 = undefined;
        
        while (true) {
            const n = stream.read(&buffer) catch break;
            if (n == 0) break;

            parser.append(buffer[0..n]) catch break;
            const frames = parser.process() catch break;
            defer frames.deinit();

            for (frames.items) |frame| {
                // Process gRPC frame
                _ = frame;
                // In a full implementation, you would:
                // 1. Parse the protobuf message
                // 2. Route to the appropriate service handler
                // 3. Send the response back
            }
        }
    }
};

// ============================================================================
// Main Entry Point
// ============================================================================

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Start NATS server on port 4222
    var nats_server = try NatsServer.init(allocator, 4222);
    
    // Start gRPC server on port 50051
    var grpc_server = try GrpcServer.init(allocator, 50051);

    // Run servers in separate threads
    const nats_thread = try std.Thread.spawn(.{}, runNats, .{&nats_server});
    const grpc_thread = try std.Thread.spawn(.{}, runGrpc, .{&grpc_server});

    nats_thread.join();
    grpc_thread.join();
}

fn runNats(server: *NatsServer) void {
    server.run() catch |err| {
        std.debug.print("NATS Server error: {}\n", .{err});
    };
}

fn runGrpc(server: *GrpcServer) void {
    server.run() catch |err| {
        std.debug.print("gRPC Server error: {}\n", .{err});
    };
}
