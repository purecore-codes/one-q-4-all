const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const benchmark = b.addExecutable(.{
        .name = "nats-benchmark",
        .root_source_file = b.path("benchmark.zig"),
        .target = target,
        .optimize = optimize,
    });

    b.installArtifact(benchmark);

    const run_cmd = b.addRunArtifact(benchmark);
    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Executar o stress test");
    run_step.dependOn(&run_cmd.step);

    const unit_tests = b.addTest(.{
        .root_source_file = b.path("benchmark.zig"),
        .target = target,
        .optimize = optimize,
    });

    const run_unit_tests = b.addRunArtifact(unit_tests);
    const test_step = b.step("test", "Executar testes unitários");
    test_step.dependOn(&run_unit_tests.step);
}
