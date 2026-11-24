const std = @import("std");
const pubsub = @import("pubsub");

const Event = union(enum) { number: u32, signal: void };
const Bus = pubsub.Bus(Event);

/// 在独立线程中接收，以验证关闭会唤醒真实的并发操作。
const Reader = struct {
    subscription: Bus.Subscription,
    result: pubsub.RecvError!usize = 0,
    output: [1]Event = undefined,

    fn run(self: *Reader) void {
        self.result = self.subscription.recv(std.testing.io, &self.output, 1);
    }

    fn waitUntilRegistered(self: *Reader) void {
        const node = self.subscription.node;
        while (true) {
            node.recv_lock.lockUncancelable(std.testing.io);
            const active = node.active_receivers;
            node.recv_lock.unlock(std.testing.io);
            if (active > 0) return;
            std.atomic.spinLoopHint();
        }
    }
};

test "deinit wakes readers and releases all topics idempotently" {
    var tracking = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var bus = Bus.init(tracking.allocator());
    const first = try bus.subscribe(std.testing.io, .number, 1);
    const second = try bus.subscribe(std.testing.io, .signal, 1);
    var reader1: Reader = .{ .subscription = first };
    var reader2: Reader = .{ .subscription = second };
    const thread1 = try std.Thread.spawn(.{}, Reader.run, .{&reader1});
    const thread2 = try std.Thread.spawn(.{}, Reader.run, .{&reader2});
    reader1.waitUntilRegistered();
    reader2.waitUntilRegistered();

    // deinit 必须自行唤醒阻塞接收，而不是要求调用方先结束它们。
    bus.deinit(std.testing.io);
    thread1.join();
    thread2.join();
    try std.testing.expectError(error.Closed, reader1.result);
    try std.testing.expectError(error.Closed, reader2.result);
    bus.deinit(std.testing.io);
    bus.unsubscribe(std.testing.io, first);
    bus.unsubscribe(std.testing.io, second);
    try std.testing.expectEqual(tracking.allocations, tracking.deallocations);
    try std.testing.expectEqual(tracking.allocated_bytes, tracking.freed_bytes);
    try std.testing.expectError(error.Closed, bus.publish(std.testing.io, .{ .number = 1 }));
    try std.testing.expectError(error.Closed, bus.subscribe(std.testing.io, .number, 1));
    var output: [1]Event = undefined;
    try std.testing.expectError(error.NotSubscribed, first.recv(std.testing.io, &output, 0));
    try std.testing.expectError(error.NotSubscribed, second.recv(std.testing.io, &output, 1));
}

test "unsubscribe wakes a blocked receiver before freeing its queue" {
    var bus = Bus.init(std.testing.allocator);
    defer bus.deinit(std.testing.io);
    const subscription = try bus.subscribe(std.testing.io, .number, 1);
    var reader: Reader = .{ .subscription = subscription };
    const thread = try std.Thread.spawn(.{}, Reader.run, .{&reader});
    reader.waitUntilRegistered();

    bus.unsubscribe(std.testing.io, subscription);
    thread.join();
    try std.testing.expectError(error.Closed, reader.result);
    var output: [1]Event = undefined;
    try std.testing.expectError(error.NotSubscribed, subscription.recv(std.testing.io, &output, 0));
}

test "deinit releases buffered messages and empty publishers" {
    var tracking = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var bus = Bus.init(tracking.allocator());
    const subscription = try bus.subscribe(std.testing.io, .number, 2);
    const result = try bus.publish(std.testing.io, .{ .number = 123 });
    try std.testing.expectEqual(@as(usize, 1), result.delivered);
    bus.deinit(std.testing.io);
    try std.testing.expectEqual(tracking.allocations, tracking.deallocations);
    var output: [1]Event = undefined;
    try std.testing.expectError(error.NotSubscribed, subscription.recv(std.testing.io, &output, 0));

    var empty = Bus.init(std.testing.allocator);
    empty.deinit(std.testing.io);
    empty.deinit(std.testing.io);
}
