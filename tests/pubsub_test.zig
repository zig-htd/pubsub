const std = @import("std");
const pubsub = @import("pubsub");

const Event = union(enum) {
    number: u32,
    signal: void,
};
const Bus = pubsub.Bus(Event);

test "union variants map to compile-time topic slots" {
    try std.testing.expectEqual(@as(usize, 2), Bus.topic_count);
    try std.testing.expectEqual(@as(usize, 0), Bus.topicIndex(.number));
    try std.testing.expectEqual(@as(usize, 1), Bus.topicIndex(.signal));
}

test "publisher starts open with empty topic routes" {
    const bus = Bus.init(std.testing.allocator);
    try std.testing.expect(!bus.closed);
    try std.testing.expect(bus.topics[0].subscribers == null);
}

test "subscriptions belong to their creating publisher" {
    // 不同发布者的锁彼此独立；错误的发布者不得访问或摘除订阅节点。
    var first_bus = Bus.init(std.testing.allocator);
    var second_bus = Bus.init(std.testing.allocator);
    const subscription = try first_bus.subscribe(std.testing.io, .number, 1);
    second_bus.unsubscribe(std.testing.io, subscription);
    const result = try first_bus.publish(std.testing.io, .{ .number = 11 });
    try std.testing.expectEqual(@as(usize, 1), result.delivered);
    first_bus.unsubscribe(std.testing.io, subscription);
}

test "failed subscription allocation leaves no routing or allocated memory" {
    // 分别让节点与缓冲区的分配失败，确认每一步的回滚均有效。
    for (0..2) |fail_index| {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = fail_index });
        var bus = Bus.init(failing.allocator());
        try std.testing.expectError(error.OutOfMemory, bus.subscribe(std.testing.io, .number, 1));
        try std.testing.expect(bus.topics[0].subscribers == null);
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
    }
}

test "subscribing to a closed publisher releases allocations" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var bus = Bus.init(failing.allocator());
    bus.closed = true;
    try std.testing.expectError(error.Closed, bus.subscribe(std.testing.io, .number, 1));
    try std.testing.expect(bus.topics[0].subscribers == null);
    try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
}

test "unsubscribe destroys both subscription and queue buffer" {
    // 分别计数堆分配与释放，验证包含未消费消息的队列也会归还缓冲区。
    var tracking = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var bus = Bus.init(tracking.allocator());
    const subscription = try bus.subscribe(std.testing.io, .number, 2);
    try std.testing.expectEqual(@as(usize, 2), tracking.allocations);
    const result = try bus.publish(std.testing.io, .{ .number = 123 });
    try std.testing.expectEqual(@as(usize, 1), result.delivered);
    bus.unsubscribe(std.testing.io, subscription);
    bus.unsubscribe(std.testing.io, subscription);
    try std.testing.expect(bus.topics[0].subscribers == null);
    try std.testing.expectEqual(tracking.allocations, tracking.deallocations);
    try std.testing.expectEqual(tracking.allocated_bytes, tracking.freed_bytes);
}

test "repeated unsubscribe does not affect a newer subscription" {
    // 旧句柄保留身份编号，内存地址被重用时也不能摘除新订阅。
    var tracking = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var bus = Bus.init(tracking.allocator());
    const old = try bus.subscribe(std.testing.io, .number, 1);
    const old_copy = old;
    bus.unsubscribe(std.testing.io, old);
    const newer = try bus.subscribe(std.testing.io, .number, 1);
    bus.unsubscribe(std.testing.io, old_copy);
    const result = try bus.publish(std.testing.io, .{ .number = 42 });
    try std.testing.expectEqual(@as(usize, 1), result.delivered);
    var received: [1]Event = undefined;
    try std.testing.expectEqual(@as(usize, 1), try newer.recv(std.testing.io, &received, 0));
    try std.testing.expectEqual(@as(u32, 42), received[0].number);
    bus.unsubscribe(std.testing.io, newer);
    bus.unsubscribe(std.testing.io, newer);
    try std.testing.expectEqual(tracking.allocations, tracking.deallocations);
}

test "concurrent unsubscribe destroys a subscription once" {
    const Task = struct {
        fn run(bus: *Bus, subscription: Bus.Subscription) void {
            bus.unsubscribe(std.testing.io, subscription);
        }
    };
    var tracking = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var bus = Bus.init(tracking.allocator());
    const subscription = try bus.subscribe(std.testing.io, .number, 1);
    const thread = try std.Thread.spawn(.{}, Task.run, .{ &bus, subscription });
    bus.unsubscribe(std.testing.io, subscription);
    thread.join();
    try std.testing.expect(bus.topics[0].subscribers == null);
    try std.testing.expectEqual(tracking.allocations, tracking.deallocations);
}

test "new subscription starts with an empty queue" {
    var bus = Bus.init(std.testing.allocator);
    const first = try bus.subscribe(std.testing.io, .number, 1);
    _ = try bus.publish(std.testing.io, .{ .number = 9 });
    bus.unsubscribe(std.testing.io, first);
    const second = try bus.subscribe(std.testing.io, .number, 1);

    // 已取消订阅的消息不会混入新订阅的队列。
    var output: [1]Event = undefined;
    try std.testing.expectEqual(@as(usize, 0), try second.recv(std.testing.io, &output, 0));
    bus.unsubscribe(std.testing.io, second);
}

test "unsubscribe removes a node from the topic route" {
    var bus = Bus.init(std.testing.allocator);
    const first = try bus.subscribe(std.testing.io, .number, 1);
    const middle = try bus.subscribe(std.testing.io, .number, 1);
    const last = try bus.subscribe(std.testing.io, .number, 1);

    // 移除中间节点后，前后节点仍可达；继续覆盖头节点和尾节点。
    bus.unsubscribe(std.testing.io, middle);
    try std.testing.expect(bus.topics[0].subscribers == last.node);
    try std.testing.expect(last.node.next_for_topic == first.node);
    bus.unsubscribe(std.testing.io, last);
    bus.unsubscribe(std.testing.io, first);
    try std.testing.expect(bus.topics[0].subscribers == null);
}

test "publish delivers to each matching subscription and preserves topic isolation" {
    var bus = Bus.init(std.testing.allocator);
    const first = try bus.subscribe(std.testing.io, .number, 2);
    const second = try bus.subscribe(std.testing.io, .number, 2);
    const other = try bus.subscribe(std.testing.io, .signal, 2);

    const result = try bus.publish(std.testing.io, .{ .number = 42 });
    try std.testing.expectEqual(@as(usize, 2), result.delivered);
    try std.testing.expectEqual(@as(usize, 0), result.full);
    var received: [1]Event = undefined;
    try std.testing.expectEqual(@as(usize, 1), try first.recv(std.testing.io, &received, 0));
    try std.testing.expectEqual(@as(u32, 42), received[0].number);
    try std.testing.expectEqual(@as(usize, 1), try second.recv(std.testing.io, &received, 1));
    try std.testing.expectEqual(@as(u32, 42), received[0].number);
    try std.testing.expectEqual(@as(usize, 0), try other.recv(std.testing.io, &received, 0));

    bus.unsubscribe(std.testing.io, first);
    const next = try bus.publish(std.testing.io, .{ .number = 7 });
    try std.testing.expectEqual(@as(usize, 1), next.delivered);
    try std.testing.expectEqual(@as(usize, 1), try second.recv(std.testing.io, &received, 0));
    try std.testing.expectEqual(@as(u32, 7), received[0].number);
    bus.unsubscribe(std.testing.io, second);
    bus.unsubscribe(std.testing.io, other);
}

test "a full queue does not block or prevent delivery to other subscribers" {
    var bus = Bus.init(std.testing.allocator);
    const full = try bus.subscribe(std.testing.io, .number, 1);
    const available = try bus.subscribe(std.testing.io, .number, 1);
    _ = try bus.publish(std.testing.io, .{ .number = 1 });
    var output: [1]Event = undefined;
    _ = try available.recv(std.testing.io, &output, 0);

    const result = try bus.publish(std.testing.io, .{ .number = 2 });
    try std.testing.expectEqual(@as(usize, 1), result.full);
    try std.testing.expectEqual(@as(usize, 1), result.delivered);
    try std.testing.expectEqual(@as(usize, 1), try available.recv(std.testing.io, &output, 0));
    try std.testing.expectEqual(@as(u32, 2), output[0].number);
    bus.unsubscribe(std.testing.io, available);
    bus.unsubscribe(std.testing.io, full);
}

test "closed subscription queues are reported without stopping delivery" {
    var bus = Bus.init(std.testing.allocator);
    const open = try bus.subscribe(std.testing.io, .number, 1);
    const closed = try bus.subscribe(std.testing.io, .number, 1);
    closed.node.queue.close(std.testing.io);

    const result = try bus.publish(std.testing.io, .{ .number = 9 });
    try std.testing.expectEqual(@as(usize, 1), result.closed);
    try std.testing.expectEqual(@as(usize, 1), result.delivered);
    var output: [1]Event = undefined;
    try std.testing.expectEqual(@as(usize, 1), try open.recv(std.testing.io, &output, 0));
    try std.testing.expectEqual(@as(u32, 9), output[0].number);
    bus.unsubscribe(std.testing.io, closed);
    bus.unsubscribe(std.testing.io, open);
}

test "a waiting subscriber receives a later publish" {
    const Reader = struct {
        subscription: Bus.Subscription,
        ready: std.Io.Event = .unset,
        result: pubsub.RecvError!usize = 0,
        output: [1]Event = undefined,

        fn run(reader: *@This()) void {
            reader.ready.set(std.testing.io);
            reader.result = reader.subscription.recv(std.testing.io, &reader.output, 1);
        }
    };
    var bus = Bus.init(std.testing.allocator);
    const subscription = try bus.subscribe(std.testing.io, .number, 1);
    var reader: Reader = .{ .subscription = subscription };
    const thread = try std.Thread.spawn(.{}, Reader.run, .{&reader});
    reader.ready.waitUncancelable(std.testing.io);
    const result = try bus.publish(std.testing.io, .{ .number = 77 });
    thread.join();
    try std.testing.expectEqual(@as(usize, 1), result.delivered);
    try std.testing.expectEqual(@as(usize, 1), try reader.result);
    try std.testing.expectEqual(@as(u32, 77), reader.output[0].number);
    bus.unsubscribe(std.testing.io, subscription);
}

test "queue preserves order before unsubscribe" {
    var bus = Bus.init(std.testing.allocator);
    const subscription = try bus.subscribe(std.testing.io, .number, 2);
    const first = try bus.publish(std.testing.io, .{ .number = 1 });
    const second = try bus.publish(std.testing.io, .{ .number = 2 });
    try std.testing.expectEqual(@as(usize, 1), first.delivered);
    try std.testing.expectEqual(@as(usize, 1), second.delivered);
    var output: [2]Event = undefined;
    try std.testing.expectEqual(@as(usize, 2), try subscription.recv(std.testing.io, &output, 0));
    try std.testing.expectEqual(@as(u32, 1), output[0].number);
    try std.testing.expectEqual(@as(u32, 2), output[1].number);
    bus.unsubscribe(std.testing.io, subscription);
    const after = try bus.publish(std.testing.io, .{ .number = 3 });
    try std.testing.expectEqual(@as(usize, 0), after.delivered);
}

test "close drains buffered messages before unsubscribe" {
    var bus = Bus.init(std.testing.allocator);
    const subscription = try bus.subscribe(std.testing.io, .number, 2);

    try std.testing.expectEqual(@as(usize, 1), (try bus.publish(std.testing.io, .{ .number = 1 })).delivered);
    try std.testing.expectEqual(@as(usize, 1), (try bus.publish(std.testing.io, .{ .number = 2 })).delivered);

    // close 只关闭队列，不从发布者路由摘除节点。
    subscription.close(std.testing.io);

    var output: [2]Event = undefined;
    try std.testing.expectEqual(@as(usize, 2), try subscription.recv(std.testing.io, &output, 0));
    try std.testing.expectEqual(@as(u32, 1), output[0].number);
    try std.testing.expectEqual(@as(u32, 2), output[1].number);

    // 队列空后继续接收应返回 Closed，且新发布不能再次进入。
    try std.testing.expectError(error.Closed, subscription.recv(std.testing.io, &output, 0));
    try std.testing.expectEqual(@as(usize, 0), (try bus.publish(std.testing.io, .{ .number = 3 })).delivered);

    // 排空并等待接收者退出后，再移除节点并释放队列。
    bus.unsubscribe(std.testing.io, subscription);
    try std.testing.expectError(error.NotSubscribed, subscription.recv(std.testing.io, &output, 0));
}

test "publish copies pointer and slice payloads by value" {
    // 带指针的字段只复制地址；测试数据在所有订阅者消费完之前保持有效。
    const Message = union(enum) {
        number: u32,
        borrowed: struct { data: []const u8, pointer: *const u32 },
    };
    const PointerBus = pubsub.Bus(Message);
    var bus = PointerBus.init(std.testing.allocator);
    const first = try bus.subscribe(std.testing.io, .borrowed, 1);
    const second = try bus.subscribe(std.testing.io, .borrowed, 1);
    const number: u32 = 42;
    const text = "hello";

    const result = try bus.publish(std.testing.io, .{ .borrowed = .{ .data = text, .pointer = &number } });
    try std.testing.expectEqual(@as(usize, 2), result.delivered);
    var output: [1]Message = undefined;
    try std.testing.expectEqual(@as(usize, 1), try first.recv(std.testing.io, &output, 0));
    try std.testing.expectEqualStrings(text, output[0].borrowed.data);
    try std.testing.expect(output[0].borrowed.pointer == &number);
    try std.testing.expectEqual(@as(usize, 1), try second.recv(std.testing.io, &output, 0));
    try std.testing.expectEqualStrings(text, output[0].borrowed.data);
    try std.testing.expect(output[0].borrowed.pointer == &number);
    bus.unsubscribe(std.testing.io, first);
    bus.unsubscribe(std.testing.io, second);
}
