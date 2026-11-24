const std = @import("std");
const Allocator = std.mem.Allocator;
const pubsub = @import("pubsub");
const rc = @import("rc");

// 每个 union 变体代表一个主题，其载荷类型可以彼此不同。
const Message = union(enum) {
    temperature: f32,
    status: u8,
    user: rc.AtomicShared(User), // 引用计数容器包装的数据，发送前会自动增加计数。共享给多个接收方，也能安全释放。
};

const MessageBug = pubsub.Bus(Message);

const User = struct {
    name: []u8,

    pub fn clone(self: *const User, allocator: Allocator) Allocator.Error!User {
        return .{ .name = try allocator.dupe(u8, self.name) };
    }

    pub fn release(self: *User, allocator: Allocator) void {
        allocator.free(self.name);
    }
};

/// 展示两个订阅者分别接收普通值消息和 AtomicShared 消息。
pub fn main(init: std.process.Init) !void {
    var bus = MessageBug.init(init.arena.allocator());
    defer bus.deinit(init.io);

    try demonstrateValues(&bus, init.io);
    try demonstrateAtomicShared(&bus, init.io, init.gpa);
}

/// 展示两个订阅者接收同一主题的纯值消息。
fn demonstrateValues(bus: *MessageBug, io: std.Io) !void {
    // 每次订阅都会分配独立的节点与队列，取消订阅时一并销毁。
    const first = try bus.subscribe(io, .temperature, 1);
    defer bus.unsubscribe(io, first);
    const second = try bus.subscribe(io, .temperature, 1);
    defer bus.unsubscribe(io, second);

    // 发布不会等待消费者；返回值明确报告投递失败的订阅数量。
    const result = try bus.publish(io, .{ .temperature = 23.5 });
    std.debug.print("delivered={d}, full={d}, closed={d}\n", .{ result.delivered, result.full, result.closed });
    if (result.delivered != 2 or result.full != 0 or result.closed != 0) return error.DeliveryIncomplete;

    var received: [1]Message = undefined;
    // 批量接收将消息写入调用方缓冲区，返回值表示实际写入的数量。
    if (try first.recv(io, &received, 1) != 1) return error.ReceiveIncomplete;
    std.debug.print("first temperature={d}\n", .{received[0].temperature});
    if (try second.recv(io, &received, 1) != 1) return error.ReceiveIncomplete;
    std.debug.print("second temperature={d}\n", .{received[0].temperature});
}

/// 展示 AtomicShared 消息投递给多个订阅者，并在每个订阅者处释放。
fn demonstrateAtomicShared(bus: *MessageBug, io: std.Io, allocator: Allocator) !void {
    // 两个订阅各有独立队列，发布时会分别增加一次引用计数。
    const first = try bus.subscribe(io, .user, 1);
    defer bus.unsubscribe(io, first);
    const second = try bus.subscribe(io, .user, 1);
    defer bus.unsubscribe(io, second);

    var user: User = .{ .name = try allocator.dupe(u8, "Alen") };
    const shared_user: rc.AtomicShared(User) = try .fromOwned(allocator, &user);
    defer shared_user.release(allocator); // 发送前会增加引用计数，所以创建者自己要释放一次。

    const result = try bus.publish(io, .{ .user = shared_user });
    if (result.delivered != 2) return error.DeliveryIncomplete;
    if (shared_user.strongCount() != 3) return error.UnexpectedReferenceCount;

    var first_buffer: [1]Message = undefined;
    if (try first.recv(io, &first_buffer, 1) != 1) return error.ReceiveIncomplete;
    const first_user = first_buffer[0].user;
    defer first_user.release(allocator);

    var second_buffer: [1]Message = undefined;
    if (try second.recv(io, &second_buffer, 1) != 1) return error.ReceiveIncomplete;
    const second_user = second_buffer[0].user;
    defer second_user.release(allocator);

    std.debug.print("first user={s}, second user={s}\n", .{
        first_user.get().name,
        second_user.get().name,
    });
}
