const std = @import("std");
const mem = std.mem;
const Io = std.Io;
const Allocator = mem.Allocator;
const subscriptions = @import("subscriptions.zig");
const rc = @import("rc");

/// 汇总所有发布操作可选的行为，默认不保留主题消息。
pub const PublishOptions = struct {
    retain: bool = false,
};
pub const AtomicShared = rc.AtomicShared;

pub const SubscribeError = Io.Cancelable || Allocator.Error || error{
    Closed,
    SubscriptionIdsExhausted, // 订阅编号耗尽
};
pub const PublishError = error{Closed};
/// 接收订阅消息时可能返回的错误集合。
pub const RecvError = subscriptions.RecvError;

/// 一次发布中成功投递、队列已满和队列已关闭的订阅数量。
pub const PublishResult = struct {
    /// 成功写入队列的订阅数量。
    delivered: usize = 0,
    /// 队列已满、无法接收此消息的订阅数量。
    full: usize = 0,
    /// 队列已关闭、无法接收此消息的订阅数量。
    closed: usize = 0,
};

/// 根据消息 union 在编译期生成固定主题表，避免主题数量与消息定义分离。
pub fn Bus(comptime Message: type) type {
    const info = @typeInfo(Message);
    if (info != .@"union") @compileError("PubSub message type must be a union");
    const union_info = info.@"union";
    const Tag = union_info.tag_type orelse @compileError("PubSub message type must be union(enum)");
    const count = union_info.field_names.len;

    return struct {
        const Self = @This();
        /// 调用方提供的消息类型及其主题 tag。
        pub const MessageType = Message;
        pub const TopicTag = Tag;
        /// 编译期确定的主题数量。
        pub const topic_count = count;
        /// 每次订阅都会生成独立节点和队列，由调用方管理其生命周期。
        pub const Subscription = subscriptions.Subscription(Message, Tag, Self);
        /// 每个主题维护自己的订阅链表入口。
        pub const Topic = struct { subscribers: ?*Subscription.Node = null };

        /// 用于分配订阅节点及其专属队列缓冲区。
        allocator: Allocator,
        /// 保护主题路由和关闭状态；发布遍历持读锁，增删订阅持写锁。
        lock: Io.RwLock = .init,
        /// 标识发布者是否已关闭。
        closed: bool = false,
        /// 全局递增的订阅编号，防止旧句柄误匹配重新分配的节点。
        next_subscription_id: u64 = 0,
        /// 每个 union 变体对应一个独立的路由槽位。
        topics: [count]Topic = mem.zeroes([count]Topic),

        /// 创建初始状态为空的发布者。
        pub fn init(allocator: Allocator) Self {
            return .{ .allocator = allocator };
        }
        /// 关闭全部订阅队列并释放发布者持有的资源；重复调用没有效果。
        /// 发布者本身须保持有效，直到并发操作全部返回。
        pub fn deinit(self: *Self, io: Io) void {
            self.lock.lockUncancelable(io);
            defer self.lock.unlock(io);
            if (self.closed) return;
            self.closed = true;
            // 先关闭所有队列，让每个主题上的等待者都能立即被唤醒。
            for (&self.topics) |*topic| {
                var next = topic.subscribers;
                while (next) |node| : (next = node.next_for_topic) node.queue.close(io);
            }
            // 已登记的接收操作退出后才能释放其队列节点。
            for (&self.topics) |*topic| {
                while (topic.subscribers) |node| {
                    topic.subscribers = node.next_for_topic;
                    node.destroy(io);
                }
            }
        }
        /// 按 union 字段声明顺序映射主题索引，不依赖 tag 的数值。
        pub fn topicIndex(tag: Tag) usize {
            inline for (union_info.field_names, 0..) |name, index| {
                if (tag == @field(Tag, name)) return index;
            }
            unreachable;
        }
        /// 创建并挂接独立订阅；调用方通过 unsubscribe 取消并销毁它。
        pub fn subscribe(self: *Self, io: Io, tag: Tag, capacity: usize) SubscribeError!Subscription {
            const subscription = try self.allocator.create(Subscription.Node);
            errdefer self.allocator.destroy(subscription);
            const buffer = try self.allocator.alloc(Message, capacity);
            errdefer self.allocator.free(buffer);
            subscription.* = .{
                .topic = tag,
                .id = undefined,
                .allocator = self.allocator,
                .buffer = buffer,
                .queue = .init(buffer),
            };
            // 分配失败或等待锁被取消时，errdefer 会清理尚未挂接的节点。
            try self.lock.lock(io);
            defer self.lock.unlock(io);
            if (self.closed) return SubscribeError.Closed;
            const id = self.next_subscription_id;
            self.next_subscription_id = std.math.add(u64, id, 1) catch return SubscribeError.SubscriptionIdsExhausted;
            subscription.id = id;
            const topic = &self.topics[topicIndex(tag)];
            subscription.next_for_topic = topic.subscribers;
            topic.subscribers = subscription;
            return .{ .owner = self, .topic = tag, .id = id, .node = subscription };
        }
        /// 摘除并销毁订阅；重复取消同一订阅无效果。
        /// 关闭队列并唤醒并发接收者；发布者自身须保持有效。
        pub fn unsubscribe(self: *Self, io: Io, subscription: Subscription) void {
            // 句柄按值保存发布者及编号，即使节点已释放也不必解引用它。
            if (subscription.owner != self) return;
            self.lock.lockUncancelable(io);
            defer self.lock.unlock(io);
            var link = &self.topics[topicIndex(subscription.topic)].subscribers;
            while (link.*) |current| {
                if (current.id == subscription.id) {
                    link.* = current.next_for_topic;
                    current.next_for_topic = null;
                    current.state = .canceled;
                    // 写锁排除新接收和发布，destroy 关闭队列并回收节点。
                    current.destroy(io);
                    return;
                }
                link = &current.next_for_topic;
            }
        }
        /// 非阻塞地按值复制消息，并报告各队列的投递结果。
        /// 指针只复制地址，调用方须保证所引用的数据在接收期间有效。
        pub fn publish(self: *Self, io: Io, message: Message) PublishError!PublishResult {
            // 持读锁遍历，保证取消订阅不能同时摘除或释放当前节点。
            self.lock.lockSharedUncancelable(io);
            defer self.lock.unlockShared(io);
            if (self.closed) return PublishError.Closed;
            var result: PublishResult = .{};
            const topic = &self.topics[topicIndex(std.meta.activeTag(message))];
            var next = topic.subscribers;
            var helper = MessageHelper(Message).init(message);
            while (next) |subscription| : (next = subscription.next_for_topic) {
                helper.prepareShare(); // 准备分享消息
                const written = subscription.queue.putUncancelable(io, &.{message}, 0) catch {
                    helper.cancelShare(self.allocator); // 发送失败，取消分享消息
                    result.closed += 1;
                    continue;
                };
                if (written == 0) {
                    result.full += 1;
                } else {
                    result.delivered += 1;
                }
            }
            return result;
        }
    };
}

pub fn MessageHelper(comptime Message: type) type {
    return struct {
        message: Message,

        pub fn init(message: Message) @This() {
            return .{ .message = message };
        }

        pub fn prepareShare(self: *@This()) void {
            switch (self.message) {
                inline else => |payload| if (comptime rc.isAtomicShared(@TypeOf(payload))) {
                    payload.retain();
                },
            }
        }

        pub fn cancelShare(self: *@This(), allocator: Allocator) void {
            switch (self.message) {
                inline else => |payload| if (comptime rc.isAtomicShared(@TypeOf(payload))) {
                    payload.release(allocator);
                },
            }
        }
    };
}
