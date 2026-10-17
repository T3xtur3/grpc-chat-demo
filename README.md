# gRPC Chat Java – TV1 shared state demo

Java 17 / Maven. Same `chat.proto` as starter. `ChatServiceImpl` implements TV1 Unary and Server Streaming. TV2 will implement Client/Bidirectional, using the same `ChatState` instance.

## Run in IntelliJ
1. Maven > Lifecycle > compile (or `mvn clean compile` if Maven is in PATH).
2. Run `com.grpcchat.server.DemoServer` (NOT `GrpcServer` for this interactive test). **Stop any previous GrpcServer first**: only one server can bind 50051.
3. Run `com.grpcchat.client.BasicClient` separately while DemoServer stays open.
4. In **DemoServer console**, type `join alice`, `join bob`, `leave alice`, `users` one command per line. In BasicClient, events arrive live. Re-run BasicClient to query fresh online users (the Unary runs once at startup).
5. Stop DemoServer by typing `quit`.

## TV2 contract
- Use one shared `ChatState` per server instance; inject it into `ChatServiceImpl`.
- On first `ChatMessage` with `type = JOIN`: call `state.join(roomId, userId, username)`.
- On explicit `LEAVE` or RPC cancellation/termination: call `state.leave(roomId, userId)` once.
- `getOnlineUsers` reads `state.onlineUsers(roomId)`.
- `watchNotifications` subscribes to join/leave events occurring after subscription. It stays open; callers cancel or set a deadline.
- `userId` must uniquely identify a chat session; for this initial version, reconnecting should create a new session ID.
- This is in-memory, single-server state. Load balancing across two independent servers does **not** synchronize it; TV3 should test balancing using separate Unary backend IDs or explicitly plan shared state.
- Streaming subscriber uses bounded queue (256 events) and its own worker thread: local teaching demo only, not a production-scale dispatcher. Streams may drop/disconnect on overload, and no message history is kept.

## Shared proto
`src/main/proto/chat.proto` is unchanged. Do not edit generated Java files in `target`.
