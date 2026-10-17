package com.grpcchat.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;

public class GrpcServer {
    public static void main(String[] args) throws Exception {
        ChatState state = new ChatState();
        Server server = ServerBuilder.forPort(50051)
                .addService(new ChatServiceImpl(state))
                .build().start();
        System.out.println("gRPC Server started on port 50051");
        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));
        server.awaitTermination();
    }
}
