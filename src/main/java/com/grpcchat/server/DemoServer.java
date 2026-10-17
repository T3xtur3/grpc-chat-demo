package com.grpcchat.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.util.Scanner;

/** Local integration smoke test: enter join/leave in server console to simulate TV2 Chat events. */
public class DemoServer {
    public static void main(String[] args) throws Exception {
        ChatState state = new ChatState();
        Server server = ServerBuilder.forPort(50051).addService(new ChatServiceImpl(state)).build().start();
        System.out.println("DemoServer on 50051. Type: join alice | leave alice | users | quit");
        try (Scanner scanner = new Scanner(System.in)) {
            while (scanner.hasNextLine()) {
                String[] command = scanner.nextLine().trim().split("\\s+", 2);
                if (command[0].equals("quit")) break;
                if (command.length == 2 && command[0].equals("join")) state.join("general", command[1], command[1]);
                else if (command.length == 2 && command[0].equals("leave")) state.leave("general", command[1]);
                else if (command[0].equals("users")) System.out.println(state.onlineUsers("general"));
                else System.out.println("Commands: join <name>, leave <name>, users, quit");
            }
        } finally { server.shutdownNow(); server.awaitTermination(); }
    }
}
