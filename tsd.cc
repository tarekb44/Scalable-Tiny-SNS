/*
 *
 * Copyright 2015, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <ctime>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <glog/logging.h>
#include <filesystem>
#include <chrono>
#include <thread>
#include <mutex>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 
#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"  // Include coordinator gRPC definitions
#include "coordinator.pb.h"
using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using csce662::Message;
using csce662::ListReply;
using csce662::Request;
using csce662::Reply;
using csce662::SNSService;
using csce662::CoordService;    // Coordinator service
using csce662::ServerInfo;
using csce662::Confirmation;
namespace fs = std::filesystem;

struct Client {
    std::string username;
    bool connected = true;
    ServerReaderWriter<Message, Message>* stream = nullptr;
    bool operator==(const Client& c1) const {
        return (username == c1.username);
    }
};


std::time_t getTimeNow() {
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

class SNSServiceImpl final : public SNSService::Service {
public:
    SNSServiceImpl(const std::string& server_dir)
        : server_dir_(server_dir) {
        monitor_thread_ = std::thread(&SNSServiceImpl::monitorTimelines, this);
        monitor_thread_.detach();
    }
    ~SNSServiceImpl() {
        if (monitor_thread_.joinable()) {
            monitor_thread_.join();
        }
    }

    Status List(ServerContext* context, const Request* request, ListReply* list_reply) override {
        log(INFO, "Serving List Request from: " + request->username());
        std::lock_guard<std::mutex> lock(client_db_mutex_);
        for (const auto& client_pair : client_db_) {
            list_reply->add_all_users(client_pair.first);
        }
        return Status::OK;
    }
    Status Follow(ServerContext* context, const Request* request, Reply* reply) override {
        return Status::OK;
    }

    Status UnFollow(ServerContext* context, const Request* request, Reply* reply) override {
        return Status::OK;
    }

    Status Login(ServerContext* context, const Request* request, Reply* reply) override {
        std::string username = request->username();
        log(INFO, "Serving Login Request: " + username);
        std::lock_guard<std::mutex> lock(client_db_mutex_);
        if (client_db_.find(username) == client_db_.end()) {
            // New client
            Client* new_client = new Client();
            new_client->username = username;
            client_db_[username] = new_client;
            reply->set_msg("Login Successful!");
        } else {
            // Existing client
            Client* existing_client = client_db_[username];
            if (existing_client->connected) {
                reply->set_msg("you have already joined");
            } else {
                existing_client->connected = true;
                reply->set_msg("Welcome Back " + username);
            }
        }
        return Status::OK;
    }


    Status Timeline(ServerContext* context, ServerReaderWriter<Message, Message>* stream) override {
        log(INFO, "Serving Timeline Request");
        Message message;
        Client* client = nullptr;
        while (stream->Read(&message)) {
            std::string username = message.username();
            {
                std::lock_guard<std::mutex> lock(client_db_mutex_);
                client = client_db_[username];
                if (client->stream == nullptr) {
                    client->stream = stream;
                }
            }

            std::string filename = server_dir_ + "/" + username + ".txt";

            if (message.msg() != "Set Stream") {
                std::ofstream user_file(filename, std::ios::app);
                if (user_file.is_open()) {
                    std::string time = google::protobuf::util::TimeUtil::ToString(message.timestamp());
                    user_file << "T " << time << "\n";
                    user_file << "U " << username << "\n";
                    user_file << "W " << message.msg() << "\n\n";
                }
            } else {
                sendLatestPosts(client);
            }
        }

        std::lock_guard<std::mutex> lock(client_db_mutex_);
        if (client) {
            client->connected = false;
            client->stream = nullptr;
        }
        return Status::OK;
    }
private:
    std::string server_dir_;
    std::map<std::string, Client*> client_db_;
    std::mutex client_db_mutex_;
    std::thread monitor_thread_;

    void monitorTimelines() {
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(5));
            std::lock_guard<std::mutex> lock(client_db_mutex_);
            for (const auto& client_pair : client_db_) {
                Client* client = client_pair.second;
                if (client->connected && client->stream != nullptr) {
                    std::string filename = server_dir_ + "/" + client->username + ".txt";
                    if (fs::exists(filename)) {
                        auto last_write_time = fs::last_write_time(filename);
                        auto current_time = fs::file_time_type::clock::now();
                        auto duration = current_time - last_write_time;
                        auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration).count();
                        if (seconds <= 30) {
                            sendLatestPosts(client);
                        }
                    }
                }
            }
        }
    }

    void sendLatestPosts(Client* client) {
        std::string filename = server_dir_ + "/" + client->username + ".txt";
        std::ifstream user_file(filename);
        if (!user_file.is_open()) {
            return;
        }
        std::vector<std::string> lines;
        std::string line;
        std::string post;
        while (std::getline(user_file, line)) {
            if (line.empty()) {
                lines.push_back(post);
                post.clear();
            } else {
                post += line + "\n";
            }
        }
        if (!post.empty()) {
            lines.push_back(post);
        }
        int start = std::max(0, static_cast<int>(lines.size()) - 20);
        for (int i = start; i < lines.size(); ++i) {
            Message msg;
            msg.set_msg(lines[i]);
            client->stream->Write(msg);
        }
    }
};

void sendHeartbeat(const std::string& coordinator_ip, const std::string& coordinator_port,
                   const std::string& cluster_id, const std::string& server_id, const std::string& port) {
    std::string coord_address = coordinator_ip + ":" + coordinator_port;

    auto coord_stub = csce662::CoordService::NewStub(grpc::CreateChannel(coord_address, grpc::InsecureChannelCredentials()));
    csce662::ServerInfo server_info;
    server_info.set_serverid(std::stoi(server_id));
    server_info.set_hostname("localhost");
    server_info.set_port(port);
    server_info.set_type("Cluster" + cluster_id);

    while (true) {
        grpc::ClientContext context;
        csce662::Confirmation confirm;
        grpc::Status status = coord_stub->Heartbeat(&context, server_info, &confirm);
        if (!status.ok()) {
            std::cerr << "Failed to send heartbeat to coordinator: " << status.error_message() << std::endl;
        } else {
            log(INFO, "Heartbeat sent to coordinator.");
        }
        std::this_thread::sleep_for(std::chrono::seconds(5));  // Send heartbeat every 5 seconds
    }
}


void RunServer(const std::string& port_no, const std::string& server_dir) {
    std::string server_address = "0.0.0.0:" + port_no;
    SNSServiceImpl service(server_dir);
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    log(INFO, "Server listening on " + server_address);
    server->Wait();
}

int main(int argc, char** argv) {
    std::string coordinator_ip = "localhost";
    std::string port = "3010";
    std::string server_id = "1";
    std::string cluster_id = "1";
    std::string coordinator_port = "9090";
    int opt = 0;
    while ((opt = getopt(argc, argv, "c:s:h:k:p:")) != -1) {
        switch (opt) {
            case 'c':
                cluster_id = optarg;
                break;
            case 's':
                server_id = optarg;
                break;
            case 'h':
                coordinator_ip = optarg;
                break;
            case 'k':
                coordinator_port = optarg;
                break;
            case 'p':
                port = optarg;
                break;
            default:
                std::cerr << "Invalid Command Line Argument\n";
                return 1;
        }
    }
    // Create the server directory
    std::string server_dir = "server_" + cluster_id + "_" + server_id;
    fs::create_directories(server_dir);
    // Initialize logging
    std::string log_file_name = "server-" + port;
    log(INFO, "Logging Initialized. Server starting...");
    // Start the heartbeat thread
    std::thread heartbeat_thread(sendHeartbeat, coordinator_ip, coordinator_port, cluster_id, server_id, port);
    heartbeat_thread.detach();  // Run independently
    // Start the server
    RunServer(port, server_dir);
    return 0;
}