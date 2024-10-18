#include <algorithm>
#include <cstdio>
#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <sys/stat.h>
#include <sys/types.h>
#include <utility>
#include <vector>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include<glog/logging.h>

#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce662::CoordService;
using csce662::ServerInfo;
using csce662::Confirmation;
using csce662::ID;
using csce662::ServerList;
using csce662::SynchService;

#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

struct zNode{
    int serverID;
    std::string hostname;
    std::string port;
    std::string type;
    std::time_t last_heartbeat;
    bool missed_heartbeat;
    bool isActive();

};

//potentially thread safe 
std::mutex v_mutex;
std::vector<zNode*> cluster1;
std::vector<zNode*> cluster2;
std::vector<zNode*> cluster3;

// creating a vector of vectors containing znodes
std::vector<std::vector<zNode*>> clusters = {cluster1, cluster2, cluster3};


//func declarations
int findServer(std::vector<zNode*> v, int id); 
std::time_t getTimeNow();
void checkHeartbeat();


bool zNode::isActive(){
    bool status = false;
    if(!missed_heartbeat){
        status = true;
    }else if(difftime(getTimeNow(),last_heartbeat) < 10){
        status = true;
    }
    return status;
}


class CoordServiceImpl final : public CoordService::Service {

Status Heartbeat(ServerContext* context, const ServerInfo* serverinfo, Confirmation* confirmation) override {
  std::lock_guard<std::mutex> lock(v_mutex);

    // Use the correct field names from your .proto file
    int server_id = serverinfo->serverid();
    std::string server_ip = serverinfo->hostname();
    std::string server_port = serverinfo->port();
    std::string type = serverinfo->type(); // If needed

    // Since 'cluster_id' is not defined, you might need to extract it from 'type' or adjust your .proto file
    int cluster_id = extractClusterId(type);
        // Validate cluster ID
        if (cluster_id < 1 || cluster_id > 3) {
            return Status(grpc::INVALID_ARGUMENT, "Invalid cluster ID");
        }

        // Find the corresponding cluster vector
        std::vector<zNode*>& cluster = clusters[cluster_id - 1];

        // Find or add the server in the cluster
        int index = findServer(cluster, server_id);
        if (index == -1) {
            // Server not found, add it
            zNode* new_server = new zNode();
            new_server->serverID = server_id;
            new_server->hostname = server_ip;
            new_server->port = server_port;
            new_server->type = "Server";
            new_server->last_heartbeat = getTimeNow();
            new_server->missed_heartbeat = false;
            cluster.push_back(new_server);

            std::cout << "Registered new server: Cluster " << cluster_id << ", Server " << server_id << std::endl;
        } else {
            // Update existing server's heartbeat time
            zNode* server = cluster[index];
            server->last_heartbeat = getTimeNow();
            server->missed_heartbeat = false;
            // std::cout << "Heartbeat received from Server " << server_id << " in Cluster " << cluster_id << std::endl;
        }
        confirmation->set_status(true);
        log(INFO, "Heartbeat received from server " << server_id);
        return Status::OK;
    }

    Status GetServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
    int client_id = id->id(); // Use 'id()' method

    int cluster_id = ((client_id - 1) % 3) + 1;
    int server_id = 1; // Assuming server IDs start from 1

        std::lock_guard<std::mutex> lock(v_mutex);

        // Get the cluster
        std::vector<zNode*>& cluster = clusters[cluster_id - 1];

        // Find the server in the cluster
        int index = findServer(cluster, server_id);
        if (index == -1) {
            return Status(grpc::NOT_FOUND, "Server not found");
        }

        zNode* server = cluster[index];

        // Check if the server is active
        if (!server->isActive()) {
            return Status(grpc::UNAVAILABLE, "Server is not active");
        }

        // Set the server info to return to the client
serverinfo->set_serverid(server->serverID);
    serverinfo->set_hostname(server->hostname);
    serverinfo->set_port(server->port);
    serverinfo->set_type("Cluster" + std::to_string(cluster_id));

        std::cout << "Client " << client_id << " assigned to Server " << server->serverID
                  << " in Cluster " << cluster_id << std::endl;

        return Status::OK;
    }

    std::thread heartbeat_thread_;
    int extractClusterId(const std::string& type) {
        // Assuming 'type' is formatted as "ClusterX" where X is the cluster ID
        if (type.find("Cluster") == 0) {
            return std::stoi(type.substr(7));
        }
        // Default or error handling
        return -1;
    }
   int findServer(const std::vector<zNode*>& v, int id) {
        for (size_t i = 0; i < v.size(); ++i) {
            if (v[i]->serverID == id) {
                return i;
            }
        }
        return -1;
    }

};


void checkHeartbeat() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(3));

        std::lock_guard<std::mutex> lock(v_mutex);

        for (auto& cluster : clusters) {
            for (auto& server : cluster) {
                if (difftime(getTimeNow(), server->last_heartbeat) > 10) {
                    if (!server->missed_heartbeat) {
                        server->missed_heartbeat = true;
                        std::cout << "Server " << server->serverID << " in Cluster "
                                    << ((server->serverID - 1) % 3) + 1 << " missed heartbeat." << std::endl;
                    }
                }
            }
        }
    }
}
void RunServer(std::string port_no){
    //start thread to check heartbeats
    std::thread hb(checkHeartbeat);
    //localhost = 127.0.0.1
    std::string server_address("0.0.0.0:"+port_no);
    CoordServiceImpl service;
    //grpc::EnableDefaultHealthCheckService(true);
    //grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}

int main(int argc, char** argv) {
    std::string port = "3010";
    int opt = 0;
    while ((opt = getopt(argc, argv, "p:")) != -1){
    switch(opt) {
        case 'p':
            port = optarg;
            break;
        default:
            std::cerr << "Invalid Command Line Argument\n";
            return 1;
        }
    }
    RunServer(port);
    return 0;
}

std::time_t getTimeNow(){
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

