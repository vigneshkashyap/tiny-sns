#include <glog/logging.h>
#include <google/protobuf/duration.pb.h>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"

// #define DELIMITER "\x1F"

#define log(severity, msg) \
    LOG(severity) << msg;  \
    google::FlushLogFiles(google::severity);

using csce662::Confirmation;
using csce662::CoordService;
using csce662::ID;
using csce662::ServerInfo;
using csce662::ServerList;
using csce662::PathAndData;
// using csce662::Status;
using csce662::SynchService;
using google::protobuf::Duration;
using google::protobuf::Timestamp;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;

struct zNode {
    int serverID;
    std::string hostname;
    std::string port;
    std::string type;
    std::time_t last_heartbeat;
    bool missed_heartbeat;
    bool isActive();
};

// potentially thread safe
std::mutex v_mutex;
std::vector<zNode*> cluster1;
std::vector<zNode*> cluster2;
std::vector<zNode*> cluster3;
const char DELIMITER = '|';

// creating a vector of vectors containing znodes
std::vector<std::vector<zNode*>> clusters = {cluster1, cluster2, cluster3};

// func declarations
int findServer(std::vector<zNode*> v, int id);
std::time_t getTimeNow();
void checkHeartbeat();

bool zNode::isActive() {
    bool status = false;
    if (!missed_heartbeat) {
        status = true;
    } else if (difftime(getTimeNow(), last_heartbeat) < 10) {
        status = true;
    }
    return status;
}

std::vector<std::string> split(const std::string& s, char delim = DELIMITER) {
    std::vector<std::string> result;
    std::stringstream ss(s);
    std::string item;
    while (getline(ss, item, delim)) {
        result.push_back(item);
    }
    return result;
}

class CoordServiceImpl final : public CoordService::Service {
    Status Heartbeat(ServerContext* context, const ServerInfo* serverInfo, Confirmation* confirmation) override {
        std::lock_guard<std::mutex> lock(v_mutex);
        std::vector<std::string> port = split(serverInfo->port());
        int serverID = serverInfo->serverid();
        int clusterID = std::stoi(port[1]) - 1;
        int index = findServer(clusters[clusterID], serverID);
        log(INFO, "Server Index:\t" + index);
        if (index == -1) {
            log(WARNING, "Hearbeat Received!\t Server ID:\t" + std::to_string(serverID) + " not found in Cluster:\t" + std::to_string(clusterID));
            // Add newServer to a cluster (for simplicity, adding to cluster1)
            confirmation->set_status(false);  // Assuming Confirmation has a set_success() method
            return Status::OK;
        }
        clusters[clusterID][index]->last_heartbeat = getTimeNow();
        clusters[clusterID][index]->missed_heartbeat = false;
        log(INFO, "Hearbeat Received!\tServer ID:\t" + std::to_string(serverID));
        // Add newServer to a cluster (for simplicity, adding to cluster1)
        confirmation->set_status(true);  // Assuming Confirmation has a set_success() method
        return Status::OK;
    }

    // function returns the server information for requested client id
    // this function assumes there are always 3 clusters and has math
    // hardcoded to represent this.
    Status GetServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
        std::lock_guard<std::mutex> lock(v_mutex);
        log(INFO, "GetServer Request by Client ID:\t" + std::to_string(id->id()));
        int clientID = id->id();
        int clusterID = ((clientID - 1) % 3);
        std::cout<< "Cluster ID:\t" << clusterID << std::endl;
        int serverID = 1;
        int index = findServer(clusters[clusterID], serverID);
        std::cout<< "Index:\t" << index << std::endl;
        serverinfo->set_hostname(clusters[clusterID][index]->hostname);
        serverinfo->set_port(clusters[clusterID][index]->port);
        // Your code here
        return Status::OK;
    }

    Status create(ServerContext* context, const PathAndData* pathAndData, csce662::Status* status) override {
        std::lock_guard<std::mutex> lock(v_mutex);
        std::vector<std::string> path = split(pathAndData->path());
        std::vector<std::string> data = split(pathAndData->data());
        std::string hostname = path[0];
        std::string port = path[1];
        int clusterId = std::stoi(data[0]) - 1;
        int serverId = std::stoi(data[1]);
        std::cout <<"Cluster ID:\t" << clusterId << "\tServer ID:\t" << serverId << std::endl;
        int index = findServer(clusters[clusterId], serverId);
        std::cout <<"Index:\t" << index << std::endl;
        // If this serverId already exists in clusterId return grpc::NOT_POSSIBLE
        if (index != -1) {
            status->set_status(false);
            return Status::OK;
        }
        zNode* newServer = new zNode();
        newServer->serverID = serverId;  // Assuming ServerInfo has an id() method
        newServer->hostname = hostname;  // Assuming ServerInfo has a hostname() method
        newServer->port = port;          // Assuming ServerInfo has a port() method
        newServer->last_heartbeat = getTimeNow();
        newServer->missed_heartbeat = false;
        clusters[clusterId].push_back(newServer);  // Adjust as needed
        log(INFO, "Server Added!\t Server ID:\t" + std::to_string(serverId) + " to cluster ID:\t" + std::to_string(clusterId));
        status->set_status(true);
        return Status::OK;
    }

};

void RunServer(std::string port_no) {
    // start thread to check heartbeats
    std::thread hb(checkHeartbeat);
    // localhost = 127.0.0.1
    std::string server_address("127.0.0.1:" + port_no);
    CoordServiceImpl service;
    // grpc::EnableDefaultHealthCheckService(true);
    // grpc::reflection::InitProtoReflectionServerBuilderPlugin();
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
    while ((opt = getopt(argc, argv, "p:")) != -1) {
        switch (opt) {
            case 'p':
                port = optarg;
                break;
            default:
                std::cerr << "Invalid Command Line Argument\n";
        }
    }
    std::string log_file_name = std::string("coordinator-port-") + port;
    google::InitGoogleLogging(log_file_name.c_str());
    RunServer(port);
    return 0;
}

int findServer(std::vector<zNode*> v, int id) {
    //    v_mutex.lock();
    return v.size() > 0? 0 : -1;
    // // iterating through the clusters vector of vectors of znodes
    // for (int i = 0; i < v.size(); i++) {
    //     if (v[i]->serverID == id) {
    //         // std::cout << "missed heartbeat from server " << s->serverID << std::endl;
    //         return 0;
    //     }
    // }

    // // v_mutex.unlock();
    // return -1;
}

void checkHeartbeat() {
    while (true) {
        // check servers for heartbeat > 10
        // if true turn missed heartbeat = true
        //  Your code below

        v_mutex.lock();

        // iterating through the clusters vector of vectors of znodes
        for (auto& c : clusters) {
            for (auto& s : c) {
                if (s == nullptr) {
                    log(WARNING, "Encountered null server pointer, skipping...");
                    continue;  // Skip if the server pointer is null
                }
                if (difftime(getTimeNow(), s->last_heartbeat) > 10) {
                    log(WARNING, "Missed Heartbeat from server\t" + std::to_string(s->serverID));
                    if (!s->missed_heartbeat) {
                        s->missed_heartbeat = true;
                        s->last_heartbeat = getTimeNow();
                    }
                }
            }
        }

        v_mutex.unlock();

        sleep(3);
    }
}

std::time_t getTimeNow() {
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}
