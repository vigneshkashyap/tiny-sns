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

class CoordServiceImpl final : public CoordService::Service {
    Status Heartbeat(ServerContext* context, const ServerInfo* serverInfo, Confirmation* confirmation) override {
        int serverId = serverInfo->serverid();
        int clusterid = serverInfo->clusterid();
        v_mutex.lock();
        std::vector<zNode*> cluster = clusters[clusterid - 1];
        zNode *server = new zNode();
        server->serverID = serverId;
        server->hostname = serverInfo->hostname();
        server->port = serverInfo->port();
        server->type = serverInfo->type();
        server->last_heartbeat = getTimeNow();
        server->missed_heartbeat = false;
        if (cluster.size() == 0) {
            server->type = "master";
            confirmation->set_status(true);
            clusters[clusterid - 1].push_back(server);
            log(INFO, "Master added");
        } else if (cluster.size() == 1 && clusters[clusterid - 1][0]->serverID != serverId) {
            server->type = "slave";
            confirmation->set_status(false);
            clusters[clusterid - 1].push_back(server);
            log(INFO, "Slave added");
        } else {
            zNode *currServer = clusters[clusterid - 1][serverId - 1];
            // Heartbeat
            if (server->type == std::string("master")) {
                confirmation->set_status(true);
                currServer->last_heartbeat = server->last_heartbeat;
                currServer->missed_heartbeat = server->missed_heartbeat;
                // We have received a heartbeat from master
                // log(INFO, "Master Heartbeat:\t" + std::to_string(currServer->last_heartbeat));
                // log(INFO, "Master heartbeat" + currServer->last_heartbeat);
            } else {
                // Slave has sent heartbeat
                // Check if Master has missedHeartbeat, then this becomes the master, and that becomes slave
                int masterServerID = serverId == 1? 1 : 0;
                zNode* master = clusters[clusterid - 1][masterServerID];
                currServer->last_heartbeat = server->last_heartbeat;
                currServer->missed_heartbeat = server->missed_heartbeat;
                confirmation->set_status(true);
                if (master->missed_heartbeat) {
                    master->type = "slave";
                    currServer->type = "master";
                    confirmation->set_status(false);
                    log(INFO, "Slave becomes Master:\t");
                }
            }
        }
        v_mutex.unlock();
        return Status::OK;
    }

    Status GetSlave (ServerContext* context, const ID* id, ServerInfo* serverInfo) override {
        log(INFO, "Get Slave Request by Server ID:\t" + std::to_string(id->id()));
        std::vector<zNode*> cluster = clusters[id->id() - 1];
        int clusterId = id->id();
        for (auto server: cluster) {
            if (server->type == std::string("slave")) {
                serverInfo->set_hostname(server->hostname);
                serverInfo->set_port(server->port);
                log(INFO, "Slave Hostname:\t" + serverInfo->hostname() + "\tPort:\t" + serverInfo->port());
                return Status::OK;
            }
        }
        return grpc::Status(grpc::StatusCode::NOT_FOUND, "Slave server not found");
    }

    Status GetServer(ServerContext* context, const ID* id, ServerInfo* serverInfo) override {
        log(INFO, "GetServer Request by Client ID:\t" + std::to_string(id->id()));
        int clientID = id->id();
        int clusterID = ((clientID - 1) % 3);
        std::vector<zNode*> cluster = clusters[clusterID];
        std::string hostname = "";
        std::string port = "";
        for (auto server : cluster) {
            if (server->type == std::string("master")) {
                hostname = server->hostname;
                port = server->port;
            }
        }
        serverInfo->set_hostname(hostname);
        serverInfo->set_port(port);
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
    v_mutex.lock();
    cluster1.clear();
    cluster2.clear();
    cluster3.clear();
    v_mutex.unlock();
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
    v_mutex.lock();
    int result = v.size() > 0 ? 0 : -1;
    v_mutex.unlock();
    return result;
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
                if (difftime(getTimeNow(), s->last_heartbeat) > 15) {
                    log(WARNING, "Missed Heartbeat from server\t" + std::to_string(s->serverID));
                    if (!s->missed_heartbeat) {
                        s->missed_heartbeat = true;
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
