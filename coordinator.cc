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
    bool isMaster;
    std::time_t last_heartbeat;
    bool missed_heartbeat;
    int num_missed_heartbeat;
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

zNode* findNode(const std::vector<zNode*>& cluster, std::function<bool(zNode*)> predicate) {
    auto it = std::find_if(cluster.begin(), cluster.end(), predicate);
    return (it != cluster.end()) ? *it : nullptr;
}

zNode* createZNode(int serverId, const std::string& hostname, const std::string& port, const std::string& type, bool isMaster) {
    zNode* server = new zNode();
    server->serverID = serverId;
    server->hostname = hostname;
    server->port = port;
    server->type = type;
    server->last_heartbeat = getTimeNow();
    server->missed_heartbeat = false;
    server->num_missed_heartbeat = 0;
    server->isMaster = isMaster;
    return server;
}

class CoordServiceImpl final : public CoordService::Service {
    Status Heartbeat(ServerContext* context, const ServerInfo* serverInfo, Confirmation* confirmation) override {
        int serverId = serverInfo->serverid();
        int clusterid = serverInfo->clusterid();
        v_mutex.lock();
        std::vector<zNode*> cluster = clusters[clusterid - 1];
        zNode* node = findNode(cluster, [&](zNode* s) { return s->serverID == serverId && s->type == serverInfo->type(); });
        zNode* master = findNode(cluster, [&](zNode* s) { return s->type == serverInfo->type() && s->isMaster == true; });
        if (node == nullptr) {
            zNode* server = createZNode(serverId, serverInfo->hostname(), serverInfo->port(), serverInfo->type(), serverInfo->ismaster());
            if (master == nullptr) {
                server->isMaster = true;
                confirmation->set_status(true);
                confirmation->set_reconnect(false);
                clusters[clusterid - 1].push_back(server);
                log(INFO, "Master " + serverInfo->type() + " added in cluster " + std::to_string(clusterid));
            } else {
                server->isMaster = false;
                confirmation->set_status(false);
                confirmation->set_reconnect(false);
                clusters[clusterid - 1].push_back(server);
                log(INFO, "Slave " + serverInfo->type() + " added in cluster " + std::to_string(clusterid));
            }
        } else {
            confirmation->set_reconnect(true);
            // If we do have this node, then we update the last_heartbeat, and set confirmation, if it is a slave
            node->last_heartbeat = getTimeNow();
            node->missed_heartbeat = false;
            if (node->isMaster) {
                // node->isMaster = true;
                confirmation->set_status(true);
                node->num_missed_heartbeat = 0;
                log(INFO, "Master " + serverInfo->type() + " Heartbeat received in cluster " + std::to_string(clusterid) + " by " + std::to_string(serverId));
            } else {
                // node->isMaster = false;
                confirmation->set_status(false);
                log(INFO, "Slave " + serverInfo->type() + " Heartbeat received in cluster " + std::to_string(clusterid) + " by " + std::to_string(serverId));
                node->last_heartbeat = getTimeNow();
                confirmation->set_status(true);
                if (master->missed_heartbeat && master->num_missed_heartbeat >= 2) {
                    master->isMaster = false;
                    node->isMaster = true;
                    confirmation->set_status(false);
                    log(INFO, "Slave " + serverInfo->type() + " becomes Master in cluster " + std::to_string(clusterid) + " by " + std::to_string(serverId));
                }
            }
        }
        // if (serverInfo->type() == "server") {
        //     // If we do not have the node, we check if there is a master or slave, and accoridngly assign
        //     if (node == nullptr) {
        //         zNode* server = createZNode(serverId, serverInfo->hostname(), serverInfo->port(), serverInfo->type(), serverInfo->ismaster());
        //         if (master == nullptr) {
        //             server->isMaster = true;
        //             confirmation->set_status(true);
        //             clusters[clusterid - 1].push_back(server);
        //             log(INFO, "Master Server added in cluster " + std::to_string(clusterid));
        //         } else {
        //             server->isMaster = false;
        //             confirmation->set_status(false);
        //             clusters[clusterid - 1].push_back(server);
        //             log(INFO, "Slave Server added in cluster " + std::to_string(clusterid));
        //         }
        //     } else {
        //         // If we do have this node, then we update the last_heartbeat, and set confirmation, if it is a slave
        //         node->last_heartbeat = getTimeNow();
        //         node->missed_heartbeat = false;
        //         if (node->isMaster) {
        //             // node->isMaster = true;
        //             confirmation->set_status(true);
        //             node->num_missed_heartbeat = 0;
        //             log(INFO, "Master Heartbeat received in cluster " + std::to_string(clusterid) + " by " + std::to_string(serverId));
        //         } else {
        //             // node->isMaster = false;
        //             confirmation->set_status(false);
        //             log(INFO, "Slave Heartbeat received in cluster " + std::to_string(clusterid) + " by " + std::to_string(serverId));
        //             node->last_heartbeat = getTimeNow();
        //             confirmation->set_status(true);
        //             if (master->missed_heartbeat && master->num_missed_heartbeat >= 2) {
        //                 master->isMaster = false;
        //                 node->isMaster = true;
        //                 confirmation->set_status(false);
        //                 log(INFO, "Slave becomes Master in cluster " + std::to_string(clusterid) + " by " + std::to_string(serverId));
        //             }
        //         }
        //     }
        // } else if (serverInfo->type() == "synchronizer") {
        //     // zNode* server = findNode(cluster, [&](zNode* s) { return s->serverID == serverId && s->type == "synchronizer"; });
        //     if (node == nullptr) {
        //         zNode* server = createZNode(serverId, serverInfo->hostname(), serverInfo->port(), serverInfo->type(), serverInfo->ismaster());
        //         if (master == nullptr) {
        //             server->isMaster = true;
        //             confirmation->set_status(true);
        //             clusters[clusterid - 1].push_back(server);
        //             log(INFO, "Master Synchronizer added in cluster " + std::to_string(clusterid));
        //         } else {
        //             server->isMaster = false;
        //             confirmation->set_status(false);
        //             clusters[clusterid - 1].push_back(server);
        //             log(INFO, "Slave Synchronizer added in cluster " + std::to_string(clusterid));
        //         }
        //     } else {
        //         // If we do have this node, then we update the last_heartbeat, and set confirmation, if it is a slave
        //         node->last_heartbeat = getTimeNow();
        //         node->missed_heartbeat = false;
        //         if (node->isMaster) {
        //             confirmation->set_status(true);
        //             node->num_missed_heartbeat = 0;
        //             log(INFO, "Master Heartbeat received in cluster " + std::to_string(clusterid) + " by " + std::to_string(serverId));
        //         } else {
        //             confirmation->set_status(false);
        //             log(INFO, "Slave Heartbeat received in cluster " + std::to_string(clusterid) + " by " + std::to_string(serverId));
        //             node->last_heartbeat = getTimeNow();
        //             confirmation->set_status(true);
        //             if (master->missed_heartbeat && master->num_missed_heartbeat >= 2) {
        //                 master->isMaster = false;
        //                 node->isMaster = true;
        //                 confirmation->set_status(false);
        //                 log(INFO, "Slave becomes Master in cluster " + std::to_string(clusterid) + " by " + std::to_string(serverId));
        //             }
        //         }
        //         // // If we do have this node, then we update the last_heartbeat, and set confirmation, if it is a slave
        //         // zNode* server = findNode(cluster, [&](zNode* s) { return s->serverID == serverId && s->type == "server"; });
        //         // node->last_heartbeat = getTimeNow();
        //         // node->missed_heartbeat = false;
        //         // if (server->isMaster != node->isMaster) {
        //         //     // Roles have been altered
        //         //     confirmation->set_status(false);
        //         //     node->isMaster = server->isMaster;
        //         // } else {
        //         //     confirmation->set_status(true);
        //         //     log(INFO, "Synchronizer Heartbeat received in cluster " + std::to_string(clusterid));
        //         // }
        //     }
        // } else {
        //     log(ERROR, "Unknown server type: " + serverInfo->type());
        // }
        v_mutex.unlock();
        return Status::OK;
    }

    Status GetSlave(ServerContext* context, const ID* id, ServerInfo* serverInfo) override {
        log(INFO, "Get Slave Request in Cluster ID:\t" + std::to_string(id->id()));
        std::vector<zNode*> cluster = clusters[id->id() - 1];
        int clusterId = id->id();
        zNode* node = findNode(cluster, [&](zNode* s) {
            return s->type == "server" && s->isMaster == false;
        });
        if (node == nullptr) {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "Slave server not found");
        }
        serverInfo->set_hostname(node->hostname);
        serverInfo->set_port(node->port);
        log(INFO, "Slave Hostname:\t" + serverInfo->hostname() + "\tPort:\t" + serverInfo->port());
        return Status::OK;
    }

    Status GetServer(ServerContext* context, const ID* id, ServerInfo* serverInfo) override {
        log(INFO, "GetServer Request by Client ID:\t" + std::to_string(id->id()));
        int clientID = id->id();
        int clusterID = ((clientID - 1) % 3);
        std::vector<zNode*> cluster = clusters[clusterID];
        zNode* node = findNode(cluster, [&](zNode* s) {
            return s->type == "server" && s->isMaster == true;
        });
        if (node == nullptr) {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "Master server not found");
        }
        serverInfo->set_hostname(node->hostname);
        serverInfo->set_port(node->port);
        log(INFO, "Master found in " + std::to_string(clusterID) + "\tHostname:\t" + serverInfo->hostname() + "\tPort:\t" + serverInfo->port());
        return Status::OK;
    }

    Status GetAllFollowerServers(ServerContext* context, const ID* id, ServerList* serverList) override {
        for (const auto& cluster : clusters) {
            // For each server in the cluster, check if it's a follower synchronizer.
            for (zNode* node : cluster) {
                if (node->type == "synchronizer") {
                    // Add the follower synchronizer's details to the response.
                    serverList->add_serverid(node->serverID);
                    serverList->add_hostname(node->hostname);
                    serverList->add_port(node->port);
                    serverList->add_type(node->type);
                }
            }
        }
        // Log the success.
        log(INFO, "Found " + std::to_string(serverList->serverid_size()) + " follower synchronizers across all clusters.");
        return grpc::Status::OK;
    }

    // Status GetFollowerServer(ServerContext* context, const ID* id, ServerInfo* serverInfo) override {
    //     int clusterID = id->id();
    //     std::vector<zNode*> cluster = clusters[clusterID];
    //     zNode* node = findNode(cluster, [&](zNode* s) {
    //         return s->type == "server" && s->isMaster == true;
    //     });
    //     if (node == nullptr) {
    //         return grpc::Status(grpc::StatusCode::NOT_FOUND, "Master server not found");
    //     }
    //     serverInfo->set_hostname(node->hostname);
    //     serverInfo->set_port(node->port);
    //     log(INFO, "Master found in " + std::to_string(clusterID) + "\tHostname:\t" + serverInfo->hostname() + "\tPort:\t" + serverInfo->port());
    //     return Status::OK;
    // }
};

void RunServer(std::string port_no) {
    // start thread to check heartbeats
    std::thread hb(checkHeartbeat);
    // localhost = 127.0.0.1
    std::string server_address("127.0.0.1:" + port_no);
    CoordServiceImpl coordService;
    // grpc::EnableDefaultHealthCheckService(true);
    // grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&coordService);
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
                    s->num_missed_heartbeat++;
                    if (!s->missed_heartbeat) {
                        s->missed_heartbeat = true;
                    }
                } else {
                    s->num_missed_heartbeat = 0;
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
