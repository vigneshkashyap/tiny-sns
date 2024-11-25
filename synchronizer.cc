// NOTE: This starter code contains a primitive implementation using the default RabbitMQ protocol.
// You are recommended to look into how to make the communication more efficient,
// for example, modifying the type of exchange that publishes to one or more queues, or
// throttling how often a process consumes messages from a queue so other consumers are not starved for messages
// All the functions in this implementation are just suggestions and you can make reasonable changes as long as
// you continue to use the communication methods that the assignment requires between different processes

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <bits/fs_fwd.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <google/protobuf/duration.pb.h>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <jsoncpp/json/json.h>
#include <jsoncpp/json/reader.h>
#include <jsoncpp/json/writer.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"
#include "sns.grpc.pb.h"
#include "sns.pb.h"

#define log(severity, msg) \
    LOG(severity) << msg;  \
    google::FlushLogFiles(google::severity);

namespace fs = std::filesystem;

using csce662::AllUsers;
using csce662::Confirmation;
using csce662::CoordService;
using csce662::ID;
using csce662::ServerInfo;
using csce662::ServerList;
using csce662::SynchronizerListReply;
using csce662::SynchService;
using google::protobuf::Duration;
using google::protobuf::Timestamp;
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
// tl = timeline, fl = follow list
using csce662::TLFL;

int synchID = 1;
int clusterID = 1;
bool isMaster = true;
int total_number_of_registered_synchronizers = 6;  // update this by asking coordinator
std::string coordAddr;
std::string clusterSubdirectory;
std::vector<std::string> otherHosts;
std::unordered_map<std::string, int> timelineLengths;
std::set<std::pair<std::string, std::string>> publishedMessages;

std::vector<std::string> get_lines_from_file(std::string);
std::vector<std::string> get_all_users_func(int);
std::vector<std::string> get_tl_or_fl(int, int, bool);
std::vector<std::string> getFollowersOfUser(int);
bool file_contains_user(std::string filename, std::string user);

void Heartbeat(std::string coordinatorIp, std::string coordinatorPort, ServerInfo serverInfo, int syncID);

std::unique_ptr<csce662::CoordService::Stub> coordinator_stub_;

class SynchronizerRabbitMQ {
   private:
    amqp_connection_state_t conn;
    amqp_channel_t channel;
    std::string hostname;
    int port;
    int synchID;

    void setupRabbitMQ() {
        conn = amqp_new_connection();
        amqp_socket_t *socket = amqp_tcp_socket_new(conn);
        amqp_socket_open(socket, hostname.c_str(), port);
        amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest");
        amqp_channel_open(conn, channel);
    }

    void declareQueue(const std::string &queueName) {
        amqp_queue_declare(conn, channel, amqp_cstring_bytes(queueName.c_str()),
                           0, 0, 0, 0, amqp_empty_table);
    }

    void publishMessage(const std::string &queueName, const std::string &message) {
        if (message.empty() || message == "null") {
            return;  // Skip publishing if the message is invalid
        }
        // std::cerr << "[Publisher] Message published to queue: " << queueName << "\nMessage Content: " << message << std::endl;
        amqp_basic_publish(conn, channel, amqp_empty_bytes, amqp_cstring_bytes(queueName.c_str()),
                           0, 0, NULL, amqp_cstring_bytes(message.c_str()));
    }

    std::string consumeMessage(const std::string &queueName, int timeout_ms = 5000) {
        amqp_basic_consume(conn, channel, amqp_cstring_bytes(queueName.c_str()),
                           amqp_empty_bytes, 0, 1, 0, amqp_empty_table);

        amqp_envelope_t envelope;
        amqp_maybe_release_buffers(conn);

        struct timeval timeout;
        timeout.tv_sec = timeout_ms / 1000;
        timeout.tv_usec = (timeout_ms % 1000) * 1000;

        amqp_rpc_reply_t res = amqp_consume_message(conn, &envelope, &timeout, 0);

        if (res.reply_type != AMQP_RESPONSE_NORMAL) {
            return "";
        }

        std::string message(static_cast<char *>(envelope.message.body.bytes), envelope.message.body.len);
        // std::cerr << "[Consumer] Message received from queue: " << queueName<< "\nMessage Content: " << message << std::endl;
        // amqp_basic_ack(conn, channel, envelope.delivery_tag, 0);
        amqp_destroy_envelope(&envelope);
        if (message.empty() || message == "null") {
            return "";  // Return an empty string to indicate invalid message
        }
        return message;
    }

   public:
    SynchronizerRabbitMQ(const std::string &host, int p, int id) : hostname(host), port(p), channel(1), synchID(id) {
        setupRabbitMQ();
        declareQueue("synch" + std::to_string(synchID) + "_users_queue");
        declareQueue("synch" + std::to_string(synchID) + "_clients_relations_queue");
        declareQueue("synch" + std::to_string(synchID) + "_timeline_queue");
        // TODO: add or modify what kind of queues exist in your clusters based on your needs
    }

    void publishUserList() {
        std::vector<std::string> users = get_all_users_func(synchID);
        std::sort(users.begin(), users.end());
        Json::Value userList;
        for (const auto &user : users) {
            userList["users"].append(user);
        }
        if (userList["users"].empty()) {
            // std::cerr << "User list is empty. Skipping publish." << std::endl;
            return;
        }
        Json::StreamWriterBuilder writer;
        std::string message = Json::writeString(writer, userList);
        for (int i = 1; i <= total_number_of_registered_synchronizers; i++) {
            if (i == synchID) {
                continue;
            }
            std::string queueName = "synch" + std::to_string(i) + "_users_queue";
            publishMessage(queueName, message);
        }
        // std::string queueName = "synch" + std::to_string(synchID) + "_users_queue";
    }

    void consumeQueue(std::string queueType) {
            std::string queueName = "synch" + std::to_string(synchID) + queueType;
            std::string message = consumeMessage(queueName, 1000);  // 1 second timeout
            if (message.empty()) {
                return;
            }
            // Parse the JSON message
            Json::CharReaderBuilder readerBuilder;
            std::unique_ptr<Json::CharReader> reader(readerBuilder.newCharReader());
            Json::Value root;
            std::string errs;

            bool parsingSuccessful = reader->parse(message.c_str(), message.c_str() + message.size(), &root, &errs);
            if (!parsingSuccessful) {
                // std::cerr << "Failed to parse message: " << errs << std::endl;
                return;
            }
            // Ensure the message is a JSON object
            if (!root.isObject()) {
                // std::cerr << "Invalid JSON format. Expected an object but got something else." << std::endl;
                return;
            }

            // Get the first key
            auto memberNames = root.getMemberNames();
            if (memberNames.empty()) {
                // std::cerr << "JSON object has no keys. Skipping message." << std::endl;
                return;
            }

            std::string firstKey = memberNames[0];
            // std::cerr << "Processing message with first key: " << firstKey << std::endl;

            // Call the appropriate method based on the first key
            if (firstKey == "users") {
                // std::cerr << "We got users";
                consumeUserLists(root);
            } else if (firstKey == "userRelationship") {
                // std::cerr << "We got user relationship";
                consumeClientRelations(root[firstKey]);
            } else if (firstKey == "timeline") {
                // std::cerr << "We got user timeline";
                consumeTimeline(root[firstKey]);
            } else {
                // std::cerr << "Unknown first key: " << firstKey << ". Skipping message." << std::endl;
            }
    }

    void consumeUserLists(Json::Value root) {
        std::vector<std::string> allUsers;
        // YOUR CODE HERE

        // TODO: while the number of synchronizers is harcorded as 6 right now, you need to change this
        // to use the correct number of follower synchronizers that exist overall
        // accomplish this by making a gRPC request to the coordinator asking for the list of all follower synchronizers registered with it
        // for (int i = 1; i <= total_number_of_registered_synchronizers; i++) {
        //     std::string queueName = "synch" + std::to_string(i) + "_users_queue";
        //     std::string message = consumeMessage(queueName, 1000);  // 1 second timeout
        // if (!message.empty()) {
        // Json::CharReaderBuilder readerBuilder;
        // std::unique_ptr<Json::CharReader> reader(readerBuilder.newCharReader());
        // Json::Value root;
        // std::string errs;
        // bool parsingSuccessful = reader->parse(message.c_str(), message.c_str() + message.size(), &root, &errs);
        // if (!parsingSuccessful) {
        //     continue;
        // }
        for (const auto &user : root["users"]) {
            allUsers.push_back(user.asString());
        }
        // }
        // }
        updateAllUsersFile(allUsers);
    }

    void publishClientRelations() {
        Json::Value relations;
        std::vector<std::string> users = get_all_users_func(synchID);

        for (const auto &client : users) {
            int clientId = std::stoi(client);
            std::vector<std::string> followers = getFollowersOfUser(clientId);

            Json::Value followerList(Json::arrayValue);
            for (const auto &follower : followers) {
                followerList.append(follower);
            }

            if (!followerList.empty()) {
                relations[client] = followerList;
            }
        }
        // Check if relations is empty
        if (relations.empty()) {
            // std::cerr << "Client relations are empty. Skipping publish." << std::endl;
            return;
        }
        Json::Value root;
        root["userRelationship"] = relations;
        Json::StreamWriterBuilder writer;
        std::string message = Json::writeString(writer, root);
        // std::string queueName = "synch" + std::to_string(synchID) + "_clients_relations_queue";
        // publishMessage(queueName, message);
        for (int i = 1; i <= total_number_of_registered_synchronizers; i++) {
            if (i == synchID) {
                continue;
            }
            std::string queueName = "synch" + std::to_string(i) + "_users_queue";
            publishMessage(queueName, message);
        }
        // std::string queueName = "synch" + std::to_string(i) + "_clients_relations_queue";
    }

    void consumeClientRelations(Json::Value root) {
        std::vector<std::string> allUsers = get_all_users_func(synchID);

        // YOUR CODE HERE

        // TODO: hardcoding 6 here, but you need to get list of all synchronizers from coordinator as before
        // for (int i = 1; i <= total_number_of_registered_synchronizers; i++) {
        // std::string queueName = "synch" + std::to_string(i) + "_clients_relations_queue";
        // std::string message = consumeMessage(queueName, 1000);  // 1 second timeout
        // if (!message.empty()) {
        // Json::CharReaderBuilder readerBuilder;
        // std::unique_ptr<Json::CharReader> reader(readerBuilder.newCharReader());
        // Json::Value root;
        // std::string errs;
        // bool parsingSuccessful = reader->parse(message.c_str(), message.c_str() + message.size(), &root, &errs);
        // if (!parsingSuccessful) {
        // continue;
        // }
        for (const auto &client : allUsers) {
            // std::string followerFile = "./cluster_" + std::to_string(clusterID) + "/" + clusterSubdirectory + "/" + client + "_followers.txt";
            std::string followerFile = "./cluster" + std::to_string(clusterID) + "/" + clusterSubdirectory + "/" + client + "_followers.txt";
            std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_" + client + "_followers.txt";
            sem_t *fileSem = sem_open(semName.c_str(), O_CREAT);

            std::ofstream followerStream(followerFile, std::ios::app | std::ios::out | std::ios::in);
            if (root.isMember(client)) {
                for (const auto &follower : root[client]) {
                    if (!file_contains_user(followerFile, follower.asString())) {
                        followerStream << follower.asString() << std::endl;
                    }
                }
            }
            sem_close(fileSem);
        }
        // }
        // }
    }

    // for every client in your cluster, update all their followers' timeline files
    // by publishing your user's timeline file (or just the new updates in them)
    //  periodically to the message queue of the synchronizer responsible for that client
    void publishTimelines() {
        std::vector<std::string> users = get_all_users_func(synchID);

        for (const auto &client : users) {
            int clientId = std::stoi(client);
            int client_cluster = ((clientId - 1) % 3) + 1;
            // only do this for clients in your own cluster
            if (client_cluster != clusterID) {
                continue;
            }
            std::vector<std::string> timeline = get_tl_or_fl(synchID, clientId, true);
            if (timeline.size() == 0) {
                continue;
            }
            // std::vector<std::string> followers = getFollowersOfUser(clientId);
            std::vector<std::string> followers = get_tl_or_fl(synchID, clientId, false);
            for (const auto &follower : followers) {
                // Convert follower to integer
                int followerId = std::stoi(follower);

                // Serialize the timeline updates into a JSON message
                std::string message = serializeTimelineToJson(follower, timeline);

                // Determine the message queue for the synchronizer responsible for this follower
                int follower_cluster = ((followerId - 1) % 3) + 1;
                std::string queueName = "synch" + std::to_string(follower_cluster) + "_timeline_queue";
                std::string slaveQueueName = "synch" + std::to_string(follower_cluster + 1) + "_timeline_queue";

                // Publish the message to the follower's queue
                publishMessage(queueName, message);
                publishMessage(slaveQueueName, message);
            }
        }
    }

    std::string serializeTimelineToJson(std::string follower, const std::vector<std::string> &timeline) {
        Json::Value root(Json::objectValue);
        Json::Value timelineData(Json::objectValue);
        Json::Value posts(Json::arrayValue);

        for (size_t i = 0; i < timeline.size(); i += 3) {
            Json::Value entry;
            if (i < timeline.size() && timeline[i].rfind("T ", 0) == 0) {
                entry["T"] = timeline[i].substr(2);  // Extract timestamp after "T "
            }
            if (i + 1 < timeline.size() && timeline[i + 1].rfind("U ", 0) == 0) {
                entry["U"] = timeline[i + 1].substr(2);  // Extract userId after "U "
            }
            if (i + 2 < timeline.size() && timeline[i + 2].rfind("W ", 0) == 0) {
                entry["W"] = timeline[i + 2].substr(2);  // Extract content after "W "
            }

            if (!entry.empty()) {
                posts.append(entry);
            }
        }

        // Wrap the posts under the follower key
        timelineData[follower] = posts;

        // Add the timelineData under the "timeline" key
        root["timeline"] = timelineData;

        Json::StreamWriterBuilder writer;
        writer["indentation"] = "";  // Compact output
        std::string serialized = Json::writeString(writer, root);
        return serialized;
    }

    // For each client in your cluster, consume messages from your timeline queue and modify your client's timeline files based on what the users they follow posted to their timeline
    void consumeTimeline(Json::Value root) {
        // std::string queueName = "synch" + std::to_string(synchID) + "_timeline_queue";
        // std::string message = consumeMessage(queueName, 1000);  // 1 second timeout

        // if (!message.empty()) {
        // Deserialize the message into timeline updates
        // Json::CharReaderBuilder readerBuilder;
        // std::unique_ptr<Json::CharReader> reader(readerBuilder.newCharReader());
        // Json::Value root;
        // std::string errs;

        // bool parsingSuccessful = reader->parse(message.c_str(), message.c_str() + message.size(), &root, &errs);
        // if (!parsingSuccessful) {
        //     std::cerr << "Failed to parse message: " << errs << std::endl;
        //     return;
        // }
        printJsonValue(root);

        std::cout << "Parsing completed successfully." << std::endl;

        // Process each follower's updates
        for (const auto &followerId : root.getMemberNames()) {
            const Json::Value &updates = root[followerId];
            if (!updates.isArray()) {
                // std::cerr << "Invalid format: Expected array of updates for follower " << followerId << std::endl;
                continue;
            }

            // std::cout << "Processing updates for follower: " << followerId << std::endl;

            // Iterate over updates and append them to the timeline file
            for (const auto &update : updates) {
                if (!update.isObject() || !update.isMember("T") || !update.isMember("U") || !update.isMember("W")) {
                    // std::cerr << "Invalid update format for follower " << followerId << std::endl;
                    // printJsonValue(update);
                    continue;
                }

                std::string timestamp = update["T"].asString();
                std::string userId = update["U"].asString();
                std::string content = update["W"].asString();

                // std::cout << "Timeline in cluster " << clusterID << " received for " << synchID
                        //   << " Update: T=" << timestamp << " U=" << userId << " W=" << content << std::endl;

                // Format the update into the 3-line format
                std::vector<std::string> formattedUpdate = {
                    "T " + timestamp,
                    "U " + userId,
                    "W " + content};

                // Append to the timeline file
                appendToTimelineFile(followerId, formattedUpdate);
            }
        }
        // }
    }
    void appendToTimelineFile(std::string followerId, const std::vector<std::string> &update) {
        std::string filename = "./cluster" + std::to_string(clusterID) + "/" + clusterSubdirectory + "/" + followerId + "_timeline.txt";
        std::ifstream infile(filename);
        std::unordered_set<std::string> existingUpdates;
        std::string line;

        // Read the existing timeline file and collect updates as strings
        while (std::getline(infile, line)) {
            std::string timestamp, userId, content;
            if (line.rfind("T ", 0) == 0) {
                timestamp = line;
                if (std::getline(infile, userId) && userId.rfind("U ", 0) == 0) {
                    if (std::getline(infile, content) && content.rfind("W ", 0) == 0) {
                        std::string combinedUpdate = timestamp + "\n" + userId + "\n" + content;
                        existingUpdates.insert(combinedUpdate);
                    }
                }
            }
        }
        infile.close();

        // Prepare the new update as a single string
        std::string newUpdate = update[0] + "\n" + update[1] + "\n" + update[2];

        // Append to the file only if the update is unique
        if (existingUpdates.find(newUpdate) == existingUpdates.end()) {
            std::ofstream outfile(filename, std::ios::app);
            outfile << newUpdate << "\n\n";  // Add a newline after each set of updates
            outfile.close();
            std::cout << "Appended new update to " << filename << std::endl;
        } else {
            std::cout << "Update " << newUpdate << " exists in " << filename << std::endl;
        }
    }

    void printJsonValue(const Json::Value &value) {
        Json::StreamWriterBuilder writer;
        writer["indentation"] = "  ";  // Pretty print with 2 spaces
        std::string jsonString = Json::writeString(writer, value);
        std::cout << "Json::Value as string:\n"
                  << jsonString << std::endl;
    }

   private:
    void updateAllUsersFile(const std::vector<std::string> &users) {
        std::string usersFile = "./cluster" + std::to_string(clusterID) + "/" + clusterSubdirectory + "/all_users.txt";
        // std::string usersFile = "./cluster_" + std::to_string(clusterID) + "/" + clusterSubdirectory + "/all_users.txt";
        std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_all_users.txt";
        sem_t *fileSem = sem_open(semName.c_str(), O_CREAT);
        std::ofstream userStream(usersFile, std::ios::app | std::ios::out | std::ios::in);
        for (std::string user : users) {
            if (!file_contains_user(usersFile, user)) {
                userStream << user << std::endl;
            }
        }
        sem_close(fileSem);
    }
};

void run_synchronizer(std::string coordIP, std::string coordPort, std::string port, int synchID, SynchronizerRabbitMQ &rabbitMQ);

class SynchServiceImpl final : public SynchService::Service {
    // You do not need to modify this in any way
};

void RunServer(std::string coordIP, std::string coordPort, std::string port_no, int synchID) {
    // localhost = 127.0.0.1
    std::string server_address("127.0.0.1:" + port_no);
    log(INFO, "Starting synchronizer server at " + server_address);
    SynchServiceImpl service;
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

    // Initialize RabbitMQ connection
    SynchronizerRabbitMQ rabbitMQ("localhost", 5672, synchID);

    std::thread t1(run_synchronizer, coordIP, coordPort, port_no, synchID, std::ref(rabbitMQ));

    // Create a consumer thread
    std::thread consumerThread([&rabbitMQ]() {
        while (true) {
            // rabbitMQ.consumeUserLists();
            // rabbitMQ.consumeClientRelations();
            // rabbitMQ.consumeTimelines();
            rabbitMQ.consumeQueue("_users_queue");
            rabbitMQ.consumeQueue("_clients_relations_queue");
            rabbitMQ.consumeQueue("_timeline_queue");
            std::this_thread::sleep_for(std::chrono::seconds(5));
            // you can modify this sleep period as per your choice
        } });

    server->Wait();

    //   t1.join();
    //   consumerThread.join();
}

int main(int argc, char **argv) {
    int opt = 0;
    std::string coordIP;
    std::string coordPort;
    std::string port = "3029";

    while ((opt = getopt(argc, argv, "h:k:p:i:")) != -1) {
        switch (opt) {
            case 'h':
                coordIP = optarg;
                break;
            case 'k':
                coordPort = optarg;
                break;
            case 'p':
                port = optarg;
                break;
            case 'i':
                synchID = std::stoi(optarg);
                break;
            default:
                std::cerr << "Invalid Command Line Argument\n";
        }
    }

    std::string log_file_name = std::string("synchronizer-") + port;
    google::InitGoogleLogging(log_file_name.c_str());
    log(INFO, "Logging Initialized. Server starting...");

    coordAddr = coordIP + ":" + coordPort;
    clusterID = ((synchID - 1) % 3) + 1;
    ServerInfo serverInfo;
    serverInfo.set_hostname("localhost");
    serverInfo.set_port(port);
    serverInfo.set_type("synchronizer");
    serverInfo.set_serverid(synchID);
    serverInfo.set_clusterid(clusterID);
    serverInfo.set_ismaster(true);
    std::thread heartbeat_thread(&Heartbeat, coordIP, coordPort, serverInfo, synchID);
    heartbeat_thread.detach();
    log(INFO, "Started Heartbeat");
    // Heartbeat(coordIP, coordPort, serverInfo, synchID);
    log(INFO, "Started RunServer");
    RunServer(coordIP, coordPort, port, synchID);
    return 0;
}

void run_synchronizer(std::string coordIP, std::string coordPort, std::string port, int synchID, SynchronizerRabbitMQ &rabbitMQ) {
    // setup coordinator stub
    std::string target_str = coordIP + ":" + coordPort;
    std::unique_ptr<CoordService::Stub> coord_stub_;
    coord_stub_ = std::unique_ptr<CoordService::Stub>(CoordService::NewStub(grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials())));

    ServerInfo msg;
    Confirmation c;

    msg.set_serverid(synchID);
    msg.set_hostname("127.0.0.1");
    msg.set_port(port);
    msg.set_type("follower");

    // TODO: begin synchronization process
    while (true) {
        // the synchronizers sync files every 5 seconds
        sleep(3);
        if (!isMaster) {
            continue;
        }
        grpc::ClientContext context;
        ServerList followerServers;
        ID id;
        id.set_id(synchID);
        // making a request to the coordinator to see count of follower synchronizers
        coord_stub_->GetAllFollowerServers(&context, id, &followerServers);
        std::vector<int> server_ids;
        std::vector<std::string> hosts, ports;
        for (std::string host : followerServers.hostname()) {
            hosts.push_back(host);
        }
        for (std::string port : followerServers.port()) {
            ports.push_back(port);
        }
        for (int serverid : followerServers.serverid()) {
            server_ids.push_back(serverid);
        }
        total_number_of_registered_synchronizers = server_ids.size();
        // update the count of how many follower sychronizer processes the coordinator has registered

        // below here, you run all the update functions that synchronize the state across all the clusters
        // make any modifications as necessary to satisfy the assignments requirements

        // Publish user list
        rabbitMQ.publishUserList();

        // Publish client relations
        rabbitMQ.publishClientRelations();

        // Publish timelines
        rabbitMQ.publishTimelines();
    }
    return;
}

std::vector<std::string> get_lines_from_file(std::string filename) {
    std::vector<std::string> users;
    std::string user;
    std::ifstream file;
    std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_" + filename;
    sem_t *fileSem = sem_open(semName.c_str(), O_CREAT);
    file.open(filename);
    if (file.peek() == std::ifstream::traits_type::eof()) {
        // return empty vector if empty file
        // std::cout<<"returned empty vector bc empty file"<<std::endl;
        file.close();
        sem_close(fileSem);
        return users;
    }
    while (file) {
        getline(file, user);

        if (!user.empty())
            users.push_back(user);
    }

    file.close();
    sem_close(fileSem);

    return users;
}

int getServerId() {
    grpc::ClientContext context;
    csce662::ServerInfo *serverInfo = new ServerInfo();
    ID id;
    id.set_id(clusterID);
    Status status;
    if (isMaster) {
        status = coordinator_stub_->GetServer(&context, id, serverInfo);
    } else {
        status = coordinator_stub_->GetSlave(&context, id, serverInfo);
    }
    if (!status.ok()) {
        if (status.error_code() == grpc::StatusCode::NOT_FOUND) {
            log(ERROR, "No server found for cluster ID: " + std::to_string(clusterID));
        }
    }
    return serverInfo->serverid();
}

void Heartbeat(std::string coordinatorIp, std::string coordinatorPort, ServerInfo serverInfo, int syncID) {
    // For the synchronizer, a single initial heartbeat RPC acts as an initialization method which
    // servers to register the synchronizer with the coordinator and determine whether it is a master

    log(INFO, "Sending initial heartbeat to coordinator");
    std::string coordinatorInfo = coordinatorIp + ":" + coordinatorPort;
    // std::unique_ptr<CoordService::Stub> stub =
    coordinator_stub_ = std::unique_ptr<CoordService::Stub>(CoordService::NewStub(grpc::CreateChannel(coordinatorInfo, grpc::InsecureChannelCredentials())));
    if (!coordinator_stub_) {
        log(ERROR, "Unable to build CoordService stub");
        exit(-1);
    }
    bool firstHeartbeat = true;
    // send a heartbeat to the coordinator, which registers your follower synchronizer as either a master or a slave
    while (true) {
        // Call the Register method
        grpc::ClientContext context;
        csce662::Confirmation confirmation;
        Status status = coordinator_stub_->Heartbeat(&context, serverInfo, &confirmation);
        if (!status.ok()) {
            log(ERROR, "Failed to register with coordinator: " + status.error_message());
            exit(-1);
        }
        if (!confirmation.status() && !firstHeartbeat) {
            isMaster = !isMaster;
            clusterSubdirectory = std::to_string(getServerId());
        }
        if (firstHeartbeat) {
            if (!confirmation.status()) {
                isMaster = !isMaster;
            }
            clusterSubdirectory = std::to_string(getServerId());
            log(INFO, "Synchronizer registered successfully with coordinator.");
            firstHeartbeat = false;
        } else {
            log(INFO, "Heartbeat acknowledged");
        }
        std::this_thread::sleep_for(std::chrono::seconds(10));  // Sleep before next heartbeat
    }
}

bool file_contains_user(std::string filename, std::string user) {
    std::vector<std::string> users;
    // check username is valid
    std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_" + filename;
    sem_t *fileSem = sem_open(semName.c_str(), O_CREAT);
    users = get_lines_from_file(filename);
    for (int i = 0; i < users.size(); i++) {
        // std::cout<<"Checking if "<<user<<" = "<<users[i]<<std::endl;
        if (user == users[i]) {
            // std::cout<<"found"<<std::endl;
            sem_close(fileSem);
            return true;
        }
    }
    // std::cout<<"not found"<<std::endl;
    sem_close(fileSem);
    return false;
}

std::vector<std::string> get_all_users_func(int synchID) {
    // read all_users file master and client for correct serverID
    // std::string master_users_file = "./master"+std::to_string(synchID)+"/all_users";
    // std::string slave_users_file = "./slave"+std::to_string(synchID)+"/all_users";
    std::string clusterID = std::to_string(((synchID - 1) % 3) + 1);
    std::string master_users_file = "./cluster" + clusterID + "/1/all_users.txt";
    std::string slave_users_file = "./cluster" + clusterID + "/2/all_users.txt";
    // take longest list and package into AllUsers message
    std::vector<std::string> master_user_list = get_lines_from_file(master_users_file);
    std::vector<std::string> slave_user_list = get_lines_from_file(slave_users_file);

    if (master_user_list.size() >= slave_user_list.size())
        return master_user_list;
    else
        return slave_user_list;
}

std::vector<std::string> get_tl_or_fl(int synchID, int clientID, bool tl) {
    // std::string master_fn = "cluster_" + std::to_string(clusterID) + "/1/" + std::to_string(clientID);
    // std::string slave_fn = "cluster_" + std::to_string(clusterID) + "/2/" + std::to_string(clientID);
    std::string master_fn = "./cluster" + std::to_string(clusterID) + "/1/" + std::to_string(clientID);
    std::string slave_fn = "./cluster" + std::to_string(clusterID) + "/2/" + std::to_string(clientID);
    if (tl) {
        master_fn.append("_posts.txt");
        slave_fn.append("_posts.txt");
        // master_fn.append("_timeline.txt");
        // slave_fn.append("_timeline.txt");
    } else {
        master_fn.append("_followers.txt");
        slave_fn.append("_followers.txt");
    }

    std::vector<std::string> m = get_lines_from_file(master_fn);
    std::vector<std::string> s = get_lines_from_file(slave_fn);
    if (m.size() >= s.size()) {
        return m;
    } else {
        return s;
    }
}

std::vector<std::string> getFollowersOfUser(int ID) {
    std::vector<std::string> followers;
    std::string clientID = std::to_string(ID);
    std::vector<std::string> usersInCluster = get_all_users_func(synchID);
    for (auto userID : usersInCluster) {  // Examine each user's following file
        // std::string file = "cluster_" + std::to_string(clusterID) + "/" + clusterSubdirectory + "/" + userID + "_follow_list.txt";
        std::string file = "./cluster" + std::to_string(clusterID) + "/" + clusterSubdirectory + "/" + userID + "_following.txt";
        std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_" + userID + "_follow_list.txt";
        sem_t *fileSem = sem_open(semName.c_str(), O_CREAT);
        // std::cout << "Reading file " << file << std::endl;
        if (file_contains_user(file, clientID)) {
            followers.push_back(userID);
        }
        sem_close(fileSem);
    }
    return followers;
}