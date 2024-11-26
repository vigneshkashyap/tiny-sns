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

#include <glog/logging.h>
#include <google/protobuf/duration.pb.h>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <stdlib.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "sns.grpc.pb.h"

// #include "json.hpp"
#define log(severity, msg) \
    LOG(severity) << msg;  \
    google::FlushLogFiles(google::severity);

#include "coordinator.grpc.pb.h"
#include "sns.grpc.pb.h"

// #define DELIMITER "\x1F"

using csce662::Confirmation;
using csce662::CoordService;
using csce662::ID;
using csce662::ListReply;
using csce662::Message;
using csce662::Reply;
using csce662::Request;
using csce662::ServerInfo;
using csce662::SNSService;
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

struct Client {
    std::string username;
    bool connected = true;
    int following_file_size = 0;
    std::vector<Client *> client_followers;
    std::vector<Client *> client_following;
    bool isInitialTimelineRequest = true;
    ServerReaderWriter<Message, Message> *stream = 0;
    bool operator==(const Client &c1) const { return (username == c1.username); }
};

struct Post {
    std::string username;
    std::string content;
    long long timestamp;
};

// Vector that stores every client that has been created
std::vector<Client *> client_db;

class CoordinationService {
   private:
    ServerInfo serverInfo;
    std::unique_ptr<CoordService::Stub> stub_;
    std::string coordinator_ip;
    std::string coordinator_port;
    bool firstHeartbeat;
    ServerInfo *slaveServer;
    std::unique_ptr<SNSService::Stub> slaveStub;

   public:
    // Default constructor
    CoordinationService() {
        // Initialize members with default values or leave empty
    }
    CoordinationService(std::string server_id, std::string server_port, std::string cluster_id, std::string coordinator_ip, std::string coordinator_port) {
        serverInfo.set_serverid(std::stoi(server_id));
        serverInfo.set_clusterid(std::stoi(cluster_id));
        serverInfo.set_hostname("localhost");
        serverInfo.set_port(server_port);
        serverInfo.set_type("server");
        serverInfo.set_ismaster(true);
        this->coordinator_ip = coordinator_ip;
        this->coordinator_port = coordinator_port;
        firstHeartbeat = true;
    }
    // Add these getter functions
    const ServerInfo &getServerInfo() const {
        return serverInfo;
    }

    SNSService::Stub *getSlaveStub() const {
        return slaveStub.get();
    }

    std::string filePrefix() {
        return "./cluster" + std::to_string(serverInfo.clusterid()) + "/" + std::to_string(serverInfo.serverid()) + "/";
    }

    std::string filePrefix(std::string username) {
        return "./cluster" + std::to_string(serverInfo.clusterid()) + "/" + std::to_string(serverInfo.serverid()) + "/" + username;
    }

    void createFolder(const std::string &path) {
        std::filesystem::path dirPath(path);
        // Check if the directory already exists
        if (std::filesystem::exists(dirPath)) {
            log(WARNING, "Folder already exists:\t" + path);
        } else {
            // Create the folder
            if (std::filesystem::create_directories(dirPath)) {
                log(INFO, "Folder created:\t" + path);
            } else {
                log(ERROR, "Error creating folder:\t" + path);
            }
        }
    }

    void buildSlaveStub() {
        grpc::ClientContext context;
        csce662::ServerInfo *slaveServerInfo = new ServerInfo();
        ID id;
        id.set_id(serverInfo.clusterid());
        Status status = stub_->GetSlave(&context, id, slaveServerInfo);
        if (!status.ok()) {
            if (status.error_code() == grpc::StatusCode::NOT_FOUND) {
                log(ERROR, "No slave server found for cluster ID: " + std::to_string(serverInfo.clusterid()));
            } else {
                log(ERROR, "Failed to register with coordinator: " + status.error_message());
                exit(-1);
            }
        } else {
            log(INFO, "Stub creating with Hostname:\t" + slaveServerInfo->hostname() + "\tPort:\t" + slaveServerInfo->port());
            slaveStub = SNSService::NewStub(grpc::CreateChannel(slaveServerInfo->hostname() + ":" + slaveServerInfo->port(), grpc::InsecureChannelCredentials()));
            if (!slaveStub) {
                log(ERROR, "Slave Stub not created");
            }
        }
    }

    void heartbeat() {
        try {
            log(INFO, "Entering heartbeat loop...");
            while (true) {
                std::string coordinator_address = coordinator_ip + ":" + coordinator_port;
                stub_ = CoordService::NewStub(grpc::CreateChannel(coordinator_address, grpc::InsecureChannelCredentials()));
                // Check if stub is created successfully
                if (!stub_) {
                    log(ERROR, "Unable to build CoordService stub");
                    return;
                }
                // Call the Register method
                grpc::ClientContext context;
                csce662::Confirmation confirmation;
                Status status = stub_->Heartbeat(&context, serverInfo, &confirmation);
                if (!status.ok()) {
                    log(ERROR, "Failed to register with coordinator: " + status.error_message());
                    exit(-1);
                }
                if (!confirmation.status()) {
                    serverInfo.set_ismaster(!serverInfo.ismaster());
                    if (serverInfo.ismaster()) {
                        buildSlaveStub();
                    }
                }
                if (serverInfo.ismaster() && !slaveStub) {
                    buildSlaveStub();
                }
                // serverInfo.set_type(serverInfo.ismaster()? "master" : "slave");
                if (firstHeartbeat) {
                    log(INFO, "Server registered successfully with coordinator.");
                    std::string serverFolder = "./cluster" + std::to_string(serverInfo.clusterid()) + "/" + std::to_string(serverInfo.serverid());
                    createFolder(serverFolder);
                    if (!confirmation.reconnect()) {
                        clearClientDB();
                    }
                    firstHeartbeat = false;
                    if (serverInfo.ismaster()) {
                        buildSlaveStub();
                    }
                } else {
                    // log(INFO, "Heartbeat acknowledged");
                }
                std::this_thread::sleep_for(std::chrono::seconds(10));  // Sleep before next heartbeat
            }
        } catch (const std::exception &e) {
            log(ERROR, "Exception in heartbeat" + std::string(e.what()));
        }
    }

    void clearClientDB() {
        namespace fs = std::filesystem;
        std::string directoryPath = "./cluster" + std::to_string(serverInfo.clusterid()) + "/" + std::to_string(serverInfo.serverid()) + "/";
        try {
            if (!fs::exists(directoryPath) || !fs::is_directory(directoryPath)) {
                log(INFO, "Directory does not exist or is not a directory: " + directoryPath);
                return;
            }
            // Iterate over the directory and remove each item
            for (const auto &entry : fs::directory_iterator(directoryPath)) {
                fs::remove_all(entry);  // Remove files or subdirectories
            }
            log(INFO, "Directory cleared: " + directoryPath);
        } catch (const std::filesystem::filesystem_error &e) {
            log(INFO, "Filesystem error:\t" + std::string(e.what()));
        } catch (const std::exception &e) {
            log(ERROR, "Error:\t" + std::string(e.what()));
        }
        // std::ofstream clientDB(file_path, std::ios::trunc);
        // if (clientDB.is_open()) {
        //     clientDB.close();  // Close the file
        //     log(INFO, "CLient DB file has been trunated");
        // } else {
        //     log(WARNING, "CLient DB file does not exist");
        // }
    }

    void loadClientDB() {
        std::string all_users_path = filePrefix() + "all_users.txt";
        std::ifstream all_users(all_users_path);
        std::string username;
        if (!all_users) {
            log(ERROR, "Failed to open the file for reading.");
            return;
        }
        // Clear the existing client database before reloading
        while (std::getline(all_users, username)) {
            if (username.empty()) {
                continue;
            }
            Client *client = getClient(username);
            if (client == NULL) {
                client = new Client();
                client->username = username;
                client_db.push_back(client);
            }
        }
        all_users.close();
        log(INFO, "Client size:\t" + std::to_string(client_db.size()));
        for (Client *client : client_db) {
            loadFollowersAndFollowing(client);
        }
        log(INFO, "Client size:\t" + std::to_string(client_db.size()));
    }

    void loadFollowersAndFollowing(Client *client) {
        // client->client_followers.clear();
        client->client_following.clear();
        std::string following_file_path = filePrefix(client->username) + "_following.txt";
        std::string followers_file_path = filePrefix(client->username) + "_followers.txt";
        // Load following
        std::ifstream following_file(following_file_path);
        if (!following_file) {
            log(ERROR, "Failed to open the following file for reading: " + following_file_path);
            return;
        }
        std::string following_username;
        while (std::getline(following_file, following_username)) {
            if (following_username.empty()) {
                continue;
            }
            Client *following_client = getClient(following_username);
            client->client_following.push_back(following_client);
        }
        following_file.close();
        // Load followers
        std::ifstream followers_file(followers_file_path);
        if (!followers_file) {
            log(ERROR, "Failed to open the follower file for reading: " + followers_file_path);
            return;
        }
        std::string follower_username;
        while (std::getline(followers_file, follower_username)) {
            if (follower_username.empty()) {
                continue;
            }
            Client *follower_client = getClient(follower_username);
            bool followerPresent = false;
            for (Client *followers : client->client_followers) {
                if (followers == follower_client) {
                    followerPresent = true;
                    break;
                }
            }
            if (!followerPresent) {
                client->client_followers.push_back(follower_client);
            }
        }
        followers_file.close();
    }

    // void relinkClients() {
    //     // Create a mapping from usernames to Client* for easy lookup
    //     for (auto *client : client_db) {
    //         username_to_client[client->username] = client;
    //     }
    //     // Relink client followers and following
    //     for (auto *client : client_db) {
    //         for (auto *&follower : client->client_followers) {
    //             follower = username_to_client[follower->username];  // Update to point to the actual client
    //         }
    //         for (auto *&following : client->client_following) {
    //             following = username_to_client[following->username];  // Update to point to the actual client
    //         }
    //     }
    // }

    // void restoreDB() {
    //     std::string file_path = "./cluster" + std::to_string(serverInfo.clusterid()) + "/" + std::to_string(serverInfo.serverid()) + "/client_db.txt";
    //     // loadClientDB(file_path);
    //     relinkClients();
    // }

    Client *getClient(const std::string &username) {
        // Check if client already exists in the client_db
        for (Client *client : client_db) {
            if (client->username.compare(username) == 0) {
                // log(INFO, "We have found the client");
                return client;  // Return existing client if found
            }
        }
        // log(INFO, "We have not found client");
        return NULL;
    }
};

CoordinationService *coordinationService;

class SNSServiceImpl final : public SNSService::Service {
    Client *getClient(std::string username) {
        // Check if client already exists in the client_db
        // log(INFO, "Client size:\t" + std::to_string(client_db.size()));
        for (Client *client : client_db) {
            // log(INFO, "Curr Username:\t" + client->username);
            if (client->username.compare(username) == 0) {
                // log(INFO, "We have found the client");
                return client;  // Return existing client if found
            }
        }
        // log(INFO, "We have not found client");
        return NULL;
    }

    // RPC Login
    Status Login(ServerContext *context, const Request *request, Reply *reply) override {
        if (coordinationService->getServerInfo().ismaster() && coordinationService->getSlaveStub() != nullptr) {
            grpc::ClientContext slave_context;
            Reply slave_reply;
            grpc::Status status = coordinationService->getSlaveStub()->Login(&slave_context, *request, &slave_reply);
            if (!status.ok()) {
                log(ERROR, "Login Failed:\tError forwarding to slave server");
            }
        }
        coordinationService->loadClientDB();
        // We go over the client_db to check if username already exists, and accordingly return a grpc::Status::ALREADY_EXISTS
        Client *client = getClient(request->username());
        if (client == nullptr) {
            client = new Client();
            client->username = request->username();
            client->connected = true;
            client_db.push_back(client);
            truncateFile(client->username);
            std::ofstream all_users(coordinationService->filePrefix() + "all_users.txt", std::ios::app);
            all_users << request->username() << std::endl;
            all_users.close();
        }
        // if (user_index != NULL) {
        //     reply->set_msg("Username already exists");
        //     log(ERROR, "Login Failed:\t\tUsername " + request->username() + " already exists");
        //     reply->set_msg("1");
        //     return Status(grpc::StatusCode::ALREADY_EXISTS, "Username already exists");
        // }
        // coordinationService->saveClientDB();
        reply->set_msg("Login successful");
        log(INFO, "Login Successful:\tUser " + client->username);
        return Status::OK;
    }

    Status List(ServerContext *context, const Request *request, ListReply *list_reply) override {
        coordinationService->loadClientDB();
        std::string curr_username = request->username();
        // Iterate over the Client DB and add all usernames, and their followers
        std::vector<std::string> users;
        std::vector<std::string> followers;
        for (Client *user : client_db) {
            users.push_back(user->username);
            // list_reply->add_all_users(user->username);

            // When we go over the current user's client, we add all the followers
            if (curr_username.compare(user->username) == 0) {
                for (Client *follower : user->client_followers) {
                    followers.push_back(follower->username);
                    // list_reply->add_followers(follower->username);
                }
            }
        }
        std::sort(users.begin(), users.end());
        std::sort(followers.begin(), followers.end());
        for (std::string user : users) {
            list_reply->add_all_users(user);
        }
        for (std::string follower : followers) {
            list_reply->add_followers(follower);
        }
        log(INFO, "List Successful:\t\tRequested by User " + curr_username);
        return Status::OK;
    }

    Status Follow(ServerContext *context, const Request *request, Reply *reply) override {
        if (coordinationService->getServerInfo().ismaster() && coordinationService->getSlaveStub() != nullptr) {
            grpc::ClientContext slave_context;
            Reply slave_reply;
            grpc::Status status = coordinationService->getSlaveStub()->Follow(&slave_context, *request, &slave_reply);
            if (!status.ok()) {
                log(ERROR, "Follow Failed:\t\tError forwarding to slave server");
            }
        }
        coordinationService->loadClientDB();
        // Username of the client invoking FOLLOW
        std::string curr_user = request->username();
        // Username of the user to follow
        std::string username = request->arguments().Get(0);
        // If the user is trying to follow self, then we return the error code 1 -> FAILURE_ALREADY_EXISTS
        if (curr_user.compare(username) == 0) {
            log(ERROR, "Follow Failed:\t\tUser " + curr_user + " trying to follow self");
            reply->set_msg("1");
            return Status::OK;
        }
        Client *user = getClient(curr_user);
        Client *to_follow = getClient(username);
        // If the user to_follow is invalid, then we return the error code 3 -> FAILURE_INVALID_USERNAME
        if (user == NULL || to_follow == NULL) {
            log(ERROR, "Follow Failed:\t\tUser " + curr_user + " trying to follow invalid username " + username);
            reply->set_msg("3");
            return Status::OK;
        }
        // Check if User already follows the to_follow user, if true, then we return the error code 1 -> FAILURE_ALREADY_EXISTS
        for (Client *follower : user->client_following) {
            if (follower == to_follow) {
                log(ERROR, "Follow Failed:\t\tUser " + curr_user + " already following " + username);
                reply->set_msg("1");
                return Status::OK;
            }
        }
        // Success scenario, we add the to_follow user in user's following list, and user in the to_follow user's followers
        // Then we return the code 0 -> SUCCESS
        log(INFO, "Follow Successful:\t\tUser " + curr_user + " now following " + username);
        reply->set_msg("0");
        user->client_following.push_back(to_follow);
        to_follow->client_followers.push_back(user);
        std::ofstream following(coordinationService->filePrefix(user->username) + "_following.txt", std::ios::app);
        std::ofstream followers(coordinationService->filePrefix(to_follow->username) + "_followers.txt", std::ios::app);
        following << to_follow->username << std::endl;
        followers << user->username << std::endl;
        following.close();
        followers.close();
        // coordinationService->saveClientDB();
        return Status::OK;
    }

    Status UnFollow(ServerContext *context, const Request *request, Reply *reply) override {
        if (coordinationService->getServerInfo().ismaster() && coordinationService->getSlaveStub() != nullptr) {
            grpc::ClientContext slave_context;
            Reply slave_reply;
            grpc::Status status = coordinationService->getSlaveStub()->UnFollow(&slave_context, *request, &slave_reply);
            if (!status.ok()) {
                log(ERROR, "Follow Failed:\t\tError forwarding to slave server");
            }
        }
        coordinationService->loadClientDB();
        // Username of the client invoking FOLLOW
        std::string curr_user = request->username();
        // Username of the user to follow
        std::string username = request->arguments().Get(0);
        // If the user is trying to follow self, then we return the error code 3 -> FAILURE_INVALID_USERNAME
        if (curr_user.compare(username) == 0) {
            log(ERROR, "Unfollow Failed:\t\tUser " + curr_user + " trying to unfollow self");
            reply->set_msg("3");
            return Status::OK;
        }
        Client *user = getClient(curr_user);
        Client *to_unfollow_user = getClient(username);
        // If the user to_follow is invalid, then we return the error code 3 -> FAILURE_INVALID_USERNAME
        if (to_unfollow_user == NULL) {
            log(ERROR, "Unfollow Failed:\t\tUser " + curr_user + " trying to unfollow invalid username " + username);
            reply->set_msg("3");
            return Status::OK;
        }
        // We identify the following_index in the vector of following of user
        int following_index = -1;
        for (int i = 0; i < user->client_following.size(); i++) {
            if (user->client_following[i] == to_unfollow_user) {
                following_index = i;
                break;
            }
        }
        // We identify the follower_index in the vector of followers of the to_unfollow_user
        int follower_index = -1;
        for (int i = 0; i < to_unfollow_user->client_followers.size(); i++) {
            if (to_unfollow_user->client_followers[i] == user) {
                follower_index = i;
                break;
            }
        }
        // If we do not get either one, that means user does not follow to_unfollow_user, return code -> 4 FAILURE_NOT_A_FOLLOWER
        if (following_index == -1 || follower_index == -1) {
            log(ERROR, "Unfollow Failed:\t\tUser " + curr_user + " does not follow " + username);
            reply->set_msg("4");
            return Status::OK;
        }
        // We need to remove the to_unfollow_user's posts from user's timeline
        removePostsFromTimeline(curr_user, username);
        // We need to remove the to_unfollow_user from the user's following vector using following_index
        user->client_following.erase(user->client_following.begin() + following_index);
        // We need to remove the user from the to_unfollow_user's following vector using follower_index
        to_unfollow_user->client_followers.erase(to_unfollow_user->client_followers.begin() + follower_index);
        removeFromFollowLists(user->username, to_unfollow_user->username);
        // coordinationService->saveClientDB();
        log(INFO, "Unfollow Successful:\t\tUser " + curr_user + " unfollowed " + username);
        // We return the success code -> 0 SUCCESS
        reply->set_msg("0");
        return Status::OK;
    }

    void removeFromFollowLists(const std::string &username, const std::string &to_remove) {
        std::string following_filename = coordinationService->filePrefix(username) + "_following.txt";
        std::string follower_filename = coordinationService->filePrefix(to_remove) + "_followers.txt";

        // Helper function to remove a user from a file
        auto removeEntryFromFile = [](const std::string &filename, const std::string &entry_to_remove) {
            // Open the original file for reading
            std::ifstream file_in(filename);
            if (!file_in.is_open()) {
                // std::cerr << "Unable to open file: " << filename << std::endl;
                return;
            }

            // Open a temporary file for writing the updated content
            std::ofstream temp_out(filename + ".tmp");
            if (!temp_out.is_open()) {
                // std::cerr << "Unable to open temp file for writing." << std::endl;
                return;
            }

            std::string line;
            while (std::getline(file_in, line)) {
                if (line != entry_to_remove) {
                    // Write only lines that don't match the entry to be removed
                    temp_out << line << std::endl;
                }
            }

            file_in.close();
            temp_out.close();

            // Replace the original file with the updated one
            std::remove(filename.c_str());
            std::rename((filename + ".tmp").c_str(), filename.c_str());
        };

        // Remove 'to_remove' from the following file of the user
        removeEntryFromFile(following_filename, to_remove);

        // Remove 'username' from the follower file of 'to_remove'
        removeEntryFromFile(follower_filename, username);
    }

    // Method is used to create a new proto timestamp * from epoch seconds
    google::protobuf::Timestamp *createProtoTimestampFromEpoch(long long epoch_seconds) {
        google::protobuf::Timestamp *ts = new google::protobuf::Timestamp();
        ts->set_seconds(epoch_seconds);
        ts->set_nanos(0);
        return ts;
    }

    // Split uses the delimiter to split each line in a file and return the values as a vector of strings
    std::vector<std::string> split(const std::string &str, const std::string &delimiter) {
        std::vector<std::string> tokens;
        size_t start = 0, end = 0;
        while ((end = str.find(delimiter, start)) != std::string::npos) {
            tokens.push_back(str.substr(start, end - start));
            start = end + delimiter.length();
        }
        tokens.push_back(str.substr(start));
        return tokens;
    }

    // As the test case format display \n char to be stored, we encode it to \\n before adding to posts or timeline file
    std::string encodeNewLineChar(const std::string &content) {
        std::string encoded_content = content;
        size_t pos = 0;
        while ((pos = encoded_content.find('\n', pos)) != std::string::npos) {
            encoded_content.replace(pos, 1, "\\n");
            pos += 2;
        }
        return encoded_content;
    }

    // \n in post msg is encoded to \\n before adding to posts or timeline file, below method used to decode
    std::string decodeNewLineChar(const std::string &content) {
        std::string decoded_content = content;
        size_t pos = 0;
        while ((pos = decoded_content.find("\\n", pos)) != std::string::npos) {
            decoded_content.replace(pos, 2, "\n");
            pos += 1;
        }
        return decoded_content;
    }

    // Helper Method, to add a new Message file
    void addToFile(std::string file_path, Message message) {
        // Create a Post object
        Post post;
        post.username = message.username();
        post.content = message.msg();
        post.timestamp = message.timestamp().seconds();
        // Open the file in append mode
        std::ofstream file(file_path, std::ios::app);
        if (!file) {
            log(ERROR, "Error opening file:\t\t" + file_path);
            return;
        }
        writePostToFile(file, post);
        file.close();
    }

    std::string createDateTimeFromTimestamp(long long timestamp) {
        std::time_t time = static_cast<std::time_t>(timestamp);  // Assuming timestamp is in seconds
        std::tm *tm_time = std::localtime(&time);

        // Format as YYYY-MM-DD HH:MM:SS
        char buffer[20];
        std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", tm_time);
        return std::string(buffer);
    }

    // Reads posts from the file and returns them as a vector
    std::vector<Post> parseFileContentForPosts(const std::string &file_path, int start_line = 0) {
        std::ifstream file_in(file_path);
        std::vector<Post> posts;
        std::string line;
        if (start_line % 4 != 0) {
            log(WARNING, "Misaligned start_line: " + std::to_string(start_line) + ". Adjusting to the nearest valid start.");
            start_line -= (start_line % 4);  // Align to the nearest valid start
        }
        int current_line = 0;
        while (current_line < start_line && std::getline(file_in, line)) {
            current_line++;
        }
        while (true) {
            std::vector<std::string> lines;

            // Read lines until a blank line (or termination condition) is found
            while (std::getline(file_in, line)) {
                log(INFO, "Read line: " + line);
                current_line++;
                if (line.empty()) {
                    break;  // End of current post
                }
                lines.push_back(line);
            }

            // If no lines were read, we have reached the end of the file
            if (lines.empty()) {
                break;
            }

            // Parse the collected lines into a Post object
            Post post = parsePost(lines);
            posts.push_back(post);
        }

        file_in.close();
        return posts;
    }

    // Parses a Post object from a vector of strings
    Post parsePost(const std::vector<std::string> &lines) {
        Post post;

        // Ensure lines follow a specific order:
        if (lines.size() < 3) {
            log(ERROR, "Insufficient data to form a Post.");
            throw std::runtime_error("Insufficient data to form a Post.");
        }

        // Parse each line based on its expected format
        post.timestamp = parseTimestampLine(lines[0]);
        post.username = parseUsernameLine(lines[1]);
        post.content = parseContentLine(lines[2]);

        return post;
    }

    // Removes posts from the timeline based on the unfollowed user
    void writePostToFile(std::ofstream &file, const Post &post) {
        file << "T " << createDateTimeFromTimestamp(post.timestamp) << std::endl;  // Convert timestamp to desired format
        file << "U " << post.username << std::endl;
        file << "W " << encodeNewLineChar(post.content) << std::endl;
        file << std::endl;  // Empty line to separate posts
    }

    void removePostsFromTimeline(const std::string &username, const std::string &unfollowed_user) {
        std::string timeline_path = coordinationService->filePrefix(username) + "_timeline.txt";

        // Open the timeline file for reading
        std::ifstream file_in(timeline_path);
        if (!file_in.is_open()) {
            // std::cerr << "Unable to open timeline file: " << timeline_path << std::endl;
            return;
        }

        // Open a temporary file for writing the updated timeline
        std::ofstream temp_out(timeline_path + ".tmp");
        if (!temp_out.is_open()) {
            // std::cerr << "Unable to open temp file for writing." << std::endl;
            return;
        }

        std::string line;
        Post current_post;

        // Reading the file 3 lines at a time (assuming the file is structured in blocks of 3 lines per post)
        while (std::getline(file_in, line)) {
            if (line.rfind("T ", 0) == 0) {
                // Parse the timestamp line
                current_post.timestamp = parseTimestampLine(line);
            }

            std::getline(file_in, line);  // Username line
            current_post.username = parseUsernameLine(line);

            std::getline(file_in, line);                    // Content line
            current_post.content = parseContentLine(line);  // Assuming the prefix "W "

            std::getline(file_in, line);  // This should be the empty line between posts

            // If the current post's username is not the unfollowed user's, keep it
            if (current_post.username != unfollowed_user) {
                writePostToFile(temp_out, current_post);
            }
        }

        file_in.close();
        temp_out.close();

        // Replace the original file with the updated one
        std::remove(timeline_path.c_str());
        std::rename((timeline_path + ".tmp").c_str(), timeline_path.c_str());
    }
    // Helper functions to parse individual components from lines
    long long parseTimestampLine(const std::string &line) {
        // Extract the date-time string (assuming it starts at index 2, skipping 'T ')
        std::string datetime_str = line.substr(2);

        // Create a tm struct to store the parsed date and time
        std::tm tm = {};
        std::istringstream ss(datetime_str);

        // Parse the string into the tm struct (format: YYYY-MM-DD HH:MM:SS)
        ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");

        if (ss.fail()) {
            // std::cerr << "Failed to parse the timestamp: " << datetime_str << std::endl;
            return -1;
        }

        // Convert to time_t (which is equivalent to Unix timestamp)
        std::time_t time_since_epoch = std::mktime(&tm);

        // Check if mktime failed
        if (time_since_epoch == -1) {
            // std::cerr << "Failed to convert time to timestamp." << std::endl;
            return -1;
        }

        return static_cast<long long>(time_since_epoch);
    }

    std::string parseUsernameLine(const std::string &line) {
        return line.substr(2);  // Assuming 'U ' prefix
    }

    std::string parseContentLine(const std::string &line) {
        return decodeNewLineChar(line.substr(2));  // Assuming 'W ' prefix
    }

    // When logging in a user, below helper method turncates timeline and posts file of user
    void truncateFile(const std::string username) {
        // Open the file in truncate mode
        std::string timeline_path = coordinationService->filePrefix(username) + "_timeline.txt";
        std::string posts_path = coordinationService->filePrefix(username) + "_posts.txt";
        std::string following_path = coordinationService->filePrefix(username) + "_following.txt";
        std::string follower_path = coordinationService->filePrefix(username) + "_followers.txt";
        std::ofstream timeline(timeline_path, std::ios::trunc);
        std::ofstream post(posts_path, std::ios::trunc);
        std::ofstream following(following_path, std::ios::trunc);
        std::ofstream follower(follower_path, std::ios::trunc);
        if (!timeline) {
            log(ERROR, "Error opening file: " + timeline_path);
            return;
        }
        if (!post) {
            log(ERROR, "Error opening file: " + posts_path);
            return;
        }
        if (!following) {
            log(ERROR, "Error opening file: " + following_path);
            return;
        }
        if (!follower) {
            log(ERROR, "Error opening file: " + follower_path);
            return;
        }
        timeline.close();
        post.close();
        following.close();
        follower.close();
    }

    // Helper method that uses the ServerReaderWriter stream of client, and Client, to get Timeline Posts, sort by timestamp and write them to client
    void displayTimeline(ServerReaderWriter<Message, Message> *stream, Client *user, std::unordered_map<std::string, int> &lastProcessedLine) {
        std::string timeline_path = coordinationService->filePrefix(user->username) + "_timeline.txt";
        std::vector<Post> posts = parseFileContentForPosts(timeline_path);
        // boolean compare method to sort by larger timestamp
        auto compareByTimestamp = [](const Post &a, const Post &b) {
            return a.timestamp < b.timestamp;
        };
        // Sort method to sort all the posts
        std::sort(posts.begin(), posts.end(), compareByTimestamp);
        int length = posts.size();
        // Decide the number of posts
        int posts_size = std::min(length, 20);
        Message msg;
        // Iterate over the posts and set the Message variables
        for (int idx = 0; idx < posts_size; idx++) {
            const auto &post = posts[idx];
            msg.set_allocated_timestamp(createProtoTimestampFromEpoch(post.timestamp));
            msg.set_username(post.username);
            msg.set_msg(post.content);
            // Use client's stream to write the timeline post
            stream->Write(msg);
        }
        log(INFO, "GetTimeline Successful:\tUser " + user->username + " has " + std::to_string(posts_size) + " posts");
        lastProcessedLine[user->username] = posts_size * 4;
    }

    void monitorTimelineUpdates(Client *user, ServerReaderWriter<Message, Message> *stream, std::unordered_map<std::string, int> &lastProcessedLine) {
        std::string timelineFile = coordinationService->filePrefix(user->username) + "_timeline.txt";
        log(INFO, "Started monitoring updates for User " + user->username)
        while (user->connected) {
            std::this_thread::sleep_for(std::chrono::seconds(10));  // Wait for 5 seconds

            int start_line = lastProcessedLine[user->username];
            std::vector<Post> newPosts = parseFileContentForPosts(timelineFile, start_line);

            // Send new posts over the stream
            for (const auto &post : newPosts) {
                Message msg;
                msg.set_allocated_timestamp(createProtoTimestampFromEpoch(post.timestamp));
                msg.set_username(post.username);
                msg.set_msg(post.content);
                stream->Write(msg);
                log(INFO, "Sent new post to User " + user->username + ": " + post.content);
            }
            // Update the last processed line count
            lastProcessedLine[user->username] += newPosts.size() * 4;  // Each post has 3 lines + 1 blank line
        }
        log(INFO, "Stopped monitoring updates for User " + user->username);
    }

    void makePost(Message new_post, Client *user) {
        coordinationService->loadClientDB();
        // Write the post to the user's posts file
        std::string posts_file_path = coordinationService->filePrefix(user->username) + "_posts.txt";
        log(INFO, "Add to File:\t\tMessage from User " + user->username + " to User " + user->username + "'s posts file " + posts_file_path);
        addToFile(posts_file_path, new_post);

        // Write the post to followers' timeline files
        for (Client *follower : user->client_followers) {
            std::string follower_timeline_path = coordinationService->filePrefix(follower->username) + "_timeline.txt";
            log(INFO, "Add To File:\t\tMessage from User " + new_post.username() + " to User " + follower->username + "'s timeline file " + follower_timeline_path);
            addToFile(follower_timeline_path, new_post);
        }
    }

    Status Timeline(ServerContext *context, ServerReaderWriter<Message, Message> *stream) override {
        Message message;
        Client *user = nullptr;
        std::unordered_map<std::string, int> lastProcessedLine;

        while (stream->Read(&message)) {
            user = getClient(message.username());
            user->stream = stream;

            // Handle the first timeline request
            if (user->isInitialTimelineRequest) {
                user->connected = true;
                log(INFO, "Timeline Request:\t\tUser " + message.username());
                displayTimeline(stream, user, lastProcessedLine);

                // Spawn a thread to monitor timeline updates
                std::thread monitorThread([this, user, stream, &lastProcessedLine]() { this->monitorTimelineUpdates(user, stream, lastProcessedLine); });

                monitorThread.detach();  // Detach the thread to allow it to run independently

                user->isInitialTimelineRequest = false;
            } else {
                log(INFO, "Adding a new Post " + message.username());
                // Helper Method to Broadcast the New Post to all followers and write to the respective files
                makePost(message, user);
            }
        }

        user->connected = false;  // Mark the user as disconnected
        return Status::OK;
    }
};

int RunServer(std::string server_id, std::string server_port, std::string cluster_id, std::string coordinator_ip, std::string coordinator_port) {
    std::string server_address = "0.0.0.0:" + server_port;
    SNSServiceImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    if (coordinationService != nullptr) {
        delete coordinationService;  // Clean up if it already exists
    }
    coordinationService = new CoordinationService(server_id, server_port, cluster_id, coordinator_ip, coordinator_port);
    std::thread heartbeat_thread(&CoordinationService::heartbeat, coordinationService);
    heartbeat_thread.detach();
    server->Wait();
    return 0;
}

int main(int argc, char **argv) {
    std::string port = "3010";
    std::string server_id = "1";
    std::string cluster_id = "1";
    std::string coordinator_ip = "localhost";
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
        }
    }
    std::string log_file_name = std::string("cluster-") + cluster_id + std::string("-server-") + server_id + "-port-" + port;
    google::InitGoogleLogging(log_file_name.c_str());
    log(INFO, "Logging Initialized. Server starting...");
    RunServer(server_id, port, cluster_id, coordinator_ip, coordinator_port);
    return 0;
}
