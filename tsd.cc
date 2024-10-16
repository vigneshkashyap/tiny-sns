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
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

// #include "json.hpp"
#define log(severity, msg) \
    LOG(severity) << msg;  \
    google::FlushLogFiles(google::severity);

#include "coordinator.grpc.pb.h"
#include "sns.grpc.pb.h"

#define DELIMITER "\x1F"

using csce662::Confirmation;
using csce662::CoordService;
using csce662::ListReply;
using csce662::Message;
using csce662::Reply;
using csce662::Request;
using csce662::ServerInfo;
using csce662::SNSService;
using google::protobuf::Duration;
using google::protobuf::Timestamp;
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
ServerInfo serverInfo;

class SNSServiceImpl final : public SNSService::Service {
    Client *getClient(std::string username) {
        for (Client *client : client_db) {
            if (client->username.compare(username) == 0) {
                return client;
            }
        }
        return NULL;
    }

    std::string filePrefix(std::string username) {
        return "server_" + std::to_string(serverInfo.clusterid()) + "_" +
               std::to_string(serverInfo.serverid()) + "/" + username;
    }

    Status List(ServerContext *context, const Request *request, ListReply *list_reply) override {
        std::string curr_username = request->username();
        // Iterate over the Client DB and add all usernames, and their followers
        for (Client *user : client_db) {
            list_reply->add_all_users(user->username);
            // When we go over the current user's client, we add all the followers
            if (curr_username.compare(user->username) == 0) {
                for (Client *follower : user->client_followers) {
                    list_reply->add_followers(follower->username);
                }
            }
        }
        log(INFO, "List Successful:\t\tRequested by User " + curr_username);
        return Status::OK;
    }

    Status Follow(ServerContext *context, const Request *request, Reply *reply) override {
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
        std::ofstream following(filePrefix(user->username) + "_following.txt", std::ios::app);
        std::ofstream followers(filePrefix(to_follow->username) + "_follower.txt", std::ios::app);
        following << to_follow->username << std::endl;
        followers << user->username << std::endl;
        following.close();
        followers.close();
        return Status::OK;
    }

    Status UnFollow(ServerContext *context, const Request *request, Reply *reply) override {
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

        log(INFO, "Unfollow Successful:\t\tUser " + curr_user + " unfollowed " + username);
        // We return the success code -> 0 SUCCESS
        reply->set_msg("0");
        return Status::OK;
    }

    void removeFromFollowLists(const std::string &username, const std::string &to_remove) {
        std::string following_filename = filePrefix(username) + "_following.txt";
        std::string follower_filename = filePrefix(to_remove) + "_follower.txt";

        // Helper function to remove a user from a file
        auto removeEntryFromFile = [](const std::string &filename, const std::string &entry_to_remove) {
            // Open the original file for reading
            std::ifstream file_in(filename);
            if (!file_in.is_open()) {
                std::cerr << "Unable to open file: " << filename << std::endl;
                return;
            }

            // Open a temporary file for writing the updated content
            std::ofstream temp_out(filename + ".tmp");
            if (!temp_out.is_open()) {
                std::cerr << "Unable to open temp file for writing." << std::endl;
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

    // RPC Login
    Status Login(ServerContext *context, const Request *request, Reply *reply) override {
        // We go over the client_db to check if username already exists, and accordingly return a grpc::Status::ALREADY_EXISTS
        Client *user_index = getClient(request->username());
        if (user_index != NULL) {
            reply->set_msg("Username already exists");
            log(ERROR, "Login Failed:\t\tUsername " + request->username() + " already exists");
            reply->set_msg("1");
            return Status(grpc::StatusCode::ALREADY_EXISTS, "Username already exists");
        }
        Client *client = new Client();
        client->username = request->username();
        client->connected = true;
        client_db.push_back(client);
        truncateFile(client->username);
        reply->set_msg("Login successful");
        log(INFO, "Login Successful:\t\tUser " + client->username);
        return Status::OK;
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

    // Method invoked when unfollowing is successful, from user's timeline, the to_unfollow_user's posts are removed

    // Reads posts from the file and returns them as a vector
    std::vector<Post> parseFileContentForPosts(const std::string &file_path) {
        std::ifstream file_in(file_path);
        std::vector<Post> posts;
        std::string line;

        while (true) {
            std::vector<std::string> lines;

            // Read lines until a blank line (or termination condition) is found
            while (std::getline(file_in, line)) {
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
        std::string timeline_path = filePrefix(username) + "_timeline.txt";

        // Open the timeline file for reading
        std::ifstream file_in(timeline_path);
        if (!file_in.is_open()) {
            std::cerr << "Unable to open timeline file: " << timeline_path << std::endl;
            return;
        }

        // Open a temporary file for writing the updated timeline
        std::ofstream temp_out(timeline_path + ".tmp");
        if (!temp_out.is_open()) {
            std::cerr << "Unable to open temp file for writing." << std::endl;
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
            std::cerr << "Failed to parse the timestamp: " << datetime_str << std::endl;
            return -1;
        }

        // Convert to time_t (which is equivalent to Unix timestamp)
        std::time_t time_since_epoch = std::mktime(&tm);

        // Check if mktime failed
        if (time_since_epoch == -1) {
            std::cerr << "Failed to convert time to timestamp." << std::endl;
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
        std::string timeline_path = filePrefix(username) + "_timeline.txt";
        std::string posts_path = filePrefix(username) + "_posts.txt";
        std::string following_path = filePrefix(username) + "_following.txt";
        std::string follower_path = filePrefix(username) + "_follower.txt";
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
    void displayTimeline(ServerReaderWriter<Message, Message> *stream, Client *user) {
        std::string timeline_path = filePrefix(user->username) + "_timeline.txt";
        std::vector<Post> posts = parseFileContentForPosts(timeline_path);
        // boolean compare method to sort by larger timestamp
        auto compareByTimestamp = [](const Post &a, const Post &b) {
            return a.timestamp > b.timestamp;
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
    }

    // Helper method to make Post for a user
    void makePost(Message new_post, Client *user) {
        // std::string content = new_post.msg();
        std::string file_path = filePrefix(user->username) + "_posts.txt";
        // google::protobuf::Timestamp temptime = message.timestamp();
        // google::protobuf::Timestamp *ts_ptr = new google::protobuf::Timestamp();
        // ts_ptr->CopyFrom(temptime);  // Copy the contents of temptime
        // Message new_post;
        // new_post.set_allocated_timestamp(ts_ptr);
        // new_post.set_msg(content);
        // new_post.set_username(user->username);
        for (Client *follower : user->client_followers) {
            // Send it to all followers
            if (follower->stream) {
                log(INFO, "Streaming:\t\tMessage from User " + new_post.username() + " to User " + follower->username);
                // Using follower's stream to Write the message to follower's Timeline
                follower->stream->Write(new_post);
            }
            std::string follower_timeline_path = filePrefix(follower->username) + "_timeline.txt";
            log(INFO, "Add To File:\t\tMessage from User " + new_post.username() + " to User " + follower->username + "'s timeline file " + follower_timeline_path);
            // Write the message regardless to the follower's timeline file
            addToFile(follower_timeline_path, new_post);
        }
        std::string posts_file_path = filePrefix(user->username) + "_posts.txt";
        log(INFO, "Add to File:\t\tMessage from User " + user->username + " to User " + user->username + "'s posts file " + posts_file_path);
        addToFile(posts_file_path, new_post);
    }

    Status Timeline(ServerContext *context, ServerReaderWriter<Message, Message> *stream) override {
        Message message;
        Client *user = nullptr;
        while (stream->Read(&message)) {
            // std::string input = message.msg();
            user = getClient(message.username());
            user->stream = stream;
            // isInitialTimelineRequest is defaulted in struct Client to true, whenever user initiates timeline, the first time, timeline displayed
            if (user->isInitialTimelineRequest) {
                log(INFO, "Timeline Request:\t\tUser " + message.username());
                user = getClient(message.username());
                // Helper Method to get display timeline Posts of user
                displayTimeline(stream, user);
                // After first time, the user is not shown the timeline
                user->isInitialTimelineRequest = false;
            } else {
                // Helper Method to Broadcast the New Post to all followers and write to the respective files
                makePost(message, user);
            }
        }
        return Status::OK;
    }
};

void createFolder(const std::string &path) {
    std::filesystem::path dirPath(path);

    // Check if the directory already exists
    if (std::filesystem::exists(dirPath)) {
        std::cout << "Folder already exists: " << path << std::endl;
    } else {
        // Create the folder
        if (std::filesystem::create_directory(dirPath)) {
            std::cout << "Folder created: " << path << std::endl;
        } else {
            std::cerr << "Error creating folder: " << path << std::endl;
        }
    }
}

void heartbeat(csce662::ServerInfo serverInfo, std::string coordinator_ip, std::string coordinator_port) {
    try {
        bool isRegisterHeartBeat = true;

        std::cout << "Entering heartbeat loop..." << std::endl
                  << std::flush;

        while (true) {
            std::string coordinator_address = coordinator_ip + ":" + coordinator_port;
            std::cout << "Making a stub for coordinator at: " << coordinator_address << std::endl
                      << std::flush;

            // Make the stub to call the coordinator
            std::unique_ptr<CoordService::Stub> stub = CoordService::NewStub(grpc::CreateChannel(coordinator_address, grpc::InsecureChannelCredentials()));

            // Check if stub is created successfully
            if (!stub) {
                std::cerr << "Failed to create stub for coordinator at: " << coordinator_address << std::endl
                          << std::flush;
                return;
            }

            // Create the request
            // ServerInfo serverInfo;
            std::cout << "Creating ServerInfo object..." << std::endl
                      << std::flush;

            // Call the Register method
            grpc::ClientContext context;
            Confirmation reply;
            std::cout << "Making Heartbeat call..." << std::endl
                      << std::flush;
            Status status = stub->Heartbeat(&context, serverInfo, &reply);
            std::cout << "Made a call to Heartbeat." << std::endl
                      << std::flush;  // Log after the call

            if (status.ok()) {
                if (isRegisterHeartBeat) {
                    std::cout << "Server registered successfully with coordinator." << std::endl
                              << std::flush;
                    isRegisterHeartBeat = false;
                    std::string serverFolder = "./server_" + std::to_string(serverInfo.clusterid()) + "_" + std::to_string(serverInfo.serverid());
                    createFolder(serverFolder);
                } else {
                    std::cout << "Received a success from coordinator" << std::endl
                              << std::flush;
                }
            } else {
                std::cerr << "Failed to register with coordinator: " << status.error_message() << std::endl
                          << std::flush;
            }
            std::this_thread::sleep_for(std::chrono::seconds(5));  // Sleep before next heartbeat
        }
    } catch (const std::exception &e) {
        std::cerr << "Exception caught in heartbeat: " << e.what() << std::endl
                  << std::flush;
    }
}

void RunServer(std::string server_id, std::string server_port, std::string cluster_id, std::string coordinator_ip, std::string coordinator_port) {
    std::string server_address = "0.0.0.0:" + server_port;
    SNSServiceImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    serverInfo.set_clusterid(std::stoi(cluster_id));
    serverInfo.set_serverid(std::stoi(server_id));
    serverInfo.set_hostname("localhost");
    serverInfo.set_port(server_port);
    serverInfo.set_type("master");
    std::thread heartbeat_thread(heartbeat, serverInfo, coordinator_ip, coordinator_port);
    heartbeat_thread.detach();
    server->Wait();
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

    std::string log_file_name = std::string("server-") + port;
    google::InitGoogleLogging(log_file_name.c_str());
    log(INFO, "Logging Initialized. Server starting...");
    RunServer(server_id, port, cluster_id, coordinator_ip, coordinator_port);
    // RunServer(port);
    return 0;
}
