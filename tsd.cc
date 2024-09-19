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
#include <fstream>
#include <iostream>
#include <memory>
#include <string>

// #include "json.hpp"
#define log(severity, msg) \
    LOG(severity) << msg;  \
    google::FlushLogFiles(google::severity);

#include "sns.grpc.pb.h"

#define DELIMITER "\x1F"

// using json = nlohmann::json;

using csce662::ListReply;
using csce662::Message;
using csce662::Reply;
using csce662::Request;
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

class SNSServiceImpl final : public SNSService::Service {
    Client *getClient(std::string username) {
        for (Client *client : client_db) {
            if (client->username.compare(username) == 0) {
                return client;
            }
        }
        return NULL;
    }
    Status List(ServerContext *context, const Request *request, ListReply *list_reply) override {
        for (Client *client : client_db) {
            log(INFO, client->username);
            list_reply->add_all_users(client->username);
            for (Client *follower : client->client_followers) {
                list_reply->add_followers(follower->username);
            }
        }
        return Status::OK;
    }

    Status Follow(ServerContext *context, const Request *request, Reply *reply) override {
        std::string username = request->username();
        std::string username2 = request->arguments().Get(0);
        if (username.compare(username2) == 0) {
            log(INFO, "Invalid username, already exists");
            reply->set_msg("1");
            return Status::OK;
            // return Status(grpc::StatusCode::ALREADY_EXISTS, "Invalid Username, already exists");
        }
        Client *user1 = getClient(username);
        Client *user2 = getClient(username2);
        if (user1 == NULL || user2 == NULL) {
            log(INFO, "Invalid username");
            reply->set_msg("3");
            return Status::OK;
            // return Status(grpc::StatusCode::ALREADY_EXISTS, "Invalid Username, already exists");
        }
        // reply->set_comm_status(0);
        reply->set_msg("0");
        user1->client_following.push_back(user2);
        user2->client_followers.push_back(user1);
        return Status::OK;
    }

    Status UnFollow(ServerContext *context, const Request *request, Reply *reply) override {
        std::string username = request->username();
        std::string username2 = request->arguments().Get(0);
        if (username.compare(username2) == 0) {
            log(INFO, "Username are the same");
            reply->set_msg("3");
            return Status::OK;
        }
        Client *user1 = getClient(username);
        Client *user2 = getClient(username2);
        int following_index = -1;
        log(INFO, "user1 Following Size: " + user1->client_following.size());
        for (int i = 0; i < user1->client_following.size(); i++) {
            if (user1->client_following[i] == user2) {
                log(INFO, "Following Index");
                following_index = i;
                break;
            }
        }
        int follower_index = -1;
        log(INFO, "user2 Followers Size:" + user2->client_followers.size());
        for (int i = 0; i < user2->client_followers.size(); i++) {
            if (user2->client_followers[i] == user1) {
                log(INFO, "Follower Index");
                follower_index = i;
                break;
            }
        }
        log(INFO, "Got the Index");
        log(INFO, "Indexes: " + follower_index + following_index);
        if (following_index == -1 || follower_index == -1) {
            reply->set_msg("3");
            return Status::OK;
        }
        log(INFO, "Follower Index" + follower_index + following_index);
        // TO Remove the Posts from Timeline of the Follower of the user1
        removePostsFromTimeline(user1->username, user2->username);
        user1->client_following.erase(user1->client_following.begin() + following_index);
        user2->client_followers.erase(user2->client_followers.begin() + follower_index);
        reply->set_msg("0");
        return Status::OK;
    }

    // RPC Login
    Status Login(ServerContext *context, const Request *request, Reply *reply) override {
        for (Client *client : client_db) {
            if (client->username == request->username()) {
                reply->set_msg("Username already exists");
                log(INFO, "Username already exists\t" + request->username());
                reply->set_msg("1");
                return Status(grpc::StatusCode::ALREADY_EXISTS, "Username already exists");;
            }
        }
        Client *client = new Client();
        client->username = request->username();
        client->connected = true;
        client_db.push_back(client);
        truncateFile(client->username);
        reply->set_msg("Login successful");
        log(INFO, "New client logged in: " + client->username);
        return Status::OK;
    }

    google::protobuf::Timestamp *createProtoTimestampFromEpoch(long long epoch_seconds) {
        google::protobuf::Timestamp *ts = new google::protobuf::Timestamp();
        ts->set_seconds(epoch_seconds);
        ts->set_nanos(0);  // Assuming no nanoseconds info
        return ts;
    }

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

    std::string decodeNewLineChar(const std::string &content) {
        std::string decoded_content = content;
        size_t pos = 0;
        while ((pos = decoded_content.find("\\n", pos)) != std::string::npos) {
            decoded_content.replace(pos, 2, "\n");
            pos += 1;  // Move past the replaced "\n"
        }
        return decoded_content;
    }

    std::string encodeNewLineChar(const std::string &content) {
        std::string encoded_content = content;
        size_t pos = 0;
        while ((pos = encoded_content.find('\n', pos)) != std::string::npos) {
            encoded_content.replace(pos, 1, "\\n");
            pos += 2;  // Move past the replaced "\n"
        }
        return encoded_content;
    }

    std::vector<Post> parseFileContent(const std::string &file_path) {
        std::ifstream file(file_path);
        std::vector<Post> posts;
        if (!file) {
            log(ERROR, "Error opening file: " + file_path);
            return posts;
        }
        if (file.peek() == std::ifstream::traits_type::eof()) {
            log(ERROR, "File is empty: " + file_path);
            return posts;
        }
        std::string line;
        while (std::getline(file, line)) {
            auto tokens = split(line, DELIMITER);
            if (tokens.size() == 3) {
                Post post;
                post.username = tokens[0];
                post.content = decodeNewLineChar(tokens[1]);
                try {
                    post.timestamp = std::stoll(tokens[2]);
                } catch (const std::exception &e) {
                    log(ERROR, "Error converting timestamp: " + std::string(e.what()));
                    continue;  // Skip this line if conversion fails
                }
                posts.push_back(post);
            } else {
                log(ERROR, "Invalid data format in file: " + file_path);
            }
        }
        file.close();
        return posts;
    }

    void addToFile(std::string file_path, Message message) {
        Post post;
        post.username = message.username();
        post.content = message.msg();
        post.timestamp = message.timestamp().seconds();
        // Overwrite the file with updated JSON
        std::ofstream file(file_path, std::ios::app);
        if (!file) {
            log(ERROR, "Error opening file: " + file_path);
            return;
        }
        file << post.username << DELIMITER
             << encodeNewLineChar(post.content) << DELIMITER
             << post.timestamp << std::endl;
        file.close();
    }

    void removePostsFromTimeline(const std::string username, const std::string unfollowed_user) {
        std::string timeline_path = "./timeline_" + username + ".txt";
        std::vector<Post> posts = parseFileContent(timeline_path);
        std::vector<Post> new_posts;
        for (const auto &post : posts) {
            // Filter out unfollowed users
            if (post.username != unfollowed_user) {
                new_posts.push_back(post);
            }
        }
        std::ofstream Timeline(timeline_path);
        if (!Timeline.is_open()) {
            std::cerr << "Error opening file for writing." << std::endl;
            return;
        }
        log(INFO, "We have removed the " + unfollowed_user + "'s posts from " + username + "'s timeline");
        for (const auto &post : new_posts) {
            Timeline << post.username << DELIMITER
                     << encodeNewLineChar(post.content) << DELIMITER
                     << post.timestamp << std::endl;
        }
        Timeline.close();  // Close the file after writing
    }

    void truncateFile(const std::string username) {
        // Open the file in truncate mode
        std::string timeline_path = "./timeline_" + username + ".txt";
        std::string posts_path = "./posts_" + username + ".txt";
        std::ofstream timeline(timeline_path, std::ios::trunc);
        std::ofstream post(posts_path, std::ios::trunc);
        if (!timeline) {
            log(ERROR, "Error opening file: " + timeline_path);
            return;
        }
        log(INFO, "Truncated file: " + timeline_path);
        if (!post) {
            log(ERROR, "Error opening file: " + posts_path);
            return;
        }
        log(INFO, "Truncated file: " + posts_path);
        timeline.close();
        post.close();
    }

    void displayTimeline(ServerReaderWriter<Message, Message> *stream, Client *user) {
        std::string timeline_path = "./timeline_" + user->username + ".txt";
        std::vector<Post> posts = parseFileContent(timeline_path);
        log(INFO, "Read the timeline file");
        // Load messages from JSON
        auto compareByTimestamp = [](const Post &a, const Post &b) {
            return a.timestamp > b.timestamp;
        };
        std::sort(posts.begin(), posts.end(), compareByTimestamp);
        // std::ostringstream output_stream;
        int length = posts.size();
        int posts_size = std::min(length, 20);
        // Display the messages
        Message msg;
        for (int idx = 0; idx < posts_size; idx++) {
            const auto &post = posts[idx];
            msg.set_allocated_timestamp(createProtoTimestampFromEpoch(post.timestamp));
            msg.set_username(post.username);
            msg.set_msg(post.content);
            // output_stream << "T " << msg.timestamp << std::endl;
            // output_stream << "U " << msg.username << std::endl;
            // output_stream << "W " << msg.content << std::endl;
            // output_stream << std::endl; // Empty line
            stream->Write(msg);
        }
        log(INFO, "We have gotten the timeline now, please check");
    }

    void makePost(Message message, Client *user) {
        std::string content = message.msg();
        std::string file_path = "./posts_" + user->username + ".txt";
        google::protobuf::Timestamp temptime = message.timestamp();
        google::protobuf::Timestamp *ts_ptr = new google::protobuf::Timestamp();
        ts_ptr->CopyFrom(temptime);  // Copy the contents of temptime
        Message new_post;
        new_post.set_allocated_timestamp(ts_ptr);
        new_post.set_msg(content);
        new_post.set_username(user->username);
        for (Client *follower : user->client_followers) {
            // Send it to all followers
            if (follower->stream) {
                log(INFO, "Streaming " + message.msg() + " from " + message.username() + " to " + follower->username);
                follower->stream->Write(new_post);
            }
            std::string follower_timeline_path = "./timeline_" + follower->username + ".txt";
            log(INFO, "Saving " + message.msg() + " from " + message.username() + " to " + follower->username + " in timeline");
            addToFile(follower_timeline_path, new_post);

        }
        std::string posts_file_path = "./posts_" + user->username + ".txt";
        addToFile(posts_file_path, new_post);
    }

    Status Timeline(ServerContext *context, ServerReaderWriter<Message, Message> *stream) override {
        Message message;
        Client *user = nullptr;
        while (stream->Read(&message)) {
            std::string input = message.msg();
            log(INFO, "Received message " + message.msg() + " from " + message.username()) user = getClient(message.username());
            user->stream = stream;
            if (user->isInitialTimelineRequest) {
                displayTimeline(stream, user);
                user->isInitialTimelineRequest = false;
                // Get all timelines from the user's wall file
            } else {
                makePost(message, user);
            }
        }
        return Status::OK;
    }
};

void RunServer(std::string port_no) {
    std::string server_address = "0.0.0.0:" + port_no;
    SNSServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    log(INFO, "Server listening on " + server_address);

    server->Wait();
}

int main(int argc, char **argv) {
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

    std::string log_file_name = std::string("server-") + port;
    google::InitGoogleLogging(log_file_name.c_str());
    log(INFO, "Logging Initialized. Server starting...");
    RunServer(port);

    return 0;
}
