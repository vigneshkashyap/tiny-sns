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
        log(INFO, "Unfollow Successful:\t\tUser " + curr_user + " unfollowed " + username);
        // We return the success code -> 0 SUCCESS
        reply->set_msg("0");
        return Status::OK;
    }

    // RPC Login
    Status Login(ServerContext *context, const Request *request, Reply *reply) override {
        // We go over the client_db to check if username already exists, and accordingly return a grpc::Status::ALREADY_EXISTS
        for (Client *client : client_db) {
            if (client->username == request->username()) {
                reply->set_msg("Username already exists");
                log(ERROR, "Login Failed:\t\tUsername " + request->username() + " already exists");
                reply->set_msg("1");
                return Status(grpc::StatusCode::ALREADY_EXISTS, "Username already exists");
                ;
            }
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

    // Helper Method to parse the file, and return a vector of posts, that contain the post details
    std::vector<Post> parseFileContent(const std::string &file_path) {
        std::ifstream file(file_path);
        std::vector<Post> posts;
        if (!file) {
            log(ERROR, "Error opening file:\t\t" + file_path);
            return posts;
        }
        if (file.peek() == std::ifstream::traits_type::eof()) {
            log(ERROR, "File is empty:\t\t" + file_path);
            return posts;
        }
        std::string line;
        while (std::getline(file, line)) {
            // Invoke the split helper method to get a vector of strings
            auto tokens = split(line, DELIMITER);
            if (tokens.size() == 3) {
                Post post;
                post.username = tokens[0];
                //
                post.content = decodeNewLineChar(tokens[1]);
                try {
                    // Timestamp in Post struct is long long, hence we convert from string to long long
                    post.timestamp = std::stoll(tokens[2]);
                } catch (const std::exception &e) {
                    log(ERROR, "Error converting timestamp:\t\t" + std::string(e.what()));
                    continue;
                }
                posts.push_back(post);
            } else {
                log(ERROR, "Invalid data format in file:\t\t" + file_path);
            }
        }
        file.close();
        return posts;
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
        // Add the Post content using the defined DELIMITER
        file << post.username << DELIMITER
             << encodeNewLineChar(post.content) << DELIMITER
             << post.timestamp << std::endl;
        file.close();
    }

    // Method invoked when unfollowing is successful, from user's timeline, the to_unfollow_user's posts are removed
    void removePostsFromTimeline(const std::string username, const std::string unfollowed_user) {
        std::string timeline_path = "./timeline_" + username + ".txt";
        std::vector<Post> posts = parseFileContent(timeline_path);
        std::vector<Post> new_posts;
        for (const auto &post : posts) {
            // Filter the posts that are not the unfollowed_user's
            if (post.username != unfollowed_user) {
                new_posts.push_back(post);
            }
        }
        // Open the timeline file and non-append mode, thereby truncating the data
        std::ofstream Timeline(timeline_path);
        if (!Timeline.is_open()) {
            std::cerr << "Error opening file for writing." << std::endl;
            return;
        }
        log(INFO, "Remove Posts:\t\tUser " + unfollowed_user + "'s posts removed from User " + username + "'s timeline");
        // Iterate over all posts and write all posts
        for (const auto &post : new_posts) {
            Timeline << post.username << DELIMITER
                     << encodeNewLineChar(post.content) << DELIMITER
                     << post.timestamp << std::endl;
        }
        Timeline.close();
    }

    // When logging in a user, below helper method turncates timeline and posts file of user
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
        // log(INFO, "Truncated file:\\t" + timeline_path);
        if (!post) {
            log(ERROR, "Error opening file: " + posts_path);
            return;
        }
        // log(INFO, "Truncated file:\\t" + posts_path);
        timeline.close();
        post.close();
    }

    // Helper method that uses the ServerReaderWriter stream of client, and Client, to get Timeline Posts, sort by timestamp and write them to client
    void displayTimeline(ServerReaderWriter<Message, Message> *stream, Client *user) {
        std::string timeline_path = "./timeline_" + user->username + ".txt";
        std::vector<Post> posts = parseFileContent(timeline_path);
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

            // Optional Output defined in 1.8 of MP1

            // output_stream << "T " << msg.timestamp << std::endl;
            // output_stream << "U " << msg.username << std::endl;
            // output_stream << "W " << msg.content << std::endl;
            // output_stream << std::endl;

            // Use client's stream to write the timeline post
            stream->Write(msg);
        }
        log(INFO, "Display Timeline Successful:\t\tUser " + user->username + " has " + std::to_string(posts_size) + " posts");
    }

    // Helper method to make Post for a user
    void makePost(Message new_post, Client *user) {
        // std::string content = new_post.msg();
        std::string file_path = "./posts_" + user->username + ".txt";
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
                log(INFO, "Streaming:\t\t" + new_post.msg() + " from User " + new_post.username() + " to User " + follower->username);
                // Using follower's stream to Write the message to follower's Timeline
                follower->stream->Write(new_post);
            }
            std::string follower_timeline_path = "./timeline_" + follower->username + ".txt";
            log(INFO, "Add To File:\t\t" + new_post.msg() + " from User " + new_post.username() + " to timeline file " + follower_timeline_path);
            // Write the message regardless to the follower's timeline file
            addToFile(follower_timeline_path, new_post);
        }
        std::string posts_file_path = "./posts_" + user->username + ".txt";
        log(INFO, "Add to File:\t\t" + new_post.msg() + " from User " + new_post.username() + " to posts file " + posts_file_path);
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
