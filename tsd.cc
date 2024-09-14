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
#include<glog/logging.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity);

#include "sns.grpc.pb.h"


using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce662::Message;
using csce662::ListReply;
using csce662::Request;
using csce662::Reply;
using csce662::SNSService;

struct TimestampDescendingComparator {
    bool operator()(const google::protobuf::Timestamp& lhs, const google::protobuf::Timestamp& rhs) const {
        // First compare by seconds
        if (lhs.seconds() != rhs.seconds()) {
            return lhs.seconds() > rhs.seconds();
        }
        // If seconds are equal, compare by nanoseconds
        return lhs.nanos() > rhs.nanos();
    }
};

struct Client {
  std::string username;
  bool connected = true;
  int following_file_size = 0;
  std::vector<Client*> client_followers;
  std::vector<Client*> client_following;
  ServerReaderWriter<Message, Message>* stream = 0;
  std::map<google::protobuf::Timestamp, csce662::Message, TimestampDescendingComparator> timeline;
  std::map<google::protobuf::Timestamp, csce662::Message, TimestampDescendingComparator> posts;
  bool operator==(const Client& c1) const{
    return (username == c1.username);
  }
};

//Vector that stores every client that has been created
std::vector<Client*> client_db;


class SNSServiceImpl final : public SNSService::Service {

  Client* getClient(std::string username) {
    for (Client *client: client_db) {
      if (client->username.compare(username) == 0) {
        return client;
      }
    }
    return NULL;
  }
  Status List(ServerContext* context, const Request* request, ListReply* list_reply) override {
    for (Client *client: client_db) {
      log(INFO, client->username);
      list_reply->add_all_users(client->username);
      for (Client* follower: client->client_followers) {
        list_reply->add_followers(follower->username);
      }
    }
    return Status::OK;
  }

  Status Follow(ServerContext* context, const Request* request, Reply* reply) override {
    std::string username = request->username();
    std::string username2 = request->arguments().Get(0);
    if (username.compare(username2) == 0) {
      log(INFO, "Invalid username, already exists");
      reply->set_comm_status(1);
      return Status::OK;
      // return Status(grpc::StatusCode::ALREADY_EXISTS, "Invalid Username");
    }
    Client *user1 = getClient(username);
    Client *user2 = getClient(username2);
    if (user1 == NULL || user2 == NULL) {
      log(INFO, "Invalid username, already exists");
      reply->set_comm_status(3);
      return Status::OK;
    }
    reply->set_comm_status(0);
    user1->client_following.push_back(user2);
    user2->client_followers.push_back(user1);
    return Status::OK;
  }

  Status UnFollow(ServerContext* context, const Request* request, Reply* reply) override {
    std::string username = request->username();
    std::string username2 = request->arguments().Get(0);
    if (username.compare(username2) == 0) {
      log(INFO, "Username are the same");
      reply->set_comm_status(3);
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
      reply->set_comm_status(3);
      return Status::OK;
    }
    log(INFO, "Follower Index" + follower_index + following_index);
    user1->client_following.erase(user1->client_following.begin() + following_index);
    user2->client_followers.erase(user2->client_followers.begin() + follower_index);
    reply->set_comm_status(0);
    return Status::OK;
  }

  // RPC Login
  Status Login(ServerContext* context, const Request* request, Reply* reply) override {
    for (Client *client: client_db) {
      if (client->username == request->username()) {
        reply->set_msg("Username already exists");
        log(INFO, "Username already exists\t" + request->username());
        reply->set_comm_status(1);
        return Status::OK;
        // return Status(grpc::StatusCode::ALREADY_EXISTS, "Username already exists");
      }
    }
    Client *client = new Client();
    client->username = request->username();
    client->connected = true;
    client_db.push_back(client);
    reply->set_msg("Login successful");
    log(INFO, "New client logged in: " + client->username);
    return Status::OK;
  }

  Status Timeline(ServerContext* context, ServerReaderWriter<Message, Message>* stream) override {
    Message message;
    Client* user = nullptr;
    while (stream->Read(&message)) {
      std::string input = message.msg();
      log(INFO, "Received message " + message.msg() + " from " + message.username())
      user = getClient(message.username());
      user->stream = stream;
      // If message is "Get Timeline" then go to the files and open the timeline for the requested username and write it using stream
      // Otherwise the message needs to be pushed to all followers who have a valid stream and write to their file
      if (input.compare("Get Timeline") == 0) {
        // Get all timelines from the user's wall file
        std::string wall_file_path = "./wall_" + user->username + ".txt";
        std::vector<std::string> posts;
        std::string post;
        std::ifstream Wall(wall_file_path);
        while(getline(Wall, post)) {
          posts.push_back(post);
        }
        log(INFO, "Posts length " + posts.size());
        Wall.close();
        int index = 0;
        Message msg;
        msg.set_msg(post);
        msg.set_username("vignesh");
        stream->Write(msg);
        // for (int idx = 0; idx <= 20 || posts.size() - idx > 0; idx++) {
        //   msg.set_msg(posts[posts.size() - 1 - idx]);
        //   stream->Write(msg);
        // }
      } else {
        // Manipulate the content to store in file
        std::string post = message.msg();
        // Take the input to save in file of the current user
        std::string file_path = "./posts_" + user->username + ".txt";
        google::protobuf::Timestamp temptime = message.timestamp();
        std::time_t time_var = temptime.seconds();
        std::string timestamp = std::ctime(&time_var);
        timestamp.pop_back();
        std::string file_message = user->username + std::string(" (") + timestamp + std::string(") >> ") + message.msg() + "\n";
        // Identify the followers from the client
        Message new_post;
        google::protobuf::Timestamp* ts = new google::protobuf::Timestamp();
        ts->set_seconds(time(NULL));
        ts->set_nanos(0);
        new_post.set_allocated_timestamp(ts);
        for (Client *follower: user->client_followers) {
          log(INFO, "Broadcasting " + message.msg() + " from " + message.username() + " to " + follower->username);
          // Send it to all followers
          new_post.set_msg(post);
          new_post.set_username(user->username);
          if (follower->stream) {
            log(INFO, "Streaming " + message.msg() + " from " + message.username() + " to " + follower->username);
            follower->stream->Write(new_post);
          }
          std::string follower_wall = "./wall_" + follower->username + ".txt";
          std::ofstream FollowerWall(follower_wall);
          // Append it to all the followers wall file
          FollowerWall << file_message;
          FollowerWall.close();
        }
      }
    }
    return Status::OK;
    }
};

void RunServer(std::string port_no) {
  std::string server_address = "0.0.0.0:"+port_no;
  SNSServiceImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  log(INFO, "Server listening on "+server_address);

  server->Wait();
}

int main(int argc, char** argv) {

  std::string port = "3010";

  int opt = 0;
  while ((opt = getopt(argc, argv, "p:")) != -1){
    switch(opt) {
      case 'p':
          port = optarg;break;
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
