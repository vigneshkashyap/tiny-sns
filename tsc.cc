#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <string>
#include <unistd.h>
#include <csignal>
#include <grpc++/grpc++.h>
#include "client.h"
#include <sstream>

#include "sns.grpc.pb.h"
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;
using csce662::Message;
using csce662::ListReply;
using csce662::Request;
using csce662::Reply;
using csce662::SNSService;

void sig_ignore(int sig) {
  std::cout << "Signal caught " + sig;
}

Message MakeMessage(const std::string& username, const std::string& msg) {
    Message m;
    m.set_username(username);
    m.set_msg(msg);
    google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
    timestamp->set_seconds(time(NULL));
    timestamp->set_nanos(0);
    m.set_allocated_timestamp(timestamp);
    return m;
}


class Client : public IClient {
  public:
    Client(const std::string& hname,
    const std::string& uname,
    const std::string& p)
      :hostname(hname), username(uname), port(p) {}


  protected:
    virtual IReply connectTo();
    virtual IReply processCommand(std::string& input);
    virtual void processTimeline();

  private:
    std::string hostname;
    std::string username;
    std::string port;

    // You can have an instance of the client stub
    // as a member variable.
    std::unique_ptr<SNSService::Stub> stub_;

    IReply Login();
    IReply List();
    IReply Follow(const std::string &username);
    IReply UnFollow(const std::string &username);
    void   Timeline(const std::string &username);
};


///////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////
IReply Client::connectTo() {
  // ------------------------------------------------------------
  // In this function, you are supposed to create a stub so that
  // you call service methods in the processCommand/porcessTimeline
  // functions. That is, the stub should be accessible when you want
  // to call any service methods in those functions.
  // Please refer to gRpc tutorial how to create a stub.
  // ------------------------------------------------------------
  std::shared_ptr<::grpc::ChannelInterface> channel = grpc::CreateChannel("localhost:"+port, grpc::InsecureChannelCredentials());
  stub_ = SNSService::NewStub(channel);
  IReply ire = Login();
///////////////////////////////////////////////////////////
// YOUR CODE HERE
//////////////////////////////////////////////////////////

    return ire;
}

IReply Client::processCommand(std::string& input) {
  // ------------------------------------------------------------
  // GUIDE 1:
  // In this function, you are supposed to parse the given input
  // command and create your own message so that you call an
  // appropriate service method. The input command will be one
  // of the followings:
  //
  // FOLLOW <username>
  // UNFOLLOW <username>
  // LIST
  // TIMELINE
  // ------------------------------------------------------------

  // ------------------------------------------------------------
  // GUIDE 2:
  // Then, you should create a variable of IReply structure
  // provided by the client.h and initialize it according to
  // the result. Finally you can finish this function by returning
  // the IReply.
  // ------------------------------------------------------------

  // ------------------------------------------------------------
  // HINT: How to set the IReply?
  // Suppose you have "FOLLOW" service method for FOLLOW command,
  // IReply can be set as follow:
  //
  //     // some codes for creating/initializing parameters for
  //     // service method
  //     IReply ire;
  //     grpc::Status status = stub_->FOLLOW(&context, /* some parameters */);
  //     ire.grpc_status = status;
  //     if (status.ok()) {
  //         ire.comm_status = SUCCESS;
  //     } else {
  //         ire.comm_status = FAILURE_NOT_EXISTS;
  //     }
  //
  //      return ire;
  //
  // IMPORTANT:
  // For the command "LIST", you should set both "all_users" and
  // "following_users" member variable of IReply.
  // ------------------------------------------------------------

  IReply ire;

  std::stringstream s(input);
  std::string command;
  std::string input_username;
  s >> command;
  if (command.compare("FOLLOW") == 0) {
    s >> input_username;
    ire = Follow(input_username);
  } else if (command.compare("UNFOLLOW") == 0) {
    s >> input_username;
    ire = UnFollow(input_username);
  } else if (command.compare("LIST") == 0) {
    ire = List();
  } else if (command.compare("TIMELINE") == 0) {
    Timeline(username);
  }

  return ire;
}


void Client::processTimeline() {
    Timeline(username);
}

// List Command
IReply Client::List() {
    Request request;
    ClientContext context;
    IReply ire;
    ListReply reply;
    grpc::Status status = stub_->List(&context, request, &reply);
    ire.grpc_status = status;
    if (status.ok()) {
        ire.comm_status = SUCCESS;
        for (int i = 0; i < reply.all_users_size(); i++) {
          ire.all_users.push_back(reply.all_users(i));
        }
        for (int i = 0; i < reply.followers_size(); i++) {
          ire.followers.push_back(reply.followers(i));
        }
    } else {
        ire.comm_status = FAILURE_NOT_EXISTS;
    }
    return ire;
}

// Follow Command
IReply Client::Follow(const std::string& username2) {
    IReply ire;
    ClientContext context;
    Request request;
    request.set_username(username);
    request.add_arguments(username2);
    Reply reply;
    grpc::Status status = stub_->Follow(&context, request, &reply);
    ire.grpc_status = status;
    int comm_status_int = reply.comm_status();
    IStatus comm_status = static_cast<IStatus>(comm_status_int);
    ire.comm_status = comm_status;
    return ire;
}

// UNFollow Command
IReply Client::UnFollow(const std::string& username2) {
    IReply ire;
    ClientContext context;
    Request request;
    request.set_username(username);
    request.add_arguments(username2);
    Reply reply;
    grpc::Status status = stub_->UnFollow(&context, request, &reply);
    ire.grpc_status = status;
    int comm_status_int = reply.comm_status();
    IStatus comm_status = static_cast<IStatus>(comm_status_int);
    ire.comm_status = comm_status;
    return ire;
}


// Login Command
IReply Client::Login() {

    IReply ire;
    ClientContext context;
    Request request;
    request.set_username(username);
    Reply reply;
    grpc::Status status = stub_->Login(&context, request, &reply);
    ire.grpc_status = status;
    grpc::StatusCode status_code = status.error_code();
    // ire.comm_status = reply.comm_status();
    int comm_status_int = reply.comm_status();
    IStatus comm_status = static_cast<IStatus>(comm_status_int);
    ire.comm_status = comm_status;
    if (!status.ok()) {
       grpc::StatusCode status_code = status.error_code();
        if (status_code == grpc::StatusCode::ALREADY_EXISTS) {
            ire.comm_status = FAILURE_INVALID_USERNAME;
        } else {
            ire.comm_status = FAILURE_INVALID;
        }
    }
    return ire;
}

// Timeline Command
void Client::Timeline(const std::string& username) {
    // ------------------------------------------------------------
    // IMPORTANT NOTICE:
    //
    // Once a user enter to timeline mode , there is no way
    // to command mode. You don't have to worry about this situation,
    // and you can terminate the client program by pressing
    // CTRL-C (SIGINT)
    // ------------------------------------------------------------
    ClientContext context;
    std::shared_ptr<ClientReaderWriter<Message, Message>> stream(stub_->Timeline(&context));
    std::thread writer_thread ([stream, &username]() {
        Message message = MakeMessage(username, "Timeline");
        stream->Write(message);
        while(true) {
          std::string text = getPostMessage();
          message = MakeMessage(username, text);
          stream->Write(message);
        }
      stream->WritesDone();
    });
    Message incomingMessage;
    std::thread reader_thread ([stream, username]() {
      Message incomingMessage;
      while (stream->Read(&incomingMessage)) {
        // if (incomingMessage.username().compare("$$TIMELINE$$") == 0) {
        //   std::cout<<incomingMessage.
        // } else {
          google::protobuf::Timestamp timestamp = incomingMessage.timestamp();
          std::time_t time = timestamp.seconds();
          displayPostMessage(incomingMessage.username(), incomingMessage.msg(), time);  // Show the retrieved message
        // }
      }
    });
    writer_thread.join();
    reader_thread.join();
}



//////////////////////////////////////////////
// Main Function
/////////////////////////////////////////////
int main(int argc, char** argv) {

  std::string hostname = "localhost";
  std::string username = "default";
  std::string port = "3010";

  int opt = 0;
  while ((opt = getopt(argc, argv, "h:u:p:")) != -1){
    switch(opt) {
    case 'h':
      hostname = optarg;break;
    case 'u':
      username = optarg;break;
    case 'p':
      port = optarg;break;
    default:
      std::cout << "Invalid Command Line Argument\n";
    }
  }

  std::cout << "Logging Initialized. Client starting...";

  Client myc(hostname, username, port);

  myc.run();

  return 0;
}