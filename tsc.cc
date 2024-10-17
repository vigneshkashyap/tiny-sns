#include <grpc++/grpc++.h>
#include <unistd.h>

#include <csignal>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "client.h"
#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"
using csce662::ListReply;
using csce662::Message;
using csce662::Reply;
using csce662::Request;
using csce662::SNSService;
using csce662::ID;
using csce662::ServerInfo;
using csce662::CoordService;

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;

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
        : coordinator_hostname(hname), coordinator_port(p), username(uname) {}

   protected:
    virtual int connectTo();
    virtual IReply processCommand(std::string& input);
    virtual void processTimeline();

   private:
    std::string coordinator_hostname;
    std::string coordinator_port;
    std::string username;
    std::string hostname;
    std::string port;

    // You can have an instance of the client stub
    // as a member variable.
    std::unique_ptr<SNSService::Stub> stub_;
    std::unique_ptr<CoordService::Stub> stub_coordinator;

    IReply Login();
    IReply List();
    IReply Follow(const std::string& username);
    IReply UnFollow(const std::string& username);
    void Timeline(const std::string& username);
};

///////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////
int Client::connectTo() {
    std::shared_ptr<::grpc::ChannelInterface> channel = grpc::CreateChannel(coordinator_hostname + ":" + coordinator_port, grpc::InsecureChannelCredentials());
    stub_coordinator = CoordService::NewStub(channel);
    // Make a connection with coordinator and get the
    ServerInfo serverInfo;
    ClientContext context;
    ID id;
    id.set_id(std::stoi(username));
    grpc::Status status = stub_coordinator->GetServer(&context, id, &serverInfo);
    if (!status.ok()) {
        // grpc::StatusCode status_code = status.error_code();
        // if (status_code == grpc::StatusCode::ALREADY_EXISTS) {
        //     ire.comm_status = FAILURE_INVALID_USERNAME;
        // } else {
        //     ire.comm_status = FAILURE_INVALID;
        return -1;
    }
    hostname = serverInfo.hostname();
    port = serverInfo.port();
    stub_ = SNSService::NewStub(grpc::CreateChannel(hostname + ":" + port, grpc::InsecureChannelCredentials()));
    IReply ire = Login();
    if (!ire.grpc_status.ok()) {
      return -1;
    }
    return 1;
}

IReply Client::processCommand(std::string& input) {
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
        ire.grpc_status = Status::OK;
        ire.comm_status = SUCCESS;
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
    request.set_username(username);
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
        ire.comm_status = FAILURE_UNKNOWN;
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
    int msg_status = std::stoi(reply.msg());
    IStatus comm_status = static_cast<IStatus>(msg_status);
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
    int msg_status = std::stoi(reply.msg());
    IStatus comm_status = static_cast<IStatus>(msg_status);
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
    std::thread writer_thread([stream, &username]() {
        Message message = MakeMessage(username, "Timeline");
        stream->Write(message);
        while (true) {
            std::string text = getPostMessage();
            message = MakeMessage(username, text);
            stream->Write(message);
        }
        stream->WritesDone();
    });
    Message incomingMessage;
    std::thread reader_thread([stream, username]() {
        Message incomingMessage;
        while (stream->Read(&incomingMessage)) {
            google::protobuf::Timestamp timestamp = incomingMessage.timestamp();
            std::time_t time = timestamp.seconds();
            displayPostMessage(incomingMessage.username(), incomingMessage.msg(), time);
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
    std::string coordinator_port = "9090";

    int opt = 0;
    while ((opt = getopt(argc, argv, "h:u:p:k:")) != -1) {
        switch (opt) {
            case 'h':
                hostname = optarg;
                break;
            case 'u':
                username = optarg;
                break;
            case 'p':
                port = optarg;
                break;
            case 'k':
                coordinator_port = optarg;
                break;
            default:
                std::cout << "Invalid Command Line Argument\n";
        }
    }

    std::cout << "Logging Initialized. Client starting...";

    Client myc(hostname, username, coordinator_port);

    myc.run();

    return 0;
}