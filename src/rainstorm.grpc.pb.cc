// Generated by the gRPC C++ plugin.
// If you make any local change, they will be lost.
// source: rainstorm.proto

#include "rainstorm.pb.h"
#include "rainstorm.grpc.pb.h"

#include <functional>
#include <grpcpp/support/async_stream.h>
#include <grpcpp/support/async_unary_call.h>
#include <grpcpp/impl/channel_interface.h>
#include <grpcpp/impl/client_unary_call.h>
#include <grpcpp/support/client_callback.h>
#include <grpcpp/support/message_allocator.h>
#include <grpcpp/support/method_handler.h>
#include <grpcpp/impl/rpc_service_method.h>
#include <grpcpp/support/server_callback.h>
#include <grpcpp/impl/server_callback_handlers.h>
#include <grpcpp/server_context.h>
#include <grpcpp/impl/service_type.h>
#include <grpcpp/support/sync_stream.h>
namespace rainstorm {

static const char* RainstormService_method_names[] = {
  "/rainstorm.RainstormService/NewSrcTask",
  "/rainstorm.RainstormService/NewStageTask",
  "/rainstorm.RainstormService/UpdateTaskSnd",
  "/rainstorm.RainstormService/SendDataChunks",
};

std::unique_ptr< RainstormService::Stub> RainstormService::NewStub(const std::shared_ptr< ::grpc::ChannelInterface>& channel, const ::grpc::StubOptions& options) {
  (void)options;
  std::unique_ptr< RainstormService::Stub> stub(new RainstormService::Stub(channel, options));
  return stub;
}

RainstormService::Stub::Stub(const std::shared_ptr< ::grpc::ChannelInterface>& channel, const ::grpc::StubOptions& options)
  : channel_(channel), rpcmethod_NewSrcTask_(RainstormService_method_names[0], options.suffix_for_stats(),::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  , rpcmethod_NewStageTask_(RainstormService_method_names[1], options.suffix_for_stats(),::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  , rpcmethod_UpdateTaskSnd_(RainstormService_method_names[2], options.suffix_for_stats(),::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  , rpcmethod_SendDataChunks_(RainstormService_method_names[3], options.suffix_for_stats(),::grpc::internal::RpcMethod::BIDI_STREAMING, channel)
  {}

::grpc::Status RainstormService::Stub::NewSrcTask(::grpc::ClientContext* context, const ::rainstorm::NewSrcTaskRequest& request, ::rainstorm::OperationStatus* response) {
  return ::grpc::internal::BlockingUnaryCall< ::rainstorm::NewSrcTaskRequest, ::rainstorm::OperationStatus, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), rpcmethod_NewSrcTask_, context, request, response);
}

void RainstormService::Stub::async::NewSrcTask(::grpc::ClientContext* context, const ::rainstorm::NewSrcTaskRequest* request, ::rainstorm::OperationStatus* response, std::function<void(::grpc::Status)> f) {
  ::grpc::internal::CallbackUnaryCall< ::rainstorm::NewSrcTaskRequest, ::rainstorm::OperationStatus, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_NewSrcTask_, context, request, response, std::move(f));
}

void RainstormService::Stub::async::NewSrcTask(::grpc::ClientContext* context, const ::rainstorm::NewSrcTaskRequest* request, ::rainstorm::OperationStatus* response, ::grpc::ClientUnaryReactor* reactor) {
  ::grpc::internal::ClientCallbackUnaryFactory::Create< ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_NewSrcTask_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::rainstorm::OperationStatus>* RainstormService::Stub::PrepareAsyncNewSrcTaskRaw(::grpc::ClientContext* context, const ::rainstorm::NewSrcTaskRequest& request, ::grpc::CompletionQueue* cq) {
  return ::grpc::internal::ClientAsyncResponseReaderHelper::Create< ::rainstorm::OperationStatus, ::rainstorm::NewSrcTaskRequest, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), cq, rpcmethod_NewSrcTask_, context, request);
}

::grpc::ClientAsyncResponseReader< ::rainstorm::OperationStatus>* RainstormService::Stub::AsyncNewSrcTaskRaw(::grpc::ClientContext* context, const ::rainstorm::NewSrcTaskRequest& request, ::grpc::CompletionQueue* cq) {
  auto* result =
    this->PrepareAsyncNewSrcTaskRaw(context, request, cq);
  result->StartCall();
  return result;
}

::grpc::Status RainstormService::Stub::NewStageTask(::grpc::ClientContext* context, const ::rainstorm::NewStageTaskRequest& request, ::rainstorm::OperationStatus* response) {
  return ::grpc::internal::BlockingUnaryCall< ::rainstorm::NewStageTaskRequest, ::rainstorm::OperationStatus, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), rpcmethod_NewStageTask_, context, request, response);
}

void RainstormService::Stub::async::NewStageTask(::grpc::ClientContext* context, const ::rainstorm::NewStageTaskRequest* request, ::rainstorm::OperationStatus* response, std::function<void(::grpc::Status)> f) {
  ::grpc::internal::CallbackUnaryCall< ::rainstorm::NewStageTaskRequest, ::rainstorm::OperationStatus, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_NewStageTask_, context, request, response, std::move(f));
}

void RainstormService::Stub::async::NewStageTask(::grpc::ClientContext* context, const ::rainstorm::NewStageTaskRequest* request, ::rainstorm::OperationStatus* response, ::grpc::ClientUnaryReactor* reactor) {
  ::grpc::internal::ClientCallbackUnaryFactory::Create< ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_NewStageTask_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::rainstorm::OperationStatus>* RainstormService::Stub::PrepareAsyncNewStageTaskRaw(::grpc::ClientContext* context, const ::rainstorm::NewStageTaskRequest& request, ::grpc::CompletionQueue* cq) {
  return ::grpc::internal::ClientAsyncResponseReaderHelper::Create< ::rainstorm::OperationStatus, ::rainstorm::NewStageTaskRequest, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), cq, rpcmethod_NewStageTask_, context, request);
}

::grpc::ClientAsyncResponseReader< ::rainstorm::OperationStatus>* RainstormService::Stub::AsyncNewStageTaskRaw(::grpc::ClientContext* context, const ::rainstorm::NewStageTaskRequest& request, ::grpc::CompletionQueue* cq) {
  auto* result =
    this->PrepareAsyncNewStageTaskRaw(context, request, cq);
  result->StartCall();
  return result;
}

::grpc::Status RainstormService::Stub::UpdateTaskSnd(::grpc::ClientContext* context, const ::rainstorm::UpdateTaskSndRequest& request, ::rainstorm::OperationStatus* response) {
  return ::grpc::internal::BlockingUnaryCall< ::rainstorm::UpdateTaskSndRequest, ::rainstorm::OperationStatus, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), rpcmethod_UpdateTaskSnd_, context, request, response);
}

void RainstormService::Stub::async::UpdateTaskSnd(::grpc::ClientContext* context, const ::rainstorm::UpdateTaskSndRequest* request, ::rainstorm::OperationStatus* response, std::function<void(::grpc::Status)> f) {
  ::grpc::internal::CallbackUnaryCall< ::rainstorm::UpdateTaskSndRequest, ::rainstorm::OperationStatus, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_UpdateTaskSnd_, context, request, response, std::move(f));
}

void RainstormService::Stub::async::UpdateTaskSnd(::grpc::ClientContext* context, const ::rainstorm::UpdateTaskSndRequest* request, ::rainstorm::OperationStatus* response, ::grpc::ClientUnaryReactor* reactor) {
  ::grpc::internal::ClientCallbackUnaryFactory::Create< ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_UpdateTaskSnd_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::rainstorm::OperationStatus>* RainstormService::Stub::PrepareAsyncUpdateTaskSndRaw(::grpc::ClientContext* context, const ::rainstorm::UpdateTaskSndRequest& request, ::grpc::CompletionQueue* cq) {
  return ::grpc::internal::ClientAsyncResponseReaderHelper::Create< ::rainstorm::OperationStatus, ::rainstorm::UpdateTaskSndRequest, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), cq, rpcmethod_UpdateTaskSnd_, context, request);
}

::grpc::ClientAsyncResponseReader< ::rainstorm::OperationStatus>* RainstormService::Stub::AsyncUpdateTaskSndRaw(::grpc::ClientContext* context, const ::rainstorm::UpdateTaskSndRequest& request, ::grpc::CompletionQueue* cq) {
  auto* result =
    this->PrepareAsyncUpdateTaskSndRaw(context, request, cq);
  result->StartCall();
  return result;
}

::grpc::ClientReaderWriter< ::rainstorm::StreamDataChunk, ::rainstorm::AckDataChunk>* RainstormService::Stub::SendDataChunksRaw(::grpc::ClientContext* context) {
  return ::grpc::internal::ClientReaderWriterFactory< ::rainstorm::StreamDataChunk, ::rainstorm::AckDataChunk>::Create(channel_.get(), rpcmethod_SendDataChunks_, context);
}

void RainstormService::Stub::async::SendDataChunks(::grpc::ClientContext* context, ::grpc::ClientBidiReactor< ::rainstorm::StreamDataChunk,::rainstorm::AckDataChunk>* reactor) {
  ::grpc::internal::ClientCallbackReaderWriterFactory< ::rainstorm::StreamDataChunk,::rainstorm::AckDataChunk>::Create(stub_->channel_.get(), stub_->rpcmethod_SendDataChunks_, context, reactor);
}

::grpc::ClientAsyncReaderWriter< ::rainstorm::StreamDataChunk, ::rainstorm::AckDataChunk>* RainstormService::Stub::AsyncSendDataChunksRaw(::grpc::ClientContext* context, ::grpc::CompletionQueue* cq, void* tag) {
  return ::grpc::internal::ClientAsyncReaderWriterFactory< ::rainstorm::StreamDataChunk, ::rainstorm::AckDataChunk>::Create(channel_.get(), cq, rpcmethod_SendDataChunks_, context, true, tag);
}

::grpc::ClientAsyncReaderWriter< ::rainstorm::StreamDataChunk, ::rainstorm::AckDataChunk>* RainstormService::Stub::PrepareAsyncSendDataChunksRaw(::grpc::ClientContext* context, ::grpc::CompletionQueue* cq) {
  return ::grpc::internal::ClientAsyncReaderWriterFactory< ::rainstorm::StreamDataChunk, ::rainstorm::AckDataChunk>::Create(channel_.get(), cq, rpcmethod_SendDataChunks_, context, false, nullptr);
}

RainstormService::Service::Service() {
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      RainstormService_method_names[0],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< RainstormService::Service, ::rainstorm::NewSrcTaskRequest, ::rainstorm::OperationStatus, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(
          [](RainstormService::Service* service,
             ::grpc::ServerContext* ctx,
             const ::rainstorm::NewSrcTaskRequest* req,
             ::rainstorm::OperationStatus* resp) {
               return service->NewSrcTask(ctx, req, resp);
             }, this)));
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      RainstormService_method_names[1],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< RainstormService::Service, ::rainstorm::NewStageTaskRequest, ::rainstorm::OperationStatus, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(
          [](RainstormService::Service* service,
             ::grpc::ServerContext* ctx,
             const ::rainstorm::NewStageTaskRequest* req,
             ::rainstorm::OperationStatus* resp) {
               return service->NewStageTask(ctx, req, resp);
             }, this)));
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      RainstormService_method_names[2],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< RainstormService::Service, ::rainstorm::UpdateTaskSndRequest, ::rainstorm::OperationStatus, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(
          [](RainstormService::Service* service,
             ::grpc::ServerContext* ctx,
             const ::rainstorm::UpdateTaskSndRequest* req,
             ::rainstorm::OperationStatus* resp) {
               return service->UpdateTaskSnd(ctx, req, resp);
             }, this)));
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      RainstormService_method_names[3],
      ::grpc::internal::RpcMethod::BIDI_STREAMING,
      new ::grpc::internal::BidiStreamingHandler< RainstormService::Service, ::rainstorm::StreamDataChunk, ::rainstorm::AckDataChunk>(
          [](RainstormService::Service* service,
             ::grpc::ServerContext* ctx,
             ::grpc::ServerReaderWriter<::rainstorm::AckDataChunk,
             ::rainstorm::StreamDataChunk>* stream) {
               return service->SendDataChunks(ctx, stream);
             }, this)));
}

RainstormService::Service::~Service() {
}

::grpc::Status RainstormService::Service::NewSrcTask(::grpc::ServerContext* context, const ::rainstorm::NewSrcTaskRequest* request, ::rainstorm::OperationStatus* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

::grpc::Status RainstormService::Service::NewStageTask(::grpc::ServerContext* context, const ::rainstorm::NewStageTaskRequest* request, ::rainstorm::OperationStatus* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

::grpc::Status RainstormService::Service::UpdateTaskSnd(::grpc::ServerContext* context, const ::rainstorm::UpdateTaskSndRequest* request, ::rainstorm::OperationStatus* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

::grpc::Status RainstormService::Service::SendDataChunks(::grpc::ServerContext* context, ::grpc::ServerReaderWriter< ::rainstorm::AckDataChunk, ::rainstorm::StreamDataChunk>* stream) {
  (void) context;
  (void) stream;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}


}  // namespace rainstorm

