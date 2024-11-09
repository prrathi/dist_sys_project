#ifndef FILE_TRANSFER_SERVER_H
#define FILE_TRANSFER_SERVER_H

#include <grpcpp/grpcpp.h>
#include "hydfs.grpc.pb.h"

class FileTransferServiceImpl final : public filetransfer::FileTransferService::Service {
public:
    FileTransferServiceImpl();
    
    grpc::Status CreateFile(grpc::ServerContext* context, 
                           grpc::ServerReader<filetransfer::FileChunk>* reader,
                           filetransfer::UploadStatus* response) override;
    
    grpc::Status GetFile(grpc::ServerContext* context,
                        const filetransfer::DownloadRequest* request,
                        grpc::ServerWriter<filetransfer::FileChunk>* writer) override;
};

#endif // FILE_TRANSFER_SERVER_H 