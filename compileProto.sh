python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. master.proto
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. reducer.proto
# python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto
# python3 -m grpc_tools.protoc master.proto --proto_path=. --python_out=. --grpc_python_out=.