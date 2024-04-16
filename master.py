import grpc
from concurrent import futures
import argparse
import threading
import master_pb2
import master_pb2_grpc
from master_pb2 import Point
import random

class Address:
    def __init__(self, id: int, ip: str, port: int):
        self.id = id
        self.ip = ip
        self.port = port

class Master(master_pb2_grpc.MasterServicer):
    def __init__(self, num_mappers, num_reducers, num_centroids, num_iterations, file_path, mapper_addresses, reducer_addresses):
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.num_centroids = num_centroids
        self.num_iterations = num_iterations
        self.file_path = file_path
        self.mapper_addr = mapper_addresses
        self.reducer_addr = reducer_addresses
        self.old_centroids = []
        self.new_centroids = []
        for i in range(self.num_centroids):
            idx = i
            x = random.uniform(-50, 50)
            y = random.uniform(-50,-50)
            point = Point(index=idx, x=x, y=y)
            self.new_centroids.append(point)
        print(self.mapper_addr[0].port)
        self.split_data()

    def ProcessTask(self, request, context):
        # Simulate task processing
        return master_pb2.TaskResponse(results=["Processed data from indices: {} to {}".format(request.start_index, request.end_index)])

    def split_data(self):
        points = []
        with open(self.file_path, 'r') as file:
            for line in file:
                temp = []
                # print(line)
                x, y = line.strip().split(', ')
                # print(x)
                temp.append(float(x))
                temp.append(float(y))
                points.append(temp)
        
        data_len = len(points)

        split_data = [[] for _ in range(self.num_mappers)]
        
        for i in range(data_len):
            ind = i%self.num_mappers
            split_data[ind].append(i)
        print(split_data)

        request = []
        # for i in range(len(self.mapper_addr)):
        for i in range(self.num_mappers):
            print("1")
            thread = threading.Thread(target=self.request_partition, args=(self.mapper_addr[i],split_data[i]))    
            request.append(thread)
            thread.start()
        
        for i in request:
            i.join()
    
    def request_partition(self, i:Address, split_data):
        print("2")
        channel = grpc.insecure_channel(f"{i.ip}:{i.port}")
        stub = master_pb2_grpc.MasterStub(channel)
        print("3")
        try:
            request = master_pb2.PartitionRequest(indexes = split_data, centroids = self.new_centroids)
            response = stub.PartitionInput(request)
            if response.status == True:
                print("hogya")
            else:
                print("nhk")
        except Exception as e:
            print(e)



def serve(master):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    master_pb2_grpc.add_MasterServicer_to_server(master, server)
    server.add_insecure_port('[::]:50050')
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    mappers = int(input("Number of mappers: "))
    reducers = int(input("Number of reducers: "))
    centroids = int(input("Number of centroids: "))
    iterations = int(input("Number of iterations: "))
    file_path = "./points.txt"
    mapper_addresses = []
    reducer_addresses = []

    with open('mapper.conf') as conf:
        while  s:= conf.readline():
            n_id, *n_address = s.split()
            n_ip = n_address[0]
            n_port = int(n_address[1])
            mapper_addresses.append(Address(int(n_id), n_ip, n_port))
    
    with open('reducer.conf') as conf:
        while  s:= conf.readline():
            n_id, *n_address = s.split()
            n_ip = n_address[0]
            n_port = int(n_address[1])
            reducer_addresses.append(Address(int(n_id), n_ip, n_port))

    master = Master(mappers, reducers,centroids,iterations,file_path, mapper_addresses, reducer_addresses)
    print("Starting the master server...")
    serve(master)