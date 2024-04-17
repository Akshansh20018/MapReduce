import grpc
from concurrent import futures
import argparse
import threading
import master_pb2
import master_pb2_grpc
import reducer_pb2
import reducer_pb2_grpc
from master_pb2 import Point
import random
import time
import copy
import multiprocessing

class Address:
    def __init__(self, id: int, ip: str, port: int):
        self.id = id
        self.ip = ip
        self.port = port # Akshansh is sad

class Master(master_pb2_grpc.MasterServicer):
    def __init__(self, num_mappers, num_reducers, num_centroids, num_iterations, file_path, mapper_addresses, reducer_addresses):
        self.converged = False
        self.iter = 1
        self.map_rerun = False
        self.reducer_rerun = False
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.num_centroids = num_centroids
        self.num_iterations = num_iterations
        self.file_path = file_path
        self.mapper_addr = mapper_addresses
        self.reducer_addr = reducer_addresses
        self.old_centroids = []
        self.new_centroids = []
        self.mapper_ports = []

        self.line_count = 0
        with open(self.file_path, 'r') as file:
            for line in file:
                self.line_count+= 1

        self.rand_selection = random.sample(range(self.line_count), self.num_centroids)
        # print(f"random selection: {self.rand_selection}")

        with open(self.file_path, 'r') as file:
            count = 0
            temp_line_count = 0
            for line in file:
                if count == self.num_centroids:
                    break
                if temp_line_count in self.rand_selection:
                    idx = count
                    x, y = line.strip().split(', ')
                    point = Point(index=idx, x=float(x), y=float(y))
                    self.new_centroids.append(point)
                    count+= 1
                temp_line_count+= 1

        print(f"Initialized centroids: {self.new_centroids}")
        for i in range(self.num_mappers):
            self.mapper_ports.append(int(self.mapper_addr[i].port))

        for i in range(self.num_iterations):
            self.split_data()
            self.iter+=1
            with open(f"./centroids.txt", 'w') as file:
                for i in range (len(self.new_centroids)):
                    file.write(f"{i} {self.new_centroids[i].x} {self.new_centroids[i].y} \n")
            if(self.converged):
                return
            # print("new centroids: ", self.new_centroids)
           
        

    def ProcessTask(self, request, context):
        # Simulate task processing
        return master_pb2.TaskResponse(results=["Processed data from indices: {} to {}".format(request.start_index, request.end_index)])

    def split_data(self):
        points = []
        with open(self.file_path, 'r') as file:
            for line in file:
                x, y = line.strip().split(', ')
                points.append([float(x), float(y)])
        
        data_len = len(points)

        split_data = [[] for _ in range(self.num_mappers)]
        
        for i in range(data_len):
            ind = i%self.num_mappers
            split_data[ind].append(i)

        request = []
        for i in range(self.num_mappers):
            thread = threading.Thread(target=self.request_mapper_partition, args=(self.mapper_addr[i], split_data[i]))    
            request.append(thread)
            thread.start()
        for i in request:
            i.join()
        if(self.map_rerun):
            self.map_rerun = False
            self.split_data()
            return
        request = []
        for i in range(self.num_centroids):
            # print(f"self.num_reducers {self.num_reducers}")
            if(i>0 and (i%self.num_reducers==0)):
                # print(i)
                for j in request:
                    j.join()
                if(self.reducer_rerun):
                    self.reducer_rerun = False
                    self.split_data()
                    return
                request = []
            thread = threading.Thread(target=self.request_reducer, args=(self.reducer_addr[i%self.num_reducers],i))    
            request.append(thread)
            thread.start()

        for i in request:
            i.join()
        if(self.reducer_rerun):
            self.reducer_rerun = False
            self.split_data()
            return

        print(f"Iteration: {self.iter}")
        print(f"Centroids: {self.new_centroids}")

        if self.old_centroids == self.new_centroids:
            print(f"Converged at iteration {self.iter}")
            self.converged = True
            return
        self.old_centroids = copy.deepcopy(self.new_centroids)
    
    def request_mapper_partition(self, i:Address, split_data):
        channel = grpc.insecure_channel(f"{i.ip}:{i.port}")
        stub = master_pb2_grpc.MasterStub(channel)
        try:
            request = master_pb2.PartitionRequest(indexes = split_data, centroids = self.new_centroids, numReducers = self.num_reducers)
            response = stub.PartitionInput(request)
            print(f"Response received from mapper: {i.port} partition")
        except Exception as e:
            # time.sleep(0.2)
            self.mapper_addr.remove(i)
            self.mapper_ports.remove(int(i.port))
            self.num_mappers-=1
            self.map_rerun = True
            print("Mapper partition response failed")

    def request_reducer(self, i:Address, centroidID):
        channel = grpc.insecure_channel(f"{i.ip}:{i.port}")
        stub = reducer_pb2_grpc.ReducerStub(channel)
        try:
            request = reducer_pb2.ShuffleSortRequest(centroidID = centroidID, numMappers = self.num_mappers, mapAddresses = self.mapper_ports,numReducers = self.num_reducers)
            response = stub.ShuffleAndSort(request)
            if(response.status):
                print(f"Response received from reducer: {i.port}")
                id = response.centroidID
                x, y = response.updatedCentroids.x, response.updatedCentroids.y
                self.new_centroids[id] = Point(index = id,x = x,y = y)

                # print(self.new_centroids)
            else:
                print("Reducer shuffle sort response failed")

        except Exception as e:
            # time.sleep(0.5)
            self.reducer_addr.remove(i)
            self.num_reducers -=1
            self.reducer_rerun = True
            print("Reducer shuffle sort response failed")


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
