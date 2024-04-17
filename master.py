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
        # for i in range(self.num_centroids):
        #     x = random.uniform(30,40)
        #     y = random.uniform(30,40)
        #     self.new_centroids.append(Point(index = i, x = x, y = y))
        with open(self.file_path, 'r') as file:
            count = 0
            for line in file:
                if count == self.num_centroids:
                    break
                idx = count
                x, y = line.strip().split(', ')
                point = Point(index=idx, x=float(x), y=float(y))
                self.new_centroids.append(point)
                count+= 1
        print(self.mapper_addr[0].port)
        for i in range(self.num_mappers):
            self.mapper_ports.append(int(self.mapper_addr[i].port))

        for i in range(self.num_iterations):
            # print(i)
            self.split_data()
            self.iter+=1
            if(self.converged):
                return
            print("uasuobao: ", self.new_centroids)
           
        

    def ProcessTask(self, request, context):
        # Simulate task processing
        return master_pb2.TaskResponse(results=["Processed data from indices: {} to {}".format(request.start_index, request.end_index)])

    def split_data(self):
        points = []
        with open(self.file_path, 'r') as file:
            for line in file:
                # print(line)
                x, y = line.strip().split(', ')
                points.append([float(x), float(y)])
        
        data_len = len(points)

        split_data = [[] for _ in range(self.num_mappers)]
        
        for i in range(data_len):
            ind = i%self.num_mappers
            split_data[ind].append(i)
        # print(split_data)

        request = []
        # for i in range(len(self.mapper_addr)):
        # m = multiprocessing.Pool(self.num_mappers)
        # r = multiprocessing.Pool(self.num_centroids)
        for i in range(self.num_mappers):
            # print("1")
            thread = threading.Thread(target=self.request_mapper_partition, args=(self.mapper_addr[i], split_data[i]))    
            request.append(thread)
            thread.start()
        #     m.apply_async(self.request_mapper_partition, args = ((self.mapper_addr[i], split_data[i])))
        # # time.sleep(2)
        # m.close()
        # m.join()
        for i in request:
            i.join()
        if(self.map_rerun):
            self.map_rerun = False
            self.split_data()
            return
        if self.old_centroids == self.new_centroids:
            print(f"Converged at iteration {self.iter}")
            self.converged = True
            return
        self.old_centroids = copy.deepcopy(self.new_centroids)
        # print("BOOOOOOOO")
        request = []
        for i in range(self.num_centroids):
            if(i>0 and (i%self.num_reducers==0)):
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
        #     r.apply_async(self.request_reducer, args=(self.reducer_addr[i%self.num_reducers],i))
        # r.close()
        # r.join()
        for i in request:
            i.join()
        if(self.reducer_rerun):
            self.reducer_rerun = False
            self.split_data()
            return
        
        # print("BO2")
    
    def request_mapper_partition(self, i:Address, split_data):
        # print("2")
        channel = grpc.insecure_channel(f"{i.ip}:{i.port}")
        stub = master_pb2_grpc.MasterStub(channel)
        # print("3")
        try:
            request = master_pb2.PartitionRequest(indexes = split_data, centroids = self.new_centroids, numReducers = self.num_reducers)
            response = stub.PartitionInput(request)
            if response.status == True:
                print("hogya")
            else:
                print("Tring again")
                time.sleep(1)
        except Exception as e:
            time.sleep(0.2)
            print("Before")
            for hola in self.mapper_addr:
                print(f"port: {hola.port}", end= " ")
            self.mapper_addr.remove(i)
            print("After")
            for hola in self.mapper_addr:
                print(f"port: {hola.port}", end= " ")
            self.num_mappers-=1
            self.map_rerun = True
            print("error in mapper trying again")
            # self.request_mapper_partition(i,split_data)

    def request_reducer(self, i:Address, centroidID):
        channel = grpc.insecure_channel(f"{i.ip}:{i.port}")
        stub = reducer_pb2_grpc.ReducerStub(channel)
        try:
            print(self.mapper_ports)
            request = reducer_pb2.ShuffleSortRequest(centroidID = centroidID, numMappers = self.num_mappers, mapAddresses = self.mapper_ports,numReducers = self.num_reducers)
            print("hola")
            response = stub.ShuffleAndSort(request)
            if(response.status):
                print(" amigo")
                print(response)
                id = response.centroidID
                x, y = response.updatedCentroids.x, response.updatedCentroids.y
                self.new_centroids[id] = Point(index = id,x = x,y = y)

                # print("Here here", id, x, y)
                # self.new_centroids[response.index] = [response.x, response.y]
                # print(f"Testing Response {response.index} {response.x} {response.y}")
                print(self.new_centroids)
            else:
                print("Status response false")
                time.sleep(1)

        except Exception as e:
            time.sleep(0.5)
            self.reducer_addr.remove(i)
            self.num_reducers -=1
            self.reducer_rerun = True
            print("error in contacting reducer")
            # self.request_reducer(i,centroidID)



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
