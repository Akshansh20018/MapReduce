import grpc
from concurrent import futures
import master_pb2
import master_pb2_grpc
import sys
from addr import *
import os 
import math
import time

def helper_dist(point, centroid):
    dist = ((point[0]-centroid[0])**2)+((point[1]-centroid[1])**2)
    return math.sqrt(dist)

class Mapper:
    def __init__(self, id, address: Address):
        self.file_path = "./points.txt"
        self.address = address
        self.ind_lst = []
        self.centroids_lst = []
        self.id = id
        self.red_dict = {}
        self.num_reducers = 0
        try: 
            os.mkdir(f"./Mapper/M{self.id}")
        except:
            # print("error in mkdir")
            pass
    
    def process_data(self):
        data_points = []
        # print("Test 1")
        with open(self.file_path, 'r') as file:
            count = 0
            for line in file:
                if count in self.ind_lst:
                    x, y = line.strip().split(', ')
                    data_points.append([float(x), float(y)])
                count+= 1

        for i in range(len(data_points)):
            dist_min = 1e9
            ind_min = -1
            for j in range(len(self.centroids_lst)):
                temp_dist = helper_dist(data_points[i], self.centroids_lst[j])
                if temp_dist<dist_min:
                    dist_min = temp_dist
                    ind_min = j
            self.red_dict[ind_min].append([data_points[i], 1])
        
        for i in range(len(self.centroids_lst)):
            # print("creating file")
            with open(f"./Mapper/M{self.id}/Partition_{i%self.num_reducers}", 'a') as file:
                for j in self.red_dict[i]:
                    file.write(f"{i} {j[0][0]} {j[0][1]} {j[1]} \n")


class MasterHandler(master_pb2_grpc.MasterServicer, Mapper):
    def __init__(self, id:int, address: Address):
        super().__init__(id, address)

    def PartitionInput(self, request, context):
        print(f"Partition Input Called by Master on Mapper: {self.address.port}")
        time.sleep(2)
        for i in range(len(self.centroids_lst)):
            with open(f"./Mapper/M{self.id}/Partition_{i%self.num_reducers}", 'w') as file:
                pass
        
        indexes = request.indexes
        centroids = request.centroids
        self.num_reducers = request.numReducers

        self.ind_lst = []
        self.centroids_lst = []
        # print(centroids)
        for i in range(len(indexes)):
            self.ind_lst.append(indexes[i])

        for i in range(len(centroids)):
            self.centroids_lst.append([centroids[i].x,centroids[i].y])

        for i in range(len(self.centroids_lst)):
            self.red_dict[i] = []

        self.process_data()

        return master_pb2.MapAndPartitionResponse(status=True)
    
    def GetPoints(self, request, context):
        print(f"Reducer request for getting points on mapper at {self.address.port}")
        centroidID = request.centroidID
        pointsList = []
        with open(f"./Mapper/M{self.id}/Partition_{centroidID%self.num_reducers}", 'r') as file:
            for line in file:
                    cid, x, y, f = line.strip().split(' ')
                    # print(cid)
                    if int(cid)==centroidID:
                        pointsList.append({'index':int(cid), 'x':float(x), 'y':float(y)})
        return master_pb2.GetCentroidResponse(points = pointsList)

def run(handler: MasterHandler):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    master_pb2_grpc.add_MasterServicer_to_server(
        handler, server
    )
    server.add_insecure_port(f'[::]:{handler.address.port}')
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    global NODE_ADDR
    id = int(sys.argv[1])
    ip = "[::]"
    port = 0
    address = None
    with open('mapper.conf') as conf:
        while s := conf.readline():
            n_id, *n_address = s.split()
            if int(n_id) == id:
                address = Address(int(n_id), n_address[0], int(n_address[1]))
                NODE_ADDR = address
                ip = n_address[0]
                port = int(n_address[1])
    run(MasterHandler(id,address))