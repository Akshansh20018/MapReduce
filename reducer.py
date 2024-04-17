import grpc
import reducer_pb2
import reducer_pb2_grpc
import master_pb2
import master_pb2_grpc
from concurrent import futures
from reducer_pb2 import Centroid,Cent, ShuffleSortRequest, ReducerResponse
import numpy as np
import sys
from addr import *
import os 
import math
import time

def helper_new_centroid(data_points):
    x_sum = 0
    y_sum = 0
    for i in data_points:
        x_sum+= i[1]
        y_sum+= i[2]    
    if len(data_points) == 0:
        return 
    x_new = x_sum/len(data_points)
    y_new = y_sum/len(data_points)
    return [data_points[0][0], x_new, y_new]
    

class Reducer:
    def __init__(self, id, address):
        self.address = address
        self.id = id 
        self.data_points = []
        self.num_reducers = 0

    def reduce_data(self, grouped_data):
        new_centroids = self.calculate_new_centroids(grouped_data)
        self.send_new_centroids(new_centroids)

    def calculate_new_centroids(self, grouped_data):
        new_centroids = []
        for centroid_index, points in grouped_data.items():
            if points:
                mean_point = np.mean(points, axis=0)
                new_centroids.append((centroid_index, mean_point.tolist()))
        return new_centroids

    def receive_data(self, mapper_addresses, num_mappers, centroidID):
        for j in range(num_mappers):
            try:
                channel = grpc.insecure_channel(f"[::]:{mapper_addresses[j]}")
                stub = master_pb2_grpc.MasterStub(channel)
                request = master_pb2.GetPointByCentroidRequest(centroidID = centroidID)
                response = stub.GetPoints(request)
                points = response.points
                print(f"Reducer received point from mapper: {mapper_addresses[j]}")

                for i in range(len(points)):
                    self.data_points.append([points[i].index,points[i].x, points[i].y])
                # print(f"Len of data point: {len(self.data_points)}")
            except:
                pointsList = []
                with open(f"./Mapper/M{self.id}/Partition_{centroidID%self.num_reducers}", 'r') as file:
                    for line in file:
                        cid, x, y, f = line.strip().split(' ')
                        # print(cid)
                        if int(cid)==centroidID:
                            pointsList.append({'index':int(cid), 'x':float(x), 'y':float(y)})
                for i in range(len(pointsList)):
                    self.data_points.append([pointsList[i]['index'],pointsList[i]['x'],pointsList[i]['y']])



class ReducerHandler(reducer_pb2_grpc.ReducerServicer, Reducer):
    def __init__(self, id:int, address: Address):
        super().__init__(id, address)

    def ShuffleAndSort(self, request, context):
        print("Called Shuffle and Sort from Master to Reducer")
        time.sleep(3)
        try:
            self.data_points = []
            num_mappers = request.numMappers
            centroidID = request.centroidID
            mapper_addresses = request.mapAddresses
            self.num_reducers = request.numReducers
            # print(mapper_addresses)
            self.receive_data(mapper_addresses, num_mappers, centroidID)
            
            upd_centroid = helper_new_centroid(self.data_points)
            # print(centroidID)
            updated_centroid = Cent(x=upd_centroid[1], y=upd_centroid[2])
            # print(f"updated centroid: {upd_centroid[0]} - {updated_centroid}")
            with open(f"./Reducer/R{self.id}", 'a') as file:
                file.write(f"{upd_centroid[0]} {upd_centroid[1]} {upd_centroid[2]} \n")

            return reducer_pb2.ReducerResponse(centroidID = upd_centroid[0],updatedCentroids = updated_centroid,status =True)

        except Exception as e:
            print(f"An error occurred: {e}")            


def run(handler: ReducerHandler):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    reducer_pb2_grpc.add_ReducerServicer_to_server(
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
    with open('reducer.conf') as conf:
        while s := conf.readline():
            n_id, *n_address = s.split()
            if int(n_id) == id:
                address = Address(int(n_id), n_address[0], int(n_address[1]))
                NODE_ADDR = address
                ip = n_address[0]
                port = int(n_address[1])
    run(ReducerHandler(id,address))