import grpc
import mapreduce_pb2
import mapreduce_pb2_grpc
import numpy as np

class ReducerClient:
    def __init__(self, server_address):
        self.channel = grpc.insecure_channel(server_address)
        self.stub = mapreduce_pb2_grpc.MapReduceServiceStub(self.channel)
    
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

    def send_new_centroids(self, centroids):
        centroids_message = mapreduce_pb2.TaskResponse(results=str(centroids))
        response = self.stub.ProcessTask(centroids_message)
        print("Response from server:", response)

if __name__ == "__main__":
    reducer = ReducerClient('localhost:50051')
    # Example of grouped data, where the key is the centroid index and value is list of points
    grouped_data = {
        0: [[1.0, 2.0], [1.1, 2.1]],
        1: [[3.0, 4.0], [3.1, 4.1]]
    }
    reducer.reduce_data(grouped_data)
