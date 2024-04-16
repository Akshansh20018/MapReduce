import grpc
from concurrent import futures
import master_pb2
import master_pb2_grpc

def calculate_distance(point, centroid):
    return sum((p - c) ** 2 for p, c in zip(point, centroid))

def find_nearest_centroid(point, centroids):
    distances = [calculate_distance(point, centroid) for centroid in centroids]
    nearest_centroid_index = distances.index(min(distances))
    return nearest_centroid_index

# class MapperClient:
#     def __init__(self, server_address):
#         self.channel = grpc.insecure_channel(server_address)
#         self.stub = master_pb2_grpc.MapReduceServiceStub(self.channel)
    
#     def process_data(self, start_index, end_index, centroids):
#         # For the example, assume data is already loaded in memory or accessible
#         data_points = load_data_points(start_index, end_index)
#         results = []
        
#         for point in data_points:
#             nearest_centroid_index = find_nearest_centroid(point, centroids)
#             # Prepare result as a tuple of centroid index and the data point
#             results.append((nearest_centroid_index, point))
        
#         # Send results back to master or reducer
#         # Here, we assume it's going to the master which aggregates these
#         self.send_results(results)
    
#     def send_results(self, results):
#         # Convert results to the appropriate gRPC message type
#         result_message = master_pb2.TaskResponse(results=str(results))
#         response = self.stub.ProcessTask(result_message)
#         print("Response from server:", response)

class MasterHandler(master_pb2_grpc.MasterServicer):
    def __init__(self):
        super().__init__()
        # self.serve()

    def PartitionInput(self, request, context):
        print("success")
        return master_pb2.MapAndPartitionResponse(status=True)

    def serve(self):
        port = "50051"
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        master_pb2_grpc.add_MasterServicer_to_server(MasterHandler(), server)
        server.add_insecure_port("[::]:50051")
        server.start()
        server.wait_for_termination()

def load_data_points(start_index, end_index):
    # Placeholder function to load data from a file or database
    # You need to implement this part based on your application's data storage
    return [(x, y) for x in range(start_index, end_index) for y in range(2)]  # Example data points

if __name__ == "__main__":
    # mapper = MapperClient('localhost:50051')
    # centroids = [(1.0, 2.0), (3.0, 4.0)]  # Example centroids
    # mapper.process_data(0, 100, centroids)  # Example indices, modify as needed
    temp = MasterHandler()
    temp.serve()