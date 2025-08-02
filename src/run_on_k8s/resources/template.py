from source import fn
import pickle
import time
import os

if __name__ == "__main__":
    while not os.path.exists("/app/output/input.pkl"):
        time.sleep(0.1)

    with open("/app/output/input.pkl", "rb") as input_file:
        _input = pickle.load(input_file)

    output = fn(_input)

    # Store output as file for grabs by the client
    with open("/app/output/output.pkl", "wb") as output_file:
        pickle.dump(output, output_file)

    # # wait for output retrieval
    # while True:
    #     pass