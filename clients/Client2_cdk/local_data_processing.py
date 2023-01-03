import numpy as np
import tensorflow as tf
import os

# load the samples at clients
def input_fn(member_ID, num_train_per_client, num_test_per_client):
    getLocalSamples(member_ID, num_train_per_client, num_test_per_client)
    x_train_client = np.load('local_dataset/x_train_client.npy')
    y_train_client = np.load('local_dataset/y_train_client.npy')
    x_test_client = np.load('local_dataset/x_test_client.npy')
    y_test_client = np.load('local_dataset/y_test_client.npy')
   
    return x_train_client, y_train_client, x_test_client, y_test_client

def getLocalSamples(clientId, num_train_per_client, num_test_per_client):
    
    x_train_file = 'local_dataset/x_train_client.npy'
    y_train_file = 'local_dataset/y_train_client.npy'
    x_test_file = 'local_dataset/x_test_client.npy'
    y_test_file = 'local_dataset/y_test_client.npy'
    
    if (os.path.exists(x_train_file) and 
        os.path.exists(y_train_file) and
        os.path.exists(x_test_file) and
        os.path.exists(y_test_file)):
        return    
    
    mnist = tf.keras.datasets.mnist
    (x_train, y_train), (x_test, y_test) = mnist.load_data()
    x_train_client = x_train[(clientId -1)*num_train_per_client:clientId*num_train_per_client] / 255.0
    y_train_client = y_train[(clientId -1)*num_train_per_client:clientId*num_train_per_client]

    x_test_client = x_test[(clientId -1)*num_test_per_client:clientId*num_test_per_client] / 255.0
    y_test_client = y_test[(clientId -1)*num_test_per_client:clientId*num_test_per_client]
    
    np.save('local_dataset/x_train_client.npy', x_train_client, allow_pickle=True)
    np.save('local_dataset/y_train_client.npy', y_train_client, allow_pickle=True)
    np.save('local_dataset/x_test_client.npy', x_test_client, allow_pickle=True)
    np.save('local_dataset/y_test_client.npy', y_test_client, allow_pickle=True)