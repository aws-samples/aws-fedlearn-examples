#Make a .py class that each client will download 
import tensorflow as tf
    
class MLMODEL:
    def __init__(self):
        self.model = tf.keras.models.Sequential([
            tf.keras.layers.Flatten(input_shape=(28, 28)),
            tf.keras.layers.Dense(128, activation='relu'),
            tf.keras.layers.Dropout(0.2),
            tf.keras.layers.Dense(10)])
        loss_fn = tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True)
        self.model.compile(optimizer='adam',loss=loss_fn,metrics=['accuracy'])

    def getModel(self):
        return self.model