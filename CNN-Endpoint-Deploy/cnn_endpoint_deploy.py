import boto3
import re
from sagemaker import get_execution_role
from tensorflow.keras.preprocessing import image
import numpy as np

role = get_execution_role()


from tensorflow.keras.models import model_from_json


get_ipython().system('mkdir keras_model')


get_ipython().system('ls keras_model')


# noting here model is a custom cnn built using keras in prior cells
model.save_weights('model_weights.h5')


# save model architecture
with open('model_architecture.json', 'w') as f:
    f.write(model.to_json())


json_file = open('model_architecture.json', 'r')
loaded_model_json = json_file.read()
json_file.close()
loaded_model = model_from_json(loaded_model_json)


loaded_model.load_weights('xxxxxxx.weights.best.hdf5')
print("Loaded model from disk")


# Export the Keras model to the TensorFlow ProtoBuf format


from tensorflow.python.saved_model import builder
from tensorflow.python.saved_model.signature_def_utils import predict_signature_def
from tensorflow.python.saved_model import tag_constants


# Note: This directory structure will need to be followed - see notes for the next section
model_version = '1'
export_dir = 'export/Servo/' + model_version


# Build the Protocol Buffer SavedModel at 'export_dir'
builder = builder.SavedModelBuilder(export_dir)


# Create prediction signature to be used by TensorFlow Serving Predict API
signature = predict_signature_def(
    inputs={"inputs": loaded_model.input}, outputs={"score": loaded_model.output})


from tensorflow.python.keras import backend as K

with K.get_session() as sess:
    sess.run
    # Save the meta graph and variables
    builder.add_meta_graph_and_variables(
        sess=sess, tags=[tag_constants.SERVING], signature_def_map={"serving_default": signature})
    builder.save()


# ## Convert TensorFlow model to a SageMaker readable format


get_ipython().system('ls export')


get_ipython().system('ls export/Servo')


get_ipython().system('ls export/Servo/1')


get_ipython().system('ls export/Servo/1/variables')


# ### Tar the entire directory and upload to S3


import tarfile
with tarfile.open('model.tar.gz', mode='w:gz') as archive:
    archive.add('export', recursive=True)


import sagemaker

sagemaker_session = sagemaker.Session()
inputs = sagemaker_session.upload_data(path='model.tar.gz', key_prefix='model')


# Deploy the trained model


get_ipython().system('touch train.py')


from sagemaker.tensorflow.model import TensorFlowModel
sagemaker_model = TensorFlowModel(model_data='s3://' + sagemaker_session.default_bucket() + '/model/model.tar.gz',
                                  role=role,
                                  framework_version='1.12',
                                  entry_point='train.py')

predictor = sagemaker_model.deploy(initial_instance_count=1,
                                   instance_type='ml.m4.xlarge')


endpoint_name = 'XXXXXXXXXXXXX'


sagemaker_session = sagemaker.Session()


import sagemaker
from sagemaker.tensorflow.model import TensorFlowModel
predictor = sagemaker.tensorflow.model.TensorFlowPredictor(endpoint_name, sagemaker_session)


data = 'processed/test_images/0/0182152c50de.png'


img = image.load_img(data, target_size=(224, 224))
img = image.img_to_array(img) / 255
img_expand = np.expand_dims(img, axis=0)


predictor.predict(img_expand)
