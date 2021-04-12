#!/opt/anaconda/envs/bd9/bin/python3

import numpy as np
import pandas as pd
import pickle
import sys
import base64
import re

from sklearn.linear_model import LogisticRegression

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', -1)

#read the model, deserialize and unpickle it.

model = pickle.loads(
          base64.b64decode(
            open("lab07.model").read().encode('utf-8')
          )
        )

# print("success")
rows = [] #here we keep input data to Dataframe constructor

# iterate over standard input
for line in sys.stdin:
    line = line.replace('[', '')
    line = line.replace(']', '')
    line = line.replace('\n', '')
    line = re.split('[,]', line)

    line_dict = {}
    for i, value in enumerate(line):
        if i != len(line) - 1:
            name = "features_" + str(i)
            line_dict[name] = value
        else:
            name = "uid"
            line_dict[name] = value

    rows.append(line_dict)



#initialize a dataframe from the list
df = pd.DataFrame(rows)
if (len(df) != 0):
    uid = df['uid']
    df = df.drop('uid', 1)

    #run inference
    pred = model.predict(df)


    df['preds'] = pred
    df['uid'] = uid
    res = df[['uid', 'preds']]

    # Output to stdin, so that rdd.pipe() can return the strings to pipedRdd.
    #print(df.to_list())
    print(res)