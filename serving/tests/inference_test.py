from sys import path
from os.path import dirname as dir

path.append(dir(path[0]))
__package__ = "model"

from model import inference
from datetime import datetime


INFERENCE_TYPE = 'cmle'  # local' | 'cmle'

instances = [
  {
    "V23": -0.471709469790029,
    "V22": -0.581270019133044,
    "V21": 3.7536560833085604,
    "V20": -1.7910765472128303,
    "V27": 0.699600694529817,
    "V26": 0.795361353517608,
    "V25": -0.459130000870193,
    "V24": 0.0153454352970634,
    "V28": 0.339586330147973,
    "V1": 0.10741590340147,
    "V2": 0.8113232283170401,
    "V3": -2.7792543222248702,
    "V4": -1.17726972753825,
    "V5": -0.46540477371290995,
    "V6": -0.496894626457315,
    "V7": 0.380197905618243,
    "V8": -4.88379215131891,
    "V9": -1.10742940467555,
    "Amount": 274.8,
    "key": 8821738702745408944,
    "Time": 126481.0,
    "V18": 0.49129158267520895,
    "V19": -1.3922975162365498,
    "V12": 0.279855357730141,
    "V13": 0.377550214586591,
    "V10": 0.924402052309218,
    "V11": -1.16198047670576,
    "V16": -2.09567581478598,
    "V17": 0.21285085432553197,
    "V14": 0.794771759648585,
    "V15": -1.01132120214707
  }, # class = 0
  {"key":6370170575253253120,
    "V23":-0.0961301444,
    "V22":0.9786601263,
    "V21":2.3098801689,
    "V20":1.3540647952,
    "V27":1.693607508,
    "V26":0.6508927864,
    "V25":-0.43562793,
    "V24":0.4323767225,
    "V28":0.8576853717,
    "V1":-3.6328089493,
    "V2":5.4372633623,
    "V3":-9.1365214806,
    "V4":10.3072263079,
    "V5":-5.4218302945,
    "V6":-2.8648151493,
    "V7":-10.6340876212,
    "V8":3.01812658,
    "V9":-4.8916403211,
    "Amount":8.54,
    "Time":93824.0,
    "V18":-6.8888910928,
    "V19":2.5860932167,
    "V12":-18.5536970096,
    "V13":-0.3395334077,
    "V10":-11.2350479111,
    "V11":8.7887836671,
    "V16":-12.4279613631,
    "V17":-20.1590474539,
    "V14":-15.6231873303,
    "V15":-0.1889785742
  } # class = 1
]

print("")
print("Inference Type:{}".format(INFERENCE_TYPE))
print("")

time_start = datetime.utcnow()
print("Inference started at {}".format(time_start.strftime("%H:%M:%S")))
print(".......................................")

if INFERENCE_TYPE == 'local':
    output = inference.estimate_local(instances)
else:
    output = inference.estimate_cmle(instances)
print(output)

time_end = datetime.utcnow()
print(".......................................")
print("Inference finished at {}".format(time_end.strftime("%H:%M:%S")))
print("")
time_elapsed = time_end - time_start
print("Inference elapsed time: {} seconds".format(time_elapsed.total_seconds()))
