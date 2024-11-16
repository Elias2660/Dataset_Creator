import os
import numpy as np
import pandas as pd
import cv2

def get_video_info(file_list, path):
    # get one video and return the fps 
    video = cv2.VideoCapture(os.path.join(path, file_list[0]))
    fps = video.get(cv2.CAP_PROP_FPS)
    video.release()
    return fps
    
    