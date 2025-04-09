import os

import cv2


def get_video_info(file_list, path):
    # get fps from all videos and compute the most common fps
    fps_list = []
    for video_file in file_list:
        video_path = os.path.join(path, video_file)
        video = cv2.VideoCapture(video_path)
        fps_value = video.get(cv2.CAP_PROP_FPS)
        fps_list.append(fps_value)
        video.release()
    most_common_fps = max(set(fps_list), key=fps_list.count)

    return int(most_common_fps)


if __name__ == "__main__":
    file_list = [file for file in os.listdir() if file.endswith(".mp4")]
    print(get_video_info(file_list, "."))
