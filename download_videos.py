import argparse
import os
import json
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

'''
script : 
python /data/yt-video-download/download_videos.py \
--source_path /mnt/shared-storage/tenant/hypertext/kanelin/data/Moment-10M/json/Moment-10M_1.json \
--video_path /mnt/shared-storage/tenant/hypertext/kanelin/data/Moment-10M/video \
--max_workers 128

# source_path args: Moment-10M_0.json, Moment-10M_1.json

'''

parser = argparse.ArgumentParser()
parser.add_argument('--source_path', required=True, type=str)
parser.add_argument('--video_path', required=True, type=str)
parser.add_argument('--max_workers', type=int, default=16, help='Maximum number of worker threads')

args = parser.parse_args()

source_path = args.source_path
video_path = args.video_path
max_workers = args.max_workers 

print('Loading data.')

with open(source_path, 'r') as f:
    packed_data = json.load(f)

print('Start downloading.')

video_names = list(packed_data.keys())
youtube_video_format = 'https://www.youtube.com/watch?v={}'
video_path_format = os.path.join(video_path, '{}.mp4')

def download_video(video_name, pbar):
    try:
        url = youtube_video_format.format(video_name)
        file_path = video_path_format.format(video_name)
        if os.path.exists(file_path):
            pbar.update(1)
            return
        os.system('yt-dlp -o ' + file_path + ' -f 134 ' + url + ' > /dev/null 2>&1')
        pbar.write(f'Downloading of Video {video_name} has finished.')
    except:
        pbar.write(f'Downloading of Video {video_name} has failed.')
    pbar.update(1)

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    with tqdm(total=len(video_names), desc='Downloading videos') as pbar:
        futures = []
        for video_name in video_names:
            future = executor.submit(download_video, video_name, pbar)
            futures.append(future)
        for future in futures:
            future.result()

print('Finished.')