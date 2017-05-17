import os
import pandas as pd
import time
import youtube_dl
import subprocess
import datetime

def clip_video(video_path, settings_path, df):
    img_dir_path = TRAINING_IMG_PATH if video_path == TRAINING_VIDEO_PATH else VALIDATION_IMG_PATH
    NOT_FINISHED = 0
    start_time = time.time()
    count = 0
    while NOT_FINISHED < 2:
        print('новый цикл')
        files = os.listdir(video_path)
        completed = list(filter(lambda s: s.endswith('.mp4'), files))
        if not completed:
            time.sleep(180)
            NOT_FINISHED += 1
            continue
        NOT_FINISHED = 0
        for file in completed:
            print('другой новый цикл')
            # https://en.wikibooks.org/wiki/FFMPEG_An_Intermediate_Guide/image_sequence
            # ffmpeg -i video.webm -ss 00:00:10 -vframes 1 thumbnail.png
            video_file_path = os.path.join(video_path, file)
            ERROR_FLAG = False
            for _, row in df[df['youtube_id'] == file[:-4]].iterrows():
                full_img_path = os.path.join(img_dir_path,
                                             row['youtube_id'] + '_' +str(row['timestamp'])+'.png')
                try:
                    subprocess.check_call('ffmpeg -i {} -ss {} -r 0.3 -vframes 1 {}'.format(video_file_path,
                             row['timestamp'] // 1000, full_img_path).split(' '),
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                except Exception as e:
                    print(e)
                    ERROR_FLAG = True

            if not ERROR_FLAG:
                os.remove(video_file_path)
                count += 1
                print("Осталось порезать видео: {}".format(len(completed) - count))
                finish_time = int((len(completed) - 1) * (time.time() - start_time) / count) + start_time
                finish_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(finish_time))
                print("Ожидаемое время окончания {}".format(finish_date))
                with open(settings_path, 'a') as f:
                    f.write(file[:-4]+'\n')

def download_video(youtube_id, path, img_path, df):
    video_path = os.path.join(os.path.abspath(path), youtube_id+'.mp4')
    img_path = os.path.abspath(img_path)

    if not os.path.exists(video_path):
        ydl_opts = {'quiet': True, 'ignoreerrors': True, 'no_warnings': True,
                    'format': 'best[ext=mp4]',
                    'outtmpl': video_path}
        with youtube_dl.YoutubeDL(ydl_opts) as ydl:
            ydl.download(['youtu.be/'+youtube_id])

    if os.path.exists(video_path):
        return youtube_id
    else:
        return None