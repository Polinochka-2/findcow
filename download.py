import concurrent.futures
import os
import pandas as pd
import time
import youtube_dl
import subprocess
import datetime
import multiprocessing


# пути до файлов с данными
TRAINING_CSV_PATH = os.path.abspath('./data/training/data.csv')
VALIDATION_CSV_PATH = os.path.abspath('./data/validation/data.csv')
# пути до папок, в которых будут храниться порезанных картинки
TRAINING_IMG_PATH = os.path.abspath('./data/training/img')
VALIDATION_IMG_PATH = os.path.abspath('./data/validation/img')
# пути до папок, в которых будут храниться скачанные видео
TRAINING_VIDEO_PATH = os.path.abspath('./data/training/video')
VALIDATION_VIDEO_PATH = os.path.abspath('./data/validation/video')
# путь до файлика с настройками
TRAINING_SETTINGS_PATH = os.path.abspath('./training_settings.txt')
VALIDATION_SETTINGS_PATH = os.path.abspath('./validation_settings.txt')


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
                print(video_file_path)
                try:
                    print('ffmpeg -i {} -ss {} -vframes 1 {}'.format(video_file_path, row['timestamp'] // 1000,
                                                                     full_img_path))

                    subprocess.check_call('ffmpeg -i {} -ss {} -vframes 1 {}'.format(video_file_path,
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
    else: return None


def download_all(num_threads=4):
    threads = []
    for d_path in [TRAINING_CSV_PATH, VALIDATION_CSV_PATH]:
        settings_path = VALIDATION_SETTINGS_PATH if d_path == VALIDATION_CSV_PATH else TRAINING_SETTINGS_PATH
        settings = set()
        with open(settings_path, 'r') as settings_file:
            settings = set(settings_file.read().split('\n'))
        video_path = VALIDATION_VIDEO_PATH if d_path == VALIDATION_CSV_PATH else TRAINING_VIDEO_PATH
        img_path = VALIDATION_IMG_PATH if d_path == VALIDATION_CSV_PATH else TRAINING_IMG_PATH
        if not os.path.exists(video_path):
            os.makedirs(video_path)
        df = pd.read_csv(d_path, index_col=False, header=None)
        df.columns = ['youtube_id', 'timestamp', 'class_id', 'class_name', 'object_id', 'object_presence',
                      'xmin', 'xmax', 'ymin', 'ymax']
        df = df[df['class_name'] == 'cow']
        df = df[df['object_presence'] == 'present']
        df = df.sort_values(['youtube_id','timestamp'])
        df = df[df['youtube_id'].isin(set(df['youtube_id'])-settings)]
        total_count = len(set(df['youtube_id']))
        print('[{}] Начинаем скачивание...'.format(datetime.datetime.now()))
        start_time = time.time()
        thread = multiprocessing.Process(target=clip_video, args=(video_path, settings_path, df))
        thread.start()
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            all_ids = list(set(df['youtube_id']))
            fs = [executor.submit(download_video, youtube_id, video_path, img_path, df[df['youtube_id']==youtube_id])
                  for youtube_id in set(all_ids)]
            for i, f in enumerate(concurrent.futures.as_completed(fs)):
                print("[{}] Загружено: {}/{}".format(datetime.datetime.now(), i+1, total_count))
                total_time = time.time() - start_time
                finish_time = int(total_time/(i+1)*(total_count-i)) + start_time
                print("Ожидаемое время окончания: {}".format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(finish_time))))
        thread.join()

if __name__ == '__main__':
    download_all(10)