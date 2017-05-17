<<<<<<< HEAD
import concurrent.futures
import os
import pandas as pd
import time
import youtube_dl
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
    # Запускаем скачивание в десяти потоках
=======
import concurrent.futures
import os
import pandas as pd
import time
import youtube_dl
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
    # Запускаем скачивание в десяти потоках
>>>>>>> 0adcc561ceae1b166498b7c3663de8f884b147c2
    download_all(10)