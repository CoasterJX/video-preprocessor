import os

HDFS_ROOT_DIRECTORY = 'hdfs://hobot-bigdata/user/jianxi.wang/data/testonly/0629'
HDFS_OUTPUT_DIRECTORY = 'hdfs://hobot-bigdata/user/jianxi.wang/data/testonly/batch_testonly'
HDFS_TEMP_STORAGE = 'hdfs://hobot-bigdata/user/jianxi.wang/FYZ_SYNC/batch02_29'
LOCAL_ROOT_DIRECTORY = os.path.join('checkonly', HDFS_ROOT_DIRECTORY[HDFS_ROOT_DIRECTORY.rfind('/'):].lstrip('/'))

PATH_TO_IMAGE = os.path.join(LOCAL_ROOT_DIRECTORY, 'image')
PATH_TO_STACKED_IMAGE = os.path.join(LOCAL_ROOT_DIRECTORY, 'stacked_image')
PATH_TO_STACKED_VIDEO = os.path.join(LOCAL_ROOT_DIRECTORY, 'stacked_video')
PATH_TO_ERRLOG = os.path.join(LOCAL_ROOT_DIRECTORY, 'errlog')
PATH_TO_SHELL = os.path.join(LOCAL_ROOT_DIRECTORY, 'shell')
PATH_TO_VIDEOSYNC = 'video_sync_temp'
BLANK_IMG_DIRECTORY = 'checkonly/blank.jpg'
OUTPUT_DIRECTORY_VIDEO = 'video'

VIDEO_FORMAT = 'mp4'
IMG_FORMAT = 'jpg'
ROTATE_ANGLE = 270
IMG_SCALE = 0.6
SIZE_OF_IMG = (1080, 1920)
ZOOM_RATE = 1/3
LOCATION_DECODER_CHINESE = {
    '左上': (0, 0),
    '中上': (0, 1),
    '右上': (0, 2),
    '左中': (1, 0),
    '中': (1, 1),
    '右中': (1, 2),
    '左下': (2, 0),
    '中下': (2, 1),
    '右下': (2, 2)
}
CAMERA_DECODER_CHINESE = {
    '左上': '32',
    '中上': '12',
    '右上': '22',
    '左中': '31',
    '中': '11',
    '右中': '21',
    '左下': '33',
    '中下': '13',
    '右下': '23'
}
CAMERA_TO_COORDINATE = {
    '32': (0, 0),
    '12': (0, 1),
    '22': (0, 2),
    '31': (1, 0),
    '11': (1, 1),
    '21': (1, 2),
    '33': (2, 0),
    '13': (2, 1),
    '23': (2, 2)
}

ERRLOG_NAME = os.path.join(LOCAL_ROOT_DIRECTORY, 'errlog/errlog_fixname.txt')
ERRLOG_SYNC = os.path.join(LOCAL_ROOT_DIRECTORY, 'errlog/errlog_notsync.txt')
ERROR_TYPE = {
    '尺寸错误'
}

MAP_REDUCE_FILE = os.path.join(LOCAL_ROOT_DIRECTORY, 'shell/send_to_map_reduce.sh')
EXTRACT_IMAGE_SHELL = os.path.join(LOCAL_ROOT_DIRECTORY, 'shell/extract_image.sh')
FIX_NAME_SHELL = os.path.join(LOCAL_ROOT_DIRECTORY, 'shell/fixname.sh')
UPDATE_NAME_SHELL = os.path.join(LOCAL_ROOT_DIRECTORY, 'shell/update.sh')
FIX_SYNC_SHELL = os.path.join(LOCAL_ROOT_DIRECTORY, 'shell/fixsync.sh')
MULTITHREAD_VALUE = 40

ADVANCED_SYNC = False
