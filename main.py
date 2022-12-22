import os
import sys
import logging

from utils.image import errlog_to_dict_for_jpeg
from utils.image import rename_jpeg_files_based_on_errlog_dict
from utils.image import extract_frames
from utils.image import person_checker

from utils.script import fix_video_file_name
from utils.script import update_video_file_name
from utils.script import fix_sync_error
from utils.script import extract_img_from_videos
from utils.script import video_alignment

from utils.settings import PATH_TO_IMAGE
from utils.settings import ERRLOG_NAME
from utils.settings import IMG_FORMAT
from utils.settings import HDFS_ROOT_DIRECTORY
from utils.settings import FIX_NAME_SHELL
from utils.settings import UPDATE_NAME_SHELL
from utils.settings import EXTRACT_IMAGE_SHELL
from utils.settings import PATH_TO_VIDEOSYNC
from utils.settings import ERRLOG_SYNC
from utils.settings import PATH_TO_STACKED_IMAGE
from utils.settings import HDFS_OUTPUT_DIRECTORY
from utils.settings import MAP_REDUCE_FILE
from utils.settings import OUTPUT_DIRECTORY_VIDEO
from utils.settings import ADVANCED_SYNC
from utils.settings import ROTATE_ANGLE
from utils.settings import ZOOM_RATE
from utils.settings import VIDEO_FORMAT
from utils.settings import BLANK_IMG_DIRECTORY
from utils.settings import SIZE_OF_IMG

from utils.utils import map_ignore

from utils.video import extract_video_portions
from utils.video import sync_videos
from utils.video import merge_videos


def help():
    print('Usage:\n\
        python3 main.py [mode]\n\
\n\n\
[mode] options:\n\
\n\
        --help: display this help information\n\
\n\
        {root/path/to/videos}: align all videos in a given root path\n\
\n\
        fix_name_error: generate a shell that can be executed to fix all video naming errrors on hdfs\n\
\n\
        update_filename: generate a shell that can be executed to update all video namings in FXXXX_camXX.MOV Format\n\
\n\
        fix_sync_error: generate a shell that can be executed to delete videos with undefined errors on hdfs;\n\
            and at the same time, generate a tar to store all merged videos with defined errors\n\
            errors are defined in utils.settings.ERROR_TYPE\n\
\n\
        extract_img_from_videos: generate a shell that can be submitted to Map_Reduce for image extraction\n\
            of all videos on hdfs\n\
\n\
        align [id_1 id_2 ...]: generate a shell that can be submitted to Map_Reduce for video alignment of all videos on hdfs\n\
            [id_1 id_2 ...] is optional, those ids will be ignored during alignment process\n\
\n\
        extract_frames {root/path/to/videos}: extract 1 image from videos in the given root directory\n\
\n\
        merge {root/path/to/images}: merge images in the given root directory to 9-grid images based on naming of images\n')


def fix_name_error():
    copied_image_path = PATH_TO_IMAGE + '__COPY__'
    if os.path.exists(copied_image_path):
        os.system('rm -rf {}'.format(PATH_TO_IMAGE))
        os.system('cp -R {} {}'.format(copied_image_path, PATH_TO_IMAGE))
    else:
        os.system('cp -R {} {}'.format(PATH_TO_IMAGE, copied_image_path))
    errdict = errlog_to_dict_for_jpeg(ERRLOG_NAME, PATH_TO_IMAGE, format=IMG_FORMAT)
    rename_jpeg_files_based_on_errlog_dict(errdict)
    fix_video_file_name(ERRLOG_NAME, HDFS_ROOT_DIRECTORY, FIX_NAME_SHELL)


def main():
    if len(sys.argv) == 2:
        if sys.argv[1] == '--help':
            help()
            exit()

        elif sys.argv[1] == 'fix_name_error':
            fix_name_error()

        elif sys.argv[1] == 'update_filename':
            copied_image_path = PATH_TO_IMAGE + '__COPY__'
            if os.path.exists(copied_image_path):
                os.system('rm -rf {}'.format(copied_image_path))
                update_video_file_name(PATH_TO_IMAGE, HDFS_ROOT_DIRECTORY, UPDATE_NAME_SHELL)
            else:
                logging.error("You haven't start checking the naming errors or you have already done the file name update. \
Please check the naming errors and finish errlog_fixname.txt before update the names.")

        elif sys.argv[1] == 'fix_sync_error':
            fix_sync_error(ERRLOG_SYNC, record_error=True)

        elif sys.argv[1] == 'extract_img_from_videos':
            extract_img_from_videos(HDFS_ROOT_DIRECTORY, EXTRACT_IMAGE_SHELL, temp_file=PATH_TO_VIDEOSYNC)

        elif sys.argv[1] == 'align':
            video_alignment(PATH_TO_IMAGE, PATH_TO_STACKED_IMAGE, HDFS_ROOT_DIRECTORY, HDFS_OUTPUT_DIRECTORY, MAP_REDUCE_FILE, temp_sync=PATH_TO_VIDEOSYNC, temp_op=OUTPUT_DIRECTORY_VIDEO)

        else:
            temp_file = sys.argv[1]
            extract_video_portions(temp_file)
            file_matrix = sync_videos(temp_file, OUTPUT_DIRECTORY_VIDEO, advanced_sync=ADVANCED_SYNC)
            merge_videos(file_matrix, OUTPUT_DIRECTORY_VIDEO, rotate_angle=ROTATE_ANGLE, zoom=ZOOM_RATE, format=VIDEO_FORMAT)

    elif len(sys.argv) == 3:
        if sys.argv[1] == 'extract_frames':
            temp_file = sys.argv[2]
            extract_frames(temp_file)

        elif sys.argv[1] == 'merge':
            jpg_root_dir = sys.argv[2]
            person_checker(jpg_root_dir, PATH_TO_STACKED_IMAGE, BLANK_IMG_DIRECTORY, SIZE_OF_IMG, img_rotate=ROTATE_ANGLE, format=IMG_FORMAT)

        elif sys.argv[1] == 'align':
            ignore_ids = map_ignore(sys.argv[2:], HDFS_OUTPUT_DIRECTORY)
            video_alignment(PATH_TO_IMAGE, PATH_TO_STACKED_IMAGE, HDFS_ROOT_DIRECTORY, HDFS_OUTPUT_DIRECTORY, MAP_REDUCE_FILE, ignore_set=ignore_ids, temp_sync=PATH_TO_VIDEOSYNC, temp_op=OUTPUT_DIRECTORY_VIDEO)

        else:
            help()
            exit()

    else:
        help()
        exit()


if __name__ == '__main__':
    main()
