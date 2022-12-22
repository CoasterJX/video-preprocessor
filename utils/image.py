import os
import cv2
import logging
import numpy as np

from moviepy.editor import VideoFileClip
from moviepy.editor import clips_array

from .settings import LOCATION_DECODER_CHINESE
from .settings import IMG_FORMAT

from .utils import blankfill


def person_checker(root_dir, out_dir, blank_img_dir, img_size, img_rotate=0, format='jpg'):
    # Read all files in the root directory.
    jpg_files = os.listdir(root_dir)
    jpg_files.sort()
    prev_id = '0000'
    matrix_3x3 = [
        ['', '', ''],
        ['', '', ''],
        ['', '', '']
    ]

    # Extract informations for each file.
    for file in jpg_files:
        id_num = file.split('.')[0].strip(' ')
        if prev_id == '0000':
            prev_id = id_num
        if len(id_num) == 4:
            pass
        else:
            logging.error('{}| File Format Error!'.format(file))
            continue

        vid_format = file.split('.')[1]
        # Ignore the file with wrong format.
        if vid_format != 'MOV':
            logging.error('{} is not in MOV format.'.format(file.rstrip('.jpg')))
            # time.sleep(10)

        # Set the directory matrix.
        location = file.split('.')[2]
        if id_num != prev_id:
            # Wrap up previous id.
            matrix_3x3 = blankfill(matrix_3x3, blank_img_dir)
            op_file = Grid(prev_id, matrix_3x3, format, img_size, img_rotate)
            op_file.convert2jpeg(out_dir)

            # Start new id's information.
            prev_id = id_num
            matrix_3x3 = [
                ['', '', ''],
                ['', '', ''],
                ['', '', '']
            ]

        # Fill in the matrix.
        index = LOCATION_DECODER_CHINESE[location]
        jpg_dir = os.path.join(root_dir, file)
        matrix_3x3[index[0]][index[1]] = jpg_dir

    # Finally process the last one.
    matrix_3x3 = blankfill(matrix_3x3, blank_img_dir)
    op_file = Grid(prev_id, matrix_3x3, format, img_size, img_rotate)
    op_file.convert2jpeg(out_dir)


# Read the error log and convert it to a dictionary (picture only).
def errlog_to_dict_for_jpeg(errlog_dir, image_dir, format='jpg'):
    err_dict = {}
    with open(errlog_dir, 'r') as f:
        lines = f.readlines()
        for line in lines:
            errfile = line.split('：')[0].strip('\n')
            errlog = line.split('：')[1].split('，')[0].strip('\n')

            # Convert .mp4 to .MOV.
            if errlog == 'mp4文件格式':
                origin = errfile.replace('_', '.mp4.') + '.' + format
                origin_dir = os.path.join(image_dir, origin)
                correct = errfile.replace('_', '.MOV.') + '.' + format
                correct_dir = os.path.join(image_dir, correct)
                err_dict[origin_dir] = correct_dir + '__TEMP__'

            # Convert old name to new name.
            elif errlog == '命名错误':
                if len(line) < 19:
                    origin = errfile.replace('_', '.MOV.') + '.' + format
                else:
                    ind = errfile.rfind('_')
                    origin = errfile[:ind] + '.MOV.' + errfile[ind+1:] + '.' + format
                origin_dir = os.path.join(image_dir, origin)
                correct = line.split('：')[1].split('，')[1].strip('\n').lstrip('改为')
                if correct != '删除':
                    correct = correct.replace('_', '.MOV.') + '.' + format
                    correct_dir = os.path.join(image_dir, correct)
                    err_dict[origin_dir] = correct_dir + '__TEMP__'
                else:
                    err_dict[origin_dir] = ''
    return err_dict


def rename_jpeg_files_based_on_errlog_dict(errlog_dict):
    for file in errlog_dict:
        if errlog_dict[file] != '':
            os.rename(file, errlog_dict[file])
        else:
            print('remove {}'.format(file))
            os.system('rm -rf "{}"'.format(file))
    # Revove __TEMP__ flag.
    for new_file in errlog_dict.values():
        if new_file != '':
            os.rename(new_file, new_file.rstrip('__TEMP__'))


# Extract 1 frame for each video.
def extract_frames(video_root_dir):
    file_names = os.listdir(video_root_dir)
    for name in file_names:
        file_dir = os.path.join(video_root_dir, name)
        suc, img = cv2.VideoCapture(file_dir).read()
        if not suc:
            logging.error('Read Video Clips from {} Failed!'.format(file_dir))
        else:
            location = video_root_dir.split('_')[-1]
            outpath = os.path.join(video_root_dir, name+'.'+location+'.jpg')
            cv2.imwrite(outpath, img)


# Stack multiple image in one.
# References: youtube.com/watch?v=WQeoO7MI0Bs
def stackImages(scale, imgArray):
    rows = len(imgArray)
    cols = len(imgArray[0])
    rowsAvailable = isinstance(imgArray[0], list)
    width = imgArray[0][0].shape[1]
    height = imgArray[0][0].shape[0]
    if rowsAvailable:
        for x in range(0, rows):
            for y in range(0, cols):
                if imgArray[x][y].shape[:2] == imgArray[0][0].shape[:2]:
                    imgArray[x][y] = cv2.resize(imgArray[x][y], (0, 0), None, scale, scale)
                else:
                    imgArray[x][y] = cv2.resize(imgArray[x][y], (imgArray[0][0].shape[1], imgArray[0][0].shape[0]), None, scale, scale)
                if len(imgArray[x][y].shape) == 2:
                    imgArray[x][y] = cv2.cvtColor(imgArray[x][y], cv2.COLOR_GRAY2BGR)
        imageBlank = np.zeros((height, width, 3), np.uint8)
        hor = [imageBlank]*rows
        hor_con = [imageBlank]*rows
        for x in range(0, rows):
            hor[x] = np.hstack(imgArray[x])
        ver = np.vstack(hor)
    else:
        for x in range(0, rows):
            if imgArray[x].shape[:2] == imgArray[0].shape[:2]:
                imgArray[x] = cv2.resize(imgArray[x], (0, 0), None, scale, scale)
            else:
                imgArray[x] = cv2.resize(imgArray[x], (imgArray[0].shape[1], imgArray[0].shape[0]), None, scale, scale)
            if len(imgArray[x].shape) == 2:
                imgArray[x] = cv2.cvtColor(imgArray[x], cv2.COLOR_GRAY2BGR)
        hor = np.hstack(imgArray)
        ver = hor
    return ver


class Grid():
    def __init__(self, name, file_matrix, type, ref_size, rotate=0):
        self._name = name
        self._width = len(file_matrix[0])
        self._height = len(file_matrix)
        self._matrix = file_matrix
        self._type = type
        self._angle = rotate
        self._ref_size = ref_size

    def convert2video(self, destination_dir):
        # Prepare the clips array structure.
        clips_matrix = [[0 for x in range(self._width)] for y in range(self._height)]
        row_index, col_index = 0, 0
        for row in self._matrix:
            for file_dir in row:
                clips_matrix[row_index][col_index] = VideoFileClip(file_dir).rotate(self._angle)
                col_index += 1
            row_index += 1
            col_index = 0

        # Download stacked video.
        video_matrix = clips_array(clips_matrix)
        out_dir = os.path.join(destination_dir, self._name) + '.' + self._type
        logging.info('Writing {} ...'.format(out_dir))
        video_matrix.write_videofile(out_dir, fps=24)

    def convert2jpeg(self, destination_dir):
        # Prepare the frame matrix structure.
        frame_matrix = [[0 for x in range(self._width)] for y in range(self._height)]
        row_index, col_index = 0, 0
        for row in self._matrix:
            for file_dir in row:
                suc = True
                if self._type == 'jpg' or self._type == 'jpeg':
                    frame_matrix[row_index][col_index] = cv2.imread(file_dir)
                    if (frame_matrix[row_index][col_index].shape[0], frame_matrix[row_index][col_index].shape[1]) != self._ref_size:
                        frame_matrix[row_index][col_index] = cv2.resize(frame_matrix[row_index][col_index], self._ref_size)
                else:
                    suc, frame_matrix[row_index][col_index] = cv2.VideoCapture(file_dir).read()
                if not suc:
                    logging.error('Read Video Clips Failed!')
                col_index += 1
            row_index += 1
            col_index = 0

        # Stack them in one and output the file.
        img_stack = stackImages(0.6, tuple(frame_matrix))
        out_dir = os.path.join(destination_dir, self._name) + '.' + IMG_FORMAT
        logging.info('Writing {} ...'.format(out_dir))
        cv2.imwrite(out_dir, img_stack)

    @property
    def name(self):
        return self._name

    @property
    def dimension(self):
        return (self._width, self._height)

    @property
    def matrix(self):
        return self._matrix
