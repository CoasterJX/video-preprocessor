import os
import re
import json

from .settings import CAMERA_TO_COORDINATE

from moviepy.editor import VideoFileClip
from moviepy.editor import clips_array

from .align_videos_by_soundtrack import align


def extract_video_portions(dir):
    video_names = os.listdir(dir)
    for name in video_names:
        video_dir = os.path.join(dir, name)
        os.system('ffmpeg -i {} -ss 0 -t 60 -c copy {}'.format(video_dir, os.path.join(dir, name.replace('.MOV', '_portion.MOV'))))


def sync_videos(root_dir, output_dir, advanced_sync=False):
    video_names = os.listdir(root_dir)
    id_num = re.findall('F(.*?)_cam', video_names[0])[0]
    cmd = 'alignment_info_by_sound_track --json'
    for name in video_names:
        if advanced_sync and '_portion' in name:
            continue
        elif not advanced_sync and '_portion' not in name:
            continue
        video_dir = os.path.join(root_dir, name)
        cmd = cmd + ' ' + video_dir
    align_info = align.main(cmd.split(' '))
    print('===========')
    print(align_info)

    # Delete all nonnecessary infos.
    for i in range(len(align_info['edit_list'])):
        del align_info['edit_list'][i][1]['pad']
        del align_info['edit_list'][i][1]['pad_post']
        del align_info['edit_list'][i][1]['trim_post']

    # Save the align info in a json file.
    json_opdir = os.path.join(output_dir, id_num + '.json')
    with open(json_opdir, 'w') as f:
        json.dump(align_info, f)

    # Prepare for aligned file matrix.
    file_matrix = [['' for x in range(3)] for y in range(3)]
    for trim_info in align_info['edit_list']:
        trim_val = trim_info[1]['trim']
        if advanced_sync:
            outdir = trim_info[0].replace('.MOV', '_aligned.MOV')
            os.system('ffmpeg -i {} -ss {} -c copy {}'.format(trim_info[0], round(abs(trim_val), 3), outdir))
            camera = re.findall('cam(.*?).MOV', trim_info[0])[0]
        else:
            outdir = trim_info[0].replace('_portion.MOV', '_aligned.MOV')
            os.system('ffmpeg -i {} -ss {} -c copy {}'.format(trim_info[0].replace('_portion.MOV', '.MOV'), round(abs(trim_val), 3), outdir))
            camera = re.findall('cam(.*?)_portion.MOV', trim_info[0])[0]

        row, col = CAMERA_TO_COORDINATE[camera][0], CAMERA_TO_COORDINATE[camera][1]
        file_matrix[row][col] = outdir

    return file_matrix


def merge_videos(file_matrix, output_dir, fps=24, rotate_angle=0, zoom=1, format='mp4', start=20, end=30):
    # Firstly fill in the blankspace based on a non-empty videofile.
    ref_video = ''
    done = False
    for row in file_matrix:
        for video_file in row:
            if video_file != '':
                ref_video = video_file
                done = True
                break
        if done:
            break

    # Convert it to a clips array (version 0.1.0).
    clips_matrix = [[0 for x in range(3)] for y in range(3)]
    row_ind, col_ind = 0, 0
    for row in file_matrix:
        for video_file in row:
            if video_file == '':
                clips_matrix[row_ind][col_ind] = VideoFileClip(ref_video).rotate(rotate_angle).subclip(0, 0).resize(zoom)
            else:
                clips_matrix[row_ind][col_ind] = VideoFileClip(video_file).rotate(rotate_angle).resize(zoom).subclip(start, end)
            col_ind += 1
        row_ind += 1
        col_ind = 0

    # Create 4th row for vertical lip moving check (version 0.1.1).
    if file_matrix[0][1] != '' and file_matrix[1][1] != '' and file_matrix[2][1] != '':
        c1 = VideoFileClip(file_matrix[0][1]).rotate(rotate_angle).resize(zoom).subclip(start, end)
        c2 = VideoFileClip(file_matrix[1][1]).rotate(rotate_angle).resize(zoom).subclip(start, end)
        c3 = VideoFileClip(file_matrix[2][1]).rotate(rotate_angle).resize(zoom).subclip(start, end)
        fourth_row = [c1, c2, c3]
    elif file_matrix[0][2] != '' and file_matrix[1][2] != '' and file_matrix[2][2] != '':
        c1 = VideoFileClip(file_matrix[0][2]).rotate(rotate_angle).resize(zoom).subclip(start, end)
        c2 = VideoFileClip(file_matrix[1][2]).rotate(rotate_angle).resize(zoom).subclip(start, end)
        c3 = VideoFileClip(file_matrix[2][2]).rotate(rotate_angle).resize(zoom).subclip(start, end)
        fourth_row = [c1, c2, c3]
    else:
        c1 = VideoFileClip(file_matrix[0][0]).rotate(rotate_angle).resize(zoom).subclip(start, end)
        c2 = VideoFileClip(file_matrix[1][0]).rotate(rotate_angle).resize(zoom).subclip(start, end)
        c3 = VideoFileClip(file_matrix[2][0]).rotate(rotate_angle).resize(zoom).subclip(start, end)
        fourth_row = [c1, c2, c3]
    clips_matrix.append(fourth_row)

    # Move the last 2 row into horizontal (version 0.1.2).
    clips_matrix[0].append(VideoFileClip(ref_video).rotate(rotate_angle).subclip(0, 0).resize(zoom))
    clips_matrix[1].append(VideoFileClip(ref_video).rotate(rotate_angle).subclip(0, 0).resize(zoom))
    fourth_row = clips_matrix.pop(3)
    third_row = clips_matrix.pop(2)
    clips_matrix[0].extend(third_row)
    clips_matrix[1].extend(fourth_row)

    # Write the output file.
    stacked_video = clips_array(clips_matrix)
    output_file = re.findall('F(.*?)_cam', ref_video)[0] + '.' + format
    output_dir = os.path.join(output_dir, output_file)
    stacked_video.write_videofile(output_dir, fps=fps)
