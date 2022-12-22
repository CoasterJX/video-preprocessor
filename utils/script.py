import os
import glob

from .settings import CAMERA_DECODER_CHINESE
from .settings import LOCATION_DECODER_CHINESE


# Write all commands to a .sh file.
def write_to_shfile(round_one, round_two, filename):
    with open(filename, 'w') as f:
        f.write('')
    with open(filename, 'a') as f:
        for cmd in round_one:
            f.write(cmd)
        for cmd in round_two:
            f.write(cmd)


# Extract all videos for person with specific id.
def get_videos_from_bucket(id_num, img_root_dir, hdfs_raw_data_dir, temp_folder='video_sync_temp'):
    jpg_filenames = glob.glob(os.path.join(img_root_dir, id_num + '*'))
    cmd = 'hdfs dfs -get'
    for dir in jpg_filenames:
        # Decode corresponding bkt file.
        name = dir.lstrip(img_root_dir).lstrip('/')
        info_in_list = name.split('.')
        location = info_in_list[2]
        camera = CAMERA_DECODER_CHINESE[location]
        bucket_filename = os.path.join(location, 'F' + id_num + '_cam' + camera) + '.MOV'
        bucket_dir = os.path.join(hdfs_raw_data_dir, bucket_filename)

        cmd = cmd + ' ' + bucket_dir

    # Process the videos in a temp file.
    temp_file = temp_folder + '_' + id_num

    cmd = cmd + ' ' + temp_file
    return cmd


# Generate a .sh file that can fix synchronization errors; at the same time, generate a tar.gz that stores the videos with problems.
def fix_sync_error(errlog_dir, shell_dir, stacked_video_dir, hdfs_root_dir, record_error=False, record_error_type=set(), video_format='mp4'):
    cmds_round_one = []
    cmds_round_two = []
    with open(errlog_dir, 'r') as f:
        lines = f.readlines()
        for line in lines:
            errfile_info = line.split('：')[0].strip('\n')
            ID_and_location = errfile_info.split('_')
            errlog = line.split('：')[1].strip('\n')

            # Combine the videos that has problems.
            if errlog in record_error_type:
                if record_error:
                    if not os.path.exists('err_videos'):
                        os.mkdir('err_videos')
                    destination_dir = 'err_videos/{}'.format(errlog)
                    if not os.path.exists(destination_dir):
                        os.mkdir(destination_dir)
                    errfile = os.path.join(stacked_video_dir, ID_and_location[0]) + '.' + video_format
                    op_dir = os.path.join(destination_dir, errfile_info) + '.' + video_format
                    cmd = 'cp {} {}'.format(errfile, op_dir)
                    os.system(cmd)

            # Delete the files.
            else:
                ID_and_location = errfile_info.split('_')
                target_file = os.path.join(hdfs_root_dir, 'F{id}/F{id}_cam{coord}_aligned.MOV'.format(id=ID_and_location[0], coord=CAMERA_DECODER_CHINESE[ID_and_location[1]]))
                cmds_round_one.append('hdfs dfs -rm -f {}\n\n'.format(target_file))

    cmds_round_one[-1] = cmds_round_one[-1][:-1]
    write_to_shfile(cmds_round_one, cmds_round_two, shell_dir)
    if record_error:
        tar_cmd = 'tar -zcvf err_videos.tar.gz err_videos'
        os.system(tar_cmd)
        os.system('rm -rf err_videos')


# Generate a .sh file that can fix all naming errors.
def fix_video_file_name(errlog_dir, hdfs_raw_data_dir, shell_dir):
    cmds_round_one = []
    cmds_rownd_two = []
    with open(errlog_dir, 'r') as f:
        lines = f.readlines()
        for line in lines:
            errfile = line.split('：')[0].strip('\n')
            errlog = line.split('：')[1].split('，')[0].strip('\n')

            # Rename mp4 to MOV.
            if errlog == 'mp4文件格式':
                # Extract file directory info.
                ID_and_location = errfile.split('_')
                bucket_file_old = os.path.join(ID_and_location[1], ID_and_location[0]) + '.mp4'
                bucket_dir_old = os.path.join(hdfs_raw_data_dir, bucket_file_old)
                # Extract new file directory info.
                bucket_file_new = os.path.join(ID_and_location[1], ID_and_location[0]) + '.MOV'
                bucket_dir_new = os.path.join(hdfs_raw_data_dir, bucket_file_new)
                # Waiting for commands to be placed into .sh file.
                cmds_round_one.append('hdfs dfs -mv {} {}\n'.format(bucket_dir_old, bucket_dir_new+'__TEMP__'))
                cmds_rownd_two.append('hdfs dfs -mv {} {}\n'.format(bucket_dir_new+'__TEMP__', bucket_dir_new))

            # Rename the videos that has the wrong ID or location.
            elif errlog == '命名错误':
                # Extract file directory info.
                if len(line) < 19:
                    old_ID_and_location = errfile.split('_')
                else:
                    ind = errfile.rfind('_')
                    old_ID_and_location = [errfile[:ind], errfile[ind+1:]]
                bucket_file_old = os.path.join(old_ID_and_location[1], old_ID_and_location[0]) + '.MOV'
                bucket_dir_old = os.path.join(hdfs_raw_data_dir, bucket_file_old)
                # Extract new file directory info.
                new_ID_and_location = line.split('，')[1].strip('\n').lstrip('改为').split('_')
                if new_ID_and_location[0] == '删除':
                    cmds_round_one.append('hdfs dfs -mv {} {}\n'.format(bucket_dir_old, bucket_dir_old+'__DELETE__'))
                    cmds_rownd_two.append('hdfs dfs -rm -f {}\n'.format(bucket_dir_old+'__DELETE__'))
                    continue
                bucket_file_new = os.path.join(new_ID_and_location[1], new_ID_and_location[0]) + '.MOV'
                bucket_dir_new = os.path.join(hdfs_raw_data_dir, bucket_file_new)
                # Waiting for commands to be placed into .sh file.
                cmds_round_one.append('hdfs dfs -mv {} {}\n'.format(bucket_dir_old, bucket_dir_new+'__TEMP__'))
                cmds_rownd_two.append('hdfs dfs -mv {} {}\n'.format(bucket_dir_new+'__TEMP__', bucket_dir_new))

    write_to_shfile(cmds_round_one, cmds_rownd_two, shell_dir)


# Generate a .sh file that can update naming format of videos.
def update_video_file_name(img_dir, hdfs_raw_data_dir, shell_dir):
    cmds = []
    jpg_filenames = os.listdir(img_dir)
    jpg_filenames.sort()
    for name in jpg_filenames:
        # Extract old directory.
        info_in_list = name.split('.')
        id_num, location = info_in_list[0], info_in_list[2]
        bucket_filename_old = os.path.join(location, id_num) + '.MOV'
        bucket_dir_old = os.path.join(hdfs_raw_data_dir, bucket_filename_old)
        # Define new directory.
        camera = CAMERA_DECODER_CHINESE[location]
        bucket_filename_new = os.path.join(location, 'F' + id_num + '_cam' + camera) + '.MOV'
        bucket_dir_new = os.path.join(hdfs_raw_data_dir, bucket_filename_new)
        cmds.append('hdfs dfs -mv {} {}\n'.format(bucket_dir_old, bucket_dir_new))

    write_to_shfile(cmds, [], shell_dir)


# Generate a .sh file that can be submitted to map_reduce for getting 1 picture for each video.
def extract_img_from_videos(hdfs_raw_data_dir, shell_dir, temp_file='video_sync_temp'):
    cmd = ''
    for location in LOCATION_DECODER_CHINESE:
        # Define the temp folder.
        temp_dir = temp_file + '_' + location
        # Extract video files.
        root_dir = os.path.join(hdfs_raw_data_dir, location)
        targets = os.popen('hdfs dfs -ls {}'.format(root_dir)).read().rstrip('\n').split('\n')[1:]
        count = 0
        for target in targets:
            if count == 0:
                cmd += 'mkdir {}'.format(temp_dir) + ' &&\n'
                cmd += 'hdfs dfs -get'
            vid_file = target.split(' ')[-1]
            cmd += ' ' + vid_file
            count += 1
            if count == 5:
                cmd += ' ' + temp_dir + ' &&\n'
                cmd += 'python3 main.py extract_frames {}'.format(temp_dir) + ' &&\n'
                output_dir = os.path.join(hdfs_raw_data_dir, '图片')
                jpg_files = os.path.join(temp_dir, '*.jpg')
                cmd += 'hdfs dfs -put {} {}'.format(jpg_files, output_dir) + ' &&\n'
                cmd += 'rm -rf {}'.format(temp_dir) + '\n\n'
                count = 0

        # Process the last portion of videos.
        if count != 0:
            cmd += ' ' + temp_dir + ' &&\n'
            cmd += 'python3 main.py extract_frames {}'.format(temp_dir) + ' &&\n'
            output_dir = os.path.join(hdfs_raw_data_dir, '图片')
            jpg_files = os.path.join(temp_dir, '*.jpg')
            cmd += 'hdfs dfs -put {} {}'.format(jpg_files, output_dir) + ' &&\n'
            cmd += 'rm -rf {}'.format(temp_dir) + '\n\n'

    with open(shell_dir, 'w') as f:
        f.write(cmd[:-1])


# Generate a .sh file that can be submitted to map_reduce for video synchronization.
def video_alignment(img_root_dir, stacked_img_root_dir, hdfs_raw_data_dir, hdfs_out_dir, shell_dir, ignore_set=set(), temp_sync='video_sync_temp', temp_op='video', format='mp4'):
    all_ids = []
    jpg_files = os.listdir(stacked_img_root_dir)
    for name in jpg_files:
        # Extract id.
        id_num = name.split('.')[0]
        all_ids.append(id_num)

    cmd = ''
    for id_num in all_ids:
        if id_num in ignore_set:
            continue
        target_hdfs = os.path.join(hdfs_out_dir, 'F{}'.format(id_num))
        cmd += 'hdfs dfs -rm -f -r {}'.format(target_hdfs) + ' &&\n'
        cmd += 'hdfs dfs -mkdir {}'.format(target_hdfs) + ' &&\n'
        dir_for_checkonly = os.path.join(target_hdfs, 'checkonly')
        cmd += 'hdfs dfs -mkdir {}'.format(dir_for_checkonly) + ' &&\n'
        temp_file = temp_sync + '_' + id_num
        cmd += 'mkdir {}'.format(temp_file) + ' &&\n'
        get_video_cmd = get_videos_from_bucket(id_num, img_root_dir, hdfs_raw_data_dir, temp_folder=temp_sync)
        cmd += get_video_cmd + ' &&\n'
        cmd += 'python3 main.py {}'.format(temp_file) + ' &&\n'
        videofile = os.path.join(temp_op, id_num+'.'+format)
        jsonfile = os.path.join(temp_op, id_num+'.json')
        cmd += 'hdfs dfs -put {} {} {}'.format(videofile, jsonfile, dir_for_checkonly) + ' &&\n'
        cmd += 'hdfs dfs -put {} {}'.format(os.path.join(temp_file, '*_aligned.MOV'), target_hdfs) + ' &&\n'
        cmd += 'rm -rf {} {} {}'.format(videofile, jsonfile, temp_file) + '\n\n'

    with open(shell_dir, 'w') as f:
        f.write(cmd[:-1])
