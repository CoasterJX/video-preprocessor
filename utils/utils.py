import os
import re


def blankfill(matrix, blank_img_dir, video=False):
    filled_matrix = matrix
    row_ind, col_ind = 0, 0
    for row in matrix:
        for file_dir in row:
            if file_dir == '':
                filled_matrix[row_ind][col_ind] = blank_img_dir
            col_ind += 1
        row_ind += 1
        col_ind = 0
    return filled_matrix


# This will ignore ids that is given or already done.
def map_ignore(specific_ids, hdfs_out_dir):
    # Check merged mp4.
    find_cmd = os.path.join(hdfs_out_dir, '*/checkonly/*.mp4')
    output = os.popen('hdfs dfs -ls {}'.format(find_cmd)).read()
    all_ids = re.findall('checkonly/(.*?).mp4', output)
    ignore_ids = set()
    for id_num in specific_ids:
        ignore_ids.add(id_num)
    for id_num in all_ids:
        ignore_ids.add(id_num)

    # Check aligned videos.
    find_cmd = os.path.join(hdfs_out_dir, '*/*')
    output = os.popen('hdfs dfs -ls {}'.format(find_cmd)).read()
    targets = re.findall('FYZ_multimodal/(.*?)._COPYING_', output)
    for target in targets:
        id_num = re.findall('/F(.*?)_cam', target)[0]
        ignore_ids.remove(id_num)

    return ignore_ids
