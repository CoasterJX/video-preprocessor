import os
import sys
import logging
import subprocess

from utils.settings import PATH_TO_IMAGE
from utils.settings import ERRLOG_NAME
from utils.settings import HDFS_ROOT_DIRECTORY
from utils.settings import FIX_NAME_SHELL
from utils.settings import UPDATE_NAME_SHELL
from utils.settings import EXTRACT_IMAGE_SHELL
from utils.settings import ERRLOG_SYNC
from utils.settings import PATH_TO_STACKED_IMAGE
from utils.settings import HDFS_OUTPUT_DIRECTORY
from utils.settings import MAP_REDUCE_FILE
from utils.settings import LOCAL_ROOT_DIRECTORY
from utils.settings import PATH_TO_STACKED_VIDEO
from utils.settings import PATH_TO_ERRLOG
from utils.settings import PATH_TO_SHELL
from utils.settings import HDFS_TEMP_STORAGE
from utils.settings import FIX_SYNC_SHELL

logging.basicConfig(level=logging.DEBUG)


def check_call(cmd_str, env=None):
    try:
        logging.info("[Exec] {}".format(cmd_str))
        if env is None:
            subprocess.check_call(cmd_str, shell=True)
        else:
            subprocess.check_call(cmd_str, shell=True, env=env)
        return True
    except subprocess.CalledProcessError as cpe:
        logging.error("[command]: %s", cpe.cmd)
        logging.error("[return-code]: %s", cpe.returncode)
        logging.error("[output]: %s", cpe.output)
        return False


def run_cmd(cmd):
    suc = check_call(cmd)
    if not suc:
        logging.error('Command "{}" Failed!'.format(cmd))
        exit()


def stage_1():
    logging.info('Start the Preprocess - Stage 1')

    logging.info('Creating Project {}'.format(LOCAL_ROOT_DIRECTORY))
    if not os.path.exists('checkonly'):
        logging.critical('Project is corrupted - No Blank Image Found!')
        exit()
    if os.path.exists(LOCAL_ROOT_DIRECTORY):
        logging.error('Stage 1 Failure - Project Already Exists!')
        exit()
    os.mkdir(LOCAL_ROOT_DIRECTORY)
    os.mkdir(PATH_TO_IMAGE)
    os.mkdir(PATH_TO_STACKED_IMAGE)
    os.mkdir(PATH_TO_STACKED_VIDEO)
    os.mkdir(PATH_TO_ERRLOG)
    os.mkdir(PATH_TO_SHELL)

    logging.info('Start Image Extraction')
    hdfs_img_dir = os.path.join(HDFS_ROOT_DIRECTORY, '图片')
    run_cmd('hdfs dfs -mkdir {}'.format(hdfs_img_dir))
    run_cmd('hdfs dfs -chmod -R 777 {}'.format(hdfs_img_dir))

    logging.info('Creating shell script to extract images, please wait...')
    run_cmd('python3 main.py extract_img_from_videos')
    run_cmd('chmod -R 777 {}'.format(EXTRACT_IMAGE_SHELL))

    if os.path.exists('../map_reduce_submit_directory'):
        run_cmd('rm -rf ../map_reduce_submit_directory')
    os.mkdir('../map_reduce_submit_directory')

    logging.info('Sending to Map Reduce...')
    run_cmd('cp -R {} main.py utils ../map_reduce_submit_directory'.format(EXTRACT_IMAGE_SHELL))

    shell_name = EXTRACT_IMAGE_SHELL[EXTRACT_IMAGE_SHELL.rfind('/')+1:]
    shell_dir = os.path.join('../map_reduce_submit_directory', shell_name)
    map_cmd = 'bash -x submit.sh 6 {} {}'.format(shell_dir, HDFS_TEMP_STORAGE)

    map_reduce_dir = '../map_reduce_general_tool'
    os.chdir(map_reduce_dir)
    run_cmd(map_cmd)

    os.chdir('../fyz_video_preprocess')
    logging.info('Get images, please wait for minutes...')
    image_dir = os.path.join(HDFS_ROOT_DIRECTORY, '图片/*')
    run_cmd('hdfs dfs -get {} {}'.format(image_dir, PATH_TO_IMAGE))

    logging.info('Stacking Images, please wait...')
    run_cmd('python3 main.py merge {}'.format(PATH_TO_IMAGE))

    logging.info('Stage 1 Successful!')
    os.system('touch {}'.format(ERRLOG_NAME))
    logging.info('Now go to {} and download these files. Have a look at those images. \
Find and record all the errors in {}. Error format is given in README, please read carefully.'.format(PATH_TO_STACKED_IMAGE, ERRLOG_NAME))
    logging.info('If you finish recording all the errors, please continue preprocessing Stage 2.')


def stage_2():
    logging.info('Start the Preprocess - Stage 2')

    logging.info('Fixing Video Names, Please Wait...')
    run_cmd('python3 main.py fix_name_error')
    run_cmd('chmod -R 777 {}'.format(FIX_NAME_SHELL))
    run_cmd(FIX_NAME_SHELL)

    logging.info('Successfully Fixed Video Names')

    logging.info('Updating Video Names, This might take about 30 minutes, Please Wait...')
    run_cmd('python3 main.py update_filename')
    run_cmd('chmod -R 777 {}'.format(UPDATE_NAME_SHELL))
    run_cmd(UPDATE_NAME_SHELL)

    logging.info('Successfully Update Video Names')
    logging.info('Stage 2 Success!')


def stage_3():
    logging.info('Start Preprocess - Stage 3')

    logging.info('Aligning Videos...')
    logging.info('Generating shells for video alignment, Please Wait...')
    stacked_imgs = os.path.join(PATH_TO_STACKED_IMAGE, '*')
    run_cmd('rm -rf {}'.format(stacked_imgs))
    run_cmd('python3 main.py merge {}'.format(PATH_TO_IMAGE))
    run_cmd('python3 main.py align')
    run_cmd('chmod -R 777 {}'.format(MAP_REDUCE_FILE))

    if not os.path.exists('video'):
        os.mkdir('video')

    if os.path.exists('../map_reduce_submit_directory'):
        os.system('rm -rf ../map_reduce_submit_directory')
    os.mkdir('../map_reduce_submit_directory')

    logging.info('Sending to Map Reduce...')
    run_cmd('cp -R {} utils video main.py ../map_reduce_submit_directory'.format(MAP_REDUCE_FILE))

    shell_name = MAP_REDUCE_FILE[MAP_REDUCE_FILE.rfind('/')+1:]
    shell_dir = os.path.join('../map_reduce_submit_directory', shell_name)
    map_cmd = 'bash -x submit.sh 10 {} {}'.format(shell_dir, HDFS_TEMP_STORAGE)

    map_reduce_dir = '../map_reduce_general_tool'
    os.chdir(map_reduce_dir)
    run_cmd(map_cmd)

    os.chdir('../fyz_video_preprocess')
    logging.info('Successfulley Aligned Videos!')
    logging.info('Stage 3 Successful!')
    run_cmd('touch {}'.format(ERRLOG_SYNC))

    stacked_dir = os.path.join(HDFS_OUTPUT_DIRECTORY, 'F*/checkonly/*.mp4')
    logging.info('Once you think the number of files in {} is enough, extract {} to {} or any directory you like. \
Check the alignment quality and record the error in {}. Error format can be found in README. After finish recording please \
finish preprocessing by doing Stage 4.'.format(HDFS_OUTPUT_DIRECTORY, stacked_dir, PATH_TO_STACKED_VIDEO, ERRLOG_SYNC))


def extract_stacked_videos():
    logging.info('Start Process - Extract Stacked Videos')

    logging.info('Extracting Stacked Videos, Please Wait...')
    stacked_dir = os.path.join(HDFS_OUTPUT_DIRECTORY, 'F*/checkonly/*.mp4')
    run_cmd('hdfs dfs -get {} {}'.format(stacked_dir, PATH_TO_STACKED_VIDEO))

    logging.info('Get Stacked Videos Successfully')


def stage_4():
    logging.info('Strat Process - Stage 4')

    logging.info('Generating shell...')
    run_cmd('python3 main.py fix_sync_error')
    run_cmd('chmod -R 777 {}'.format(FIX_SYNC_SHELL))
    logging.info('Fixing sync errors, please wait...')
    run_cmd(FIX_SYNC_SHELL)

    logging.info('Stage 4 Successful!')
    print('FYZ video_preprocess is done.')


if __name__ == '__main__':
    if len(sys.argv) == 2:
        if sys.argv[1] == 'stage_1':
            stage_1()
        if sys.argv[1] == 'stage_2':
            stage_2()
        if sys.argv[1] == 'stage_3':
            stage_3()
        if sys.argv[1] == 'stage_4':
            stage_4()
        if sys.argv[1] == 'extract_stacked_videos':
            extract_stacked_videos()
