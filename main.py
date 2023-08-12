# This is a sample Python script.
import argparse
import os.path
import sys
import time


import controller
import db
import utils


# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def main(args):
    log_file = utils.set_logger('ImgFaceDetector', path=args.log_dir)
    utils.INFO(f"Logs will be written inside: {log_file}")

    config = utils.load_json_config(args.config)
    utils.INFO(f"Loaded Configs: {config}")

    dbsession = db.create_database_session(config['audit'])
    del config['audit']

    if not os.path.exists(config['run']['output_path']):
        os.mkdir(config['run']['output_path'])

    watcher = controller.Watcher(
        auto_start=True,
        recursive=config['run']['Recursive'],
        dbsession=dbsession,
        path=config['run']['input_path'],
        output=config['run']['output_path']
    )

    try:
        while True:
            time.sleep(0.25)
    except KeyboardInterrupt:
        utils.WARNING('Service was stopped forcefully. Finalizing before stopping.')
    except Exception as e:
        utils.ERROR(f'Service shutdown with unknown error: {e}')
    finally:
        db.commit_observations(dbsession)
        watcher.stop()
        sys.exit(0)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--log_dir', required=True,
                        help='Path to the directory to save generated logs inside')
    parser.add_argument('-c', '--config', required=True, help='Path to the log file')
    args = parser.parse_args()

    main(args)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
