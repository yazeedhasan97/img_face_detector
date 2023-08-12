import glob
import json, logging, os
import shutil
import subprocess
from datetime import datetime


def get_days_between_dates(date1, date2):
    # Convert the date strings to datetime objects
    datetime1 = datetime.strptime(date1, "%Y%m%d").date()
    datetime2 = datetime.strptime(date2, "%Y%m%d").date()

    # Calculate the number of days between the two dates
    num_days = abs((datetime2 - datetime1).days)

    return num_days


def find_base_directory():
    current_file = os.path.abspath(__file__)
    base_directory = os.path.dirname(current_file)
    INFO(f"The BaseDir is: {base_directory}")
    return base_directory


def load_json_config(path):
    try:
        with open(path) as file:
            loaded_dict = json.load(file)
            INFO(f"The Loaded Data is: {loaded_dict}")
            return loaded_dict
    except FileNotFoundError as e:
        ERROR(f"Config file '{path}' not found. \n\r{e}")
        raise e
    except json.JSONDecodeError as e:
        ERROR(f"Failed to load JSON from file '{path}'. Check if the file contains valid JSON. \n\t {e}")
        raise e
    except Exception as e:
        ERROR(f"An error occurred while loading JSON from file '{path}': {e}")
        raise e


def is_image(file_path):
    image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp']

    _, file_extension = os.path.splitext(file_path)

    lower_extension = file_extension.lower()

    if lower_extension in image_extensions:
        return True
    else:
        return False


def run_terminal_command(command):
    try:
        # Run the command and capture the output
        result = subprocess.run(command, shell=True, universal_newlines=True, check=False)
        print('Terminal Command Results:', result)
        INFO(f'Terminal Command: {result.args} ')
        INFO(f'Terminal Command Results: {result.returncode} ')

        return result.returncode
    except subprocess.CalledProcessError as e:
        ERROR("Command execution failed with error:", e)
        print("Command execution failed with error:", e)
        raise e
    except Exception as e:
        ERROR(f"Error executing command: {e}")
        print(f"Error executing command: {e}")
        raise e


def INFO(message):
    LOGGER.info(f"{os.getpid()} {message}")
    print(message)


def WARNING(message):
    LOGGER.warning(f"{os.getpid()} {message}")
    print(message)


def ERROR(message):
    LOGGER.error(f"{os.getpid()} {message}")
    print(message)


def DEBUG(message):
    LOGGER.debug(f"{os.getpid()} {message}")
    print(message)


def set_logger(name: str, path: str, is_test=False):
    global LOGGER
    LOGGER = logging.getLogger(name)
    try:
        formatter = logging.Formatter(
            '[%(asctime)s:%(levelname)s] || {%(pathname)s Line:%(lineno)d} -- %(message)s'
        )
        filename = os.path.join(path, f'{name}_{datetime.now():%Y%m%d_%H%M%S}.log')
        file_handler = logging.FileHandler(
            filename=filename
        )
        print(f"Log File: {filename}")
        if is_test:
            file_handler.setLevel(logging.DEBUG)
            LOGGER.setLevel(logging.DEBUG)
        else:
            file_handler.setLevel(logging.INFO)
            LOGGER.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        LOGGER.addHandler(file_handler)
        return filename
    except Exception as e:
        print("An error occurred while setting up the logger:", e)
        raise e


def recursive_op_files(source, destination, source_pattern, override=False, skip_dir=True, operation='copy'):
    files_count = 0
    try:
        assert source is not None, 'Please specify source path, Current source is None.'
        assert destination is not None, 'Please specify destination path, Current source is None.'

        if not os.path.exists(destination):
            INFO(f'Creating Dir: {destination}')
            os.mkdir(destination)

        items = glob.glob(os.path.join(source, source_pattern))

        for item in items:

            try:
                if os.path.isdir(item) and not skip_dir:
                    path = os.path.join(destination, os.path.basename(item))
                    # INFO(f'START {operation} FROM {item} TO {path}.')
                    files_count += recursive_op_files(
                        source=item, destination=path,
                        source_pattern=source_pattern, override=override
                    )
                else:
                    file = os.path.join(destination, os.path.basename(item))
                    INFO(f'START {operation} FROM {item} TO {file}.')
                    if not os.path.exists(file) or override:
                        if operation == 'copy':
                            shutil.copyfile(item, file)
                        elif operation == 'move':
                            shutil.move(item, file)
                        else:
                            raise ValueError(f"Invalid operation: {operation}")
                        files_count += 1
                    else:
                        raise FileExistsError(f'The file {file} already exists int the destination path {destination}.')
            except FileNotFoundError as e_file:
                ERROR(f"File not found error: {e_file}")
                print(f"File not found error: {e_file}")
            except PermissionError as e_permission:
                ERROR(f"Permission error: {e_permission}")
                print(f"Permission error: {e_permission}")
            except Exception as e_inner:
                ERROR(f"An error occurred: {e_inner}")
                print(f"An error occurred: {e_inner}")
    except AssertionError as e_assert:
        ERROR(f"Assertion error: {e_assert}")
        print(f"Assertion error: {e_assert}")
    except Exception as e_outer:
        ERROR(f"An error occurred: {e_outer}")
        print(f"An error occurred: {e_outer}")
    return files_count
