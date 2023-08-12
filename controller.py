from threading import Lock, Timer
import socket

import cv2
import face_recognition

import os
from datetime import datetime

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

import db
import utils
import psutil


class Periodic:
    """ A periodic task running in threading.Timers """

    def __init__(self, interval, function, *args, **kwargs):
        self._lock = Lock()
        self._timer = None
        self.function = function
        self.interval = interval
        self.args = args
        self.kwargs = kwargs
        self._stopped = True

        if kwargs.pop('autostart', True):
            self.start()

    def start(self, from_run=False):
        self._lock.acquire()
        if from_run or self._stopped:
            self._stopped = False
            self._timer = Timer(self.interval, self._run)
            self._timer.start()
        self._lock.release()

    def _run(self):
        self.start(from_run=True)
        self.function(*self.args, **self.kwargs)

    def stop(self):
        self._lock.acquire()
        self._stopped = True
        self._timer.cancel()
        self._lock.release()


class Watcher(FileSystemEventHandler):
    def __init__(self, path, dbsession, output, recursive=True, auto_start=True, ):
        super().__init__()
        self.path = path
        self.__recursive = recursive
        self.__observer = Observer()
        self.dbsession = dbsession
        self.output = output
        self.__auto_start = auto_start

        if auto_start:
            self.start()

    def __get_observer(self):
        return self.__observer

    def __set_observer(self, observer):
        self.__observer = observer

    def __del_observer(self, ):
        self.__observer = None

    observer = property(
        fset=__set_observer,
        fget=__get_observer,
        fdel=__del_observer,
    )

    def start(self):
        if not os.path.exists(self.path):
            utils.ERROR(f"Directory '{self.path}' does not exist.")
            raise ValueError(f"Directory '{self.path}' does not exist.")
        self.__observer.schedule(self, self.path, recursive=self.__recursive)
        self.__observer.start()

    def stop(self):
        self.__observer.stop()
        self.__observer.join()

    def on_created(self, event):
        if event.is_directory:
            utils.INFO(f"[Directory created] Path: {event.src_path}")
        else:
            utils.INFO(f"[File created] Path: {event.src_path}")

        self.process_event(event, 'create')

    def on_modified(self, event):
        if event.is_directory:
            utils.INFO(f"[Directory modified] Path: {event.src_path}")
        else:
            utils.INFO(f"[File modified] Path: {event.src_path}")

        self.process_event(event, 'modify')

    def on_deleted(self, event):
        if event.is_directory:
            utils.INFO(f"[Directory deleted] Path: {event.src_path}")
        else:
            utils.INFO(f"[File deleted] Path: {event.src_path}")

        # self.process_event(event, 'remove')

    def on_moved(self, event):
        print(event.pid)
        if event.is_directory:
            utils.INFO(f"[Directory moved] From: {event.src_path} To: {event.dest_path}")
        else:
            utils.INFO(f"[File moved] From: {event.src_path} To: {event.dest_path}")

        # self.process_event(event, 'move')

    def process_event(self, event, event_type: str):
        tbl_dt = int(datetime.now().strftime('%Y%m%d'))
        photo_path = os.path.abspath(event.src_path)
        node = socket.gethostbyname(socket.gethostname())

        pid = os.getpid()  # Parent process ID
        puser = os.getlogin()  # Parent process username

        system = f"Available CPUs:{os.cpu_count()},Available Memory:{psutil.virtual_memory().available / (1024.0 ** 2)}MB"

        prediction_start_time = datetime.now()
        if utils.is_image(photo_path):
            predictions_path, prediction_status, contain_faces = predict(
                input=photo_path,
                output=self.output
            )
        else:
            utils.WARNING('The provided file is not an image file.')
            predictions_path = None
            prediction_status = None
            contain_faces = None
        prediction_end_time = datetime.now()

        data = {
            "event_type": event_type,
            "tbl_dt": tbl_dt,
            "photo_path": photo_path,
            "node": node,
            "pid": pid,
            "puser": puser,
            "system": system,
            "prediction_end_time": prediction_end_time,
            "prediction_start_time": prediction_start_time,
            "predictions_path": predictions_path,
            "prediction_status": prediction_status,
            "contain_faces": contain_faces,
        }
        utils.INFO(f'Values: {data} are written to DB.')
        # Get the host IP address
        db.create_and_insert_observation(
            session=self.dbsession,
            data=data
        )
        utils.INFO('New Observation Insertion to DB done successfully')


def predict(input, output):
    output_dir = os.path.join(output, os.path.basename(os.path.splitext(input)[-2]))

    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    image = face_recognition.load_image_file(input)
    face_locations = face_recognition.face_locations(image)
    utils.INFO(f"Face locations details: {face_locations}")
    if not face_locations:
        return output_dir, 'fail', False

    predictions_file = os.path.join(output_dir, 'preds.txt')
    with open(predictions_file, 'tw') as file:
        for i, face_location in enumerate(face_locations):
            top, right, bottom, left = face_location
            utils.INFO(
                f'Face {i} is located at pixel location Top: {top}, Left: {right}, Bottom: {bottom}, Right: {left}'
            )
            file.write(' '.join(list(map(str, [i, top, right, bottom, left]))))

            face_image = image[top:bottom, left:right]
            cv2.imwrite(os.path.join(output_dir, f"{i}.{os.path.splitext(input)[-1]}"), face_image)
    return predictions_file, 'success', True

# if __name__ == "__main__":
#     watcher = Watcher(path=path, logger=logger, dbsession=dbsession, recursive=recursive, auto_start=auto_start, )
#     import time
#
#     time.sleep(5)
#     watcher.stop()
