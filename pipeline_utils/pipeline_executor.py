import logging

from configuration.config import Config
from utils.files_operations import is_file_exist, save_to_pickle, load_pickle
from utils.timer import Timer


class PipelineExecutor:

    def __init__(self, pipeline_class, clear_history=True, pipeline_path=None, save_state=True):

        self.pipeline_class = pipeline_class
        self.clear_history = clear_history
        self.pipeline_path = pipeline_path or Config.PIPELINE_PATH
        self.save_state = save_state

        self.pipeline = self.__get_pipeline(pipeline_class)

    def execute(self, steps):

        for step in steps:

            step_name = step.__name__
            logging.info("================ Executing: " + step_name + " ===========")

            getattr(self.pipeline, step_name)()

            self.__save_pipeline_if_needed()
            logging.info("================ Finished: " + step_name + " ===========")

    def __get_pipeline(self, pipeline_class):

        if self.clear_history:
            return pipeline_class()

        elif not is_file_exist(self.pipeline_path):
            return pipeline_class()

        else:
            return self.__load_pipeline()

    def __save_pipeline_if_needed(self):

        if self.save_state:
            timer = Timer('saving pipeline...').start()

            save_to_pickle(self.pipeline, self.pipeline_path)

            timer.end()

    def __load_pipeline(self):

        timer = Timer('Loading pipeline...').start()

        pipeline = load_pickle(self.pipeline_path)

        timer.end()

        return pipeline

