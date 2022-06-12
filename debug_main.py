from configuration.config import Config
from pipeline_utils.pipeline import Pipeline
from pipeline_utils.pipeline_executor import PipelineExecutor

CLEAR_HISTORY = False
SAVE_STATE = True


STEPS = [

]


def main():
    Config()

    executor = PipelineExecutor(
        pipeline_class=Pipeline,
        clear_history=CLEAR_HISTORY,
        save_state=SAVE_STATE,

    )
    executor.execute(STEPS)


if __name__ == '__main__':
    main()
