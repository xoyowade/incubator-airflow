from datetime import datetime
import logging

from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

class ScheduleTimeDeltaSensor(BaseSensorOperator):
    """
    Waits for a timedelta after the task's schedule time, which is
    approximated with the first poke time.

    :param delta: time length to wait after execution_date before succeeding
    :type delta: datetime.timedelta
    """
    template_fields = tuple()

    @apply_defaults
    def __init__(self, delta, *args, **kwargs):
        super(ScheduleTimeDeltaSensor, self).__init__(*args,
                poke_interval=delta.seconds, **kwargs)
        self.delta = delta
        self.target_dttm = None

    def poke(self, context):
        now = datetime.now()
        if not self.target_dttm:
            self.target_dttm = now + self.delta
            return False

        return now > self.target_dttm

    def is_ready(self, context):
        return True
