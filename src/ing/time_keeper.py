import time


class TimeKeeper:
    """
    Timer for remembering the time a given task started and print a task done message when the task is completed, with
    the time it took for the completion. The task should be sandwiched with a starting expression and an ending expression for this to work.
        - Starting expression could be : [ TimeKeeper constructor, tk.reset, tk.next ]
        - Ending expression could be : [ tk.done, tk.next ]

    Example
    --------

        $tk = TimeKeeper("foo() function running")
        $foo()
        $tk.done()

    Attributes
    ----------
    t: float
        The time spent processing
    end_string: str
        The string that will be printed for final string
    """
    def __init__(self, in_work_string):
        self.t = None
        self.end_string = ""
        self.reset(in_work_string)

    def done(self):
        """
        Prints the task done message and the time it took for running.
        """
        print("{}T={} seconds".format(self.end_string, time.process_time() - self.t))

    def reset(self, in_task_name_string: str):
        """
        Restarts the timer with a new task name.

        Parameters
        ----------
        in_task_name_string :
            The name of the new task.
        """
        self.t = time.process_time()
        fill_length = max(0, (70 - len(in_task_name_string)))
        pattern_repeats = fill_length // 5
        space_repeats = fill_length % 5
        self.end_string = "{} done. {}{}".format(in_task_name_string, " " * space_repeats, " ... " * pattern_repeats)
        print(f"{in_task_name_string}...")

    def next(self, in_task_name_string: str):
        """
        Completes the current task and starts timer with a new task.
        Parameters
        ----------
        in_task_name_string :
            The name of the new task.
        """
        self.done()
        self.reset(in_task_name_string)
