class NoProposalsConfiguredError(Exception):
    """Used when there are no proposals configured to run.
    """
    pass

class SchedulerTimeoutError(Exception):
    """Used when the Scheduler times out during target loop.
    """
    pass
