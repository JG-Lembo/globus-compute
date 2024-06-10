New Functionality
^^^^^^^^^^^^^^^^^

- ``WarmStarEngine`` manager is now capable of selecting specific workers to send tasks to, trying to attribute
tasks to workers that have executed the same function (i.e., same uuid) of the task previously,
allowing for warm start of dependencies.
  The manager ensures that the worker is not busy with another task and that it will not be selected for
another task at the same time.
  Now, when a task is to be sent to some worker, the manager uses a task_function_map to get the function_uuid,
which is then used to get the list of workers that have already executed this function. Then, it sends
the task to the first worker that is not busy. If they all are, then the manager simply gets a free worker
from the worker queue.
  The manager also ensures that a disconnected/terminated worker is removed from the map, guaranteeing
that no task is sent to an unavailable worker.
