New Functionality
^^^^^^^^^^^^^^^^^

- ``WarmStarEngine`` interchange is now capable of selecting specific managers to send tasks to, trying to assign
tasks to managers that have workers that executed the same function (i.e., same uuid) of the task previously,
allowing for warm start of dependencies.
  A new type of dispatch function was created for the interchange, to allow for this new method of assigning tasks
to managers. Using a function_manager_map, that tracks which managers executed each function, the new dispatch
algorithm is able to assign functions to managers that already executed them. This way, workers that already
executed the function can do it again without loading dependencies. In cases where this managers are at full
capacity, or a new function is being executed, the task is assigned to a random manager with free capacity.
  Instead of iterating through each manager like the naive dispatch, the warm start dispatch iterates for tasks,
first searching for managers in the function_manager_map and then on the remaining interesting managers.
  As of now, this dispatch is only available for hard scheduler mode, but if desire or necessity arrives, it
should be possible to expand it to adapt to the soft scheduler mode logic.
  The interchange also ensures that a disconnected/terminated manager is removed from the map, guaranteeing
that no task is sent to an unavailable or unregistered manager.
