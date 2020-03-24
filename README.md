# Utility Library

Hey there. This repository contains the most classes and functions which I used in multiple projects.
There is stuff like file handling, timeouts, multi-threading, caching, randomizing, logger setup etc.

## usage
Well because it is more of a collection than anything, you should probably just copy and paste the files.

Be careful, some of the class depend on each other, e.g. the filehandler is needed by a lot of classes.a

Furthermore the filehandler sets the project root in "../../" of itself.
If it is different for you, you should change the `PROJECT_ROOT_RELATIVE_TO_THIS_FILE` in `filehandler.py`.
Like I said, a lot depends on it ;)

And another thing: replace all the `logging.getLogger(__name__)` with the `logger_name` you set in `setup_logger`

## contribution
I am open for pull requests, although its more likely for you to just copy and build upon the files.