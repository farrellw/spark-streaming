# Spark Streaming
## Description
Application which processes streaming / micro batches of slack message data.
## Technology & Dependencies
- Scala
- SBT
- Spark


## TODO List
- Take in a batch of slack messages. Map the slack messages, removing unnessary attributes.
- Give each slack message an analysis score on mood
- Give each slack message a score on whether it occurred during inno hours.
- Compare the averages of mood during / away from inno hours. (or by different times of day)
- Insert into a datastore.