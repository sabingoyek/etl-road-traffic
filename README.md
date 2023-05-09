# Road Traffic ETL Pipeline
Analyzing the road traffic data from different toll plazas.

## Scenario
As a data engineer at a data analytics consulting company I have been assigned to a project that aims to de-congest the national highways by analyzing the road traffic data from different toll plazas. Each highway is operated by a different toll operator with different IT setup that use different file formats. As a vehicle passes a toll plaza, the vehicle's data like vehicle_id,vehicle_type,toll_plaza_id and timestamp are streamed to Kafka.
In the first lab the task is to collect data available in different formats and, consolidate it into a single file. In the second one I create a data pipe line that collects the streaming data and loads it into a database.
