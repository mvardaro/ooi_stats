# description

this repository contains the scripts that produce the statistics seen under `monthly stats` and `daily stats` at http://ooi.visualocean.net/. the repository is under active development. once finalized, the process will be documented and described in more detail.


# environment setup

`conda create -n stats python=3 requests pandas`


# content

`scripts/deployment_data_requests_loop.py`    
For each refdes, deployment, method, science stream combination:
* Request the deployment time ranges  
* Build a request url for each day within the deployment time range  
* Send the requests and stack returns into a data frame with  
  * Refdes  
  * Method  
  * Stream Name  
  * Date  
  * Value  
    * 0 if deployed, but no data returned for that day  
    * 1 if deployed and data returned for that day  



`scripts/refdes_method_reduction.py`  
These scripts roll up the status for a given day to the reference designator and stream type levels. Two seperate outputs are created.
* refdes  
  * if any stream under a reference designator recorded a data point for a given date, record 1, esle 0. This masks progress on ingestion where telemetered was available, but recovered data has been filled in.
* stream_type  
  * if any streamed, recovered or telemetered stream type received a data point for a given date, record 1, esle 0. This output is currently not being visualized at http://ooi.visualocean.net/, but will capture ingestion progress, where one stream type was previously available, but another wasn't.