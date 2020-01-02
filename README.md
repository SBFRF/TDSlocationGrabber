# TDSlocationGrabber
find historical locations of gauge placements and place into lookup table
check example file for how to querey developed database file
This takes on the order of several hours to run, when quereying due to timeouts from webservices

I run on a weekly cron-job.


- to run: 

   `python frfTDSdataCrawler.py chl`

this will run and crawl the CHLdata thredds location in the `waves` and `currents` folder to start by default. to 
modify line 103 in frfTDSdataCrawler.py
