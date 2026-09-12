* NOTE gm_tasks.list contains the key (region code) list for the regions that we want to process
* DONE build docker image from fork
* DONE fix the date
* TODO run the docker image
- (odc-stats) docker build -t usgsgm:dev .
- (working-dir)
- mkdir output
- make up
- make bash
- (/src) python usgs_gm.py
- (/src) python esa_gm.py
* NOTE LS: run for 1 month, m7a.8xlarge, 128G RAM, 32 CPUs. 4GB input when COG'd. About 12 minutes.
* NOTE LS: run for 1 year, r6a.48xlarge, 1.5T RAM, 192 CPUs. About 150 minutes.
* NOTE S2 10m 4 bands: run for 1 month, r6a.48xlarge, 1.5T RAM, 192 CPUs. mem at 20%. not good. 33 minutes. $USD 6.50
* NOTE S2 10m 4 bands: 1 month, m7a.16xlarge, 64 CPUs, 256G RAM. 50 minutes. $USD 4.63, (500,500) chunks, about 70% mem
* NOTE S2 10m 4 bands: 6 month, r6a.48xlarge, 192 CPUs, 1.5T RAM. (1000,1000) chunks, thread_per_worker=4, 150 mins, $USD 40
* NOTE IMPORTANT run assume first before attempting
* NOTE LS: 8 months, r6a.32xlarge succeeds with MADs, about 35 minutes, low memory usage
* NOTE S2 20m 6 bands: 8 months, r6a.32xlarge, about 15 minutes?
* NOTE typical obs: S2 for 8 months is 713
* NOTE typical shapes
- LS: ?
- S2 20m: 5536x5536 each time slice about 0.5GB
- S2 10m: 11071x11071 each time slice about 2GB
