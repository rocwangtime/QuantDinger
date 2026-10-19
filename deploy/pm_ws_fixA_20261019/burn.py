import time
# ~44% of one core, like celery-worker on the 2 vCPU prod host
while True:
    end = time.perf_counter() + .0044
    while time.perf_counter() < end: pass
    time.sleep(.0056)
