# ptolemy
A data processing engine that is designed to help with data onboarding within 
the Filecoin network.

Ptolemy will consist of a series of component designed to help process entire 
existing file structures and allow users to efficiently package data for 
delivery to Filecoin Storage Providers (from here on in referred to as SPs).  
This allows to track and associate legacy filenames with CIDs, build car files 
of a configurable size and even invoke deals with deal engines on the network.

To get started with Ptolemy in its current iteration you can use the http 
interface to configure and run jobs in a mode known as blitz which will attempt
to build all car files in parallel.  A future version of ptolemy (coming soon)
will have a serial mode to support processing environments where there is 
limited staging capacity.

To get started we will define a job with the orchestrator, a job consists of a 
series of flags that need to be set when making a POST request with the 
orchestrator.  Below is a brief description of the flags plus a sample curl
command to use for guidance:

Let's imagine a project named "delta" that we wish to create and ultimately
process.

shard_size - this will indicate a threshold for the largest file size in GiBs
and will support an integer between 1 and 31.  This setting will break a file
into chunks of this size if it is larger than the shard_size.  For example if
the shard_size is 1 a 10 GiB file will result in 10 chunks.

staging_dir - a directory set aside for local cache where files can be chopped
and car files can be built and placed for extraction.

target_dir - directory where our dataset lives (in this case an unstructured
dataset in a filesystem typically) that we wish to convert to car files.

car_size - an integer between 1 and 31 that will allow a user to determine how
large the generated cars should be in GiBs.

encryption - currently not implemented but in a future ptolemy release users 
will be able to specify a key alias representing a specific key the user 
wishes to encrypt data with if data privacy is desired.

load_type - blitz vs serial, currently only blitz is supported but serial 
mode will be released soon.

I am going to make a curl call to the local ptolemy agent to build project
delta with the following characteristics:

shard_size = 1 GiB
staging_dir = /srv/delta-staging/shrek-staging/ptolemy-test
target_dir = /srv/delta-staging/shrek-staging/radiant-data-to-delta
car_size = 17 GiBs
encryption = None
load_type = blitz

To create and prepare this project issue the following command:

curl -X POST http://<ptolemy ip address>:8000/v0/create/delta -H 'Content-Type: application/json' -d '{"shard_size":1,"staging_dir":"/srv/delta-staging/shrek-staging/ptolemy-test","target_dir":"/srv/delta-staging/shrek-staging/radiant-data-to-delta","car_size":17,"encryption":"None","load_type":"blitz"}'

Once this has completed we can then issue a scan of the filesystem.  This will
populate our database with all the relevant file metadata and prepare the 
dataset to be containerized.

curl -X POST http://<ptolemy ip address>:8000/v0/scan/delta

We will now containerize the dataset once the project status has been declared 
"scanning completed" and this will break up the filesystem in containers
that align with any file splitting and ensure containers are of a roughly equal
size (in this case 17 GiBs).

curl -X POST http://<ptolemy ip address>:8000/v0/containerize/delta

Once the project reaches a "containerization complete" phase we can now begin
the build of our carfiles by calling the blitz mechanism which will populate
the workers with the car files they are responsible to build and turn the 
workers loose generating car files.

curl - X POST http://<ptolemy ip address>:8000/v0/blitz/delta

Running Ptolemy:

To run Ptolemy a list of dependencies need to be resolved and satisfied.  A 
more comprehensive set of instructions is coming soon.  To get started uvicorn
must be installed along with psycopg2 for postgres database support.  Ptolemy 
will also require a working postgres instance properly configured (details 
on this to follow as well).

Once dependencies are resolved:

We can launch the ptolemy orchestrator using the following command:
python3 ptolemy.py

* Please ensure the ptolemy.ini file is properly configured with desired 
IP to listen on for the orchestrator along with the port.

Then launch a worker to do the orchestrators dirty work (it should be noted
that only one worker per IP is allowed, however setting up workers on other
machines or containers is supported).

python3 worker.py 

* Please ensure the IP and port are set along with a desired thread count to use
for processing by the worker.  It should be noted the IP address should be the 
actual machine IP so the orchestrator can communicate with the worker effectively.
Attempting to use 0.0.0.0 will not work as the listening IP.

The worker when launched will automatically connect to the orchestrator and 
begin responding to heartbeats for worker fault detection.

Special Notes:

Ptolemy uses the equivalence of apparent-disk usage when calculating file size
this can actually cause sizing discrepancies in rare circumstances.  For 
example on ZFS systems with directories containing a single small file it 
can causes inflated capacity usage numbers and make carfiles significantly 
larger in size due to this behavior.  It is recommended that this behavior be 
tracked and potentially an xfs or ext4 type of filesystem is used.
